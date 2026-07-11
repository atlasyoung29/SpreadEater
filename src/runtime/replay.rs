use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::models::DecisionReport;
use crate::persistence::FileArchive;

/// Summary of a single replayed session.
pub struct ReplaySummary {
    pub session_path: PathBuf,
    pub total_markets: usize,
    pub decision_changes: usize,
    pub avg_score_proxy: Option<Decimal>,
    pub details: Vec<ReplayDetail>,
}

pub struct ReplayDetail {
    pub condition_id: String,
    pub market_slug: String,
    pub original_would_trade: bool,
    pub replayed_would_trade: bool,
    pub original_edge: Option<Decimal>,
    pub replayed_edge: Option<Decimal>,
    pub score_proxy: Option<Decimal>,
}

/// Replays archived sessions with current (or overridden) parameters.
///
/// For existing archives without book data, the replay is limited to
/// re-evaluating viability with a synthetic score proxy based on the
/// stored max_spread and quote prices. This is approximate but still
/// useful for parameter sensitivity analysis.
pub struct ReplayEngine {
    config: Config,
}

impl ReplayEngine {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Override the competition multiplier for this replay run.
    pub fn with_competition_multiplier(mut self, multiplier: Decimal) -> Self {
        self.config.strategy.score_proxy.competition_multiplier = multiplier;
        self
    }

    /// Replay a single session file.
    pub async fn replay_session(&self, path: &Path) -> Result<ReplaySummary> {
        let reports = FileArchive::load_session(path)
            .await
            .context("Failed to load session")?;

        let mut details = Vec::new();
        let mut total_score_proxy = Decimal::ZERO;
        let mut score_proxy_count = 0u32;
        let mut decision_changes = 0usize;

        for report in &reports {
            let (replayed_viable, replayed_edge, new_score_proxy) = self.re_evaluate_report(report);

            let replayed_would_trade = if replayed_viable {
                // Preserve the original hedgeability decision — we only
                // re-evaluate the viability/score component
                report
                    .candidate_quotes
                    .iter()
                    .any(|c| c.status == crate::models::QuoteStatus::Approved)
            } else {
                false
            };

            if report.would_trade != replayed_would_trade {
                decision_changes += 1;
            }

            if let Some(sp) = new_score_proxy {
                total_score_proxy += sp;
                score_proxy_count += 1;
            }

            details.push(ReplayDetail {
                condition_id: report.condition_id.clone(),
                market_slug: report.market_slug.clone(),
                original_would_trade: report.would_trade,
                replayed_would_trade,
                original_edge: report.reward_viability.as_ref().map(|v| v.estimated_edge),
                replayed_edge: Some(replayed_edge),
                score_proxy: new_score_proxy,
            });
        }

        let avg_score_proxy = if score_proxy_count > 0 {
            Some(total_score_proxy / Decimal::from(score_proxy_count))
        } else {
            None
        };

        Ok(ReplaySummary {
            session_path: path.to_path_buf(),
            total_markets: reports.len(),
            decision_changes,
            avg_score_proxy,
            details,
        })
    }

    /// Replay all session files in a directory.
    pub async fn replay_directory(&self, archive: &FileArchive) -> Result<Vec<ReplaySummary>> {
        let sessions = archive.list_sessions().await?;
        let mut summaries = Vec::new();

        for path in &sessions {
            match self.replay_session(path).await {
                Ok(summary) => summaries.push(summary),
                Err(e) => {
                    tracing::error!(path = %path.display(), error = %e, "Failed to replay session");
                }
            }
        }

        Ok(summaries)
    }

    /// Re-evaluate a report with current parameters.
    ///
    /// Without raw book data, we use a synthetic score proxy:
    /// - If max_spread is available (new archives), estimate share from
    ///   quote prices and configured multiplier
    /// - If max_spread is zero (old archives), use min_score_share
    ///
    /// Returns (is_viable, estimated_edge, score_proxy).
    fn re_evaluate_report(&self, report: &DecisionReport) -> (bool, Decimal, Option<Decimal>) {
        let config = &self.config.strategy;
        let proxy_config = &config.score_proxy;
        let daily_reward = report.daily_reward_total;
        let max_spread = report.max_spread;

        // Compute synthetic score share from stored data
        let score_share = if max_spread > Decimal::ZERO {
            // Estimate our score from the quote candidates
            let our_score: Decimal = report
                .candidate_quotes
                .iter()
                .map(|c| {
                    // Approximate spread as distance from 0.50 midpoint
                    // This is rough but the best we can do without books
                    let mid = Decimal::new(50, 2);
                    let spread = (c.price - mid).abs();
                    synthetic_order_score(max_spread, spread, c.size)
                })
                .sum();

            // Without book data, estimate competition from multiplier alone
            // Assume competitor score ≈ our_score * competition_multiplier
            let comp = our_score * proxy_config.competition_multiplier;
            let total = our_score + comp;

            if total > Decimal::ZERO {
                (our_score / total)
                    .max(proxy_config.min_score_share)
                    .min(proxy_config.max_score_share)
            } else {
                proxy_config.min_score_share
            }
        } else {
            proxy_config.min_score_share
        };

        let estimated_reward = daily_reward * score_share;

        // Re-use the original hedge cost (we can't recompute without books)
        let estimated_hedge_cost = report
            .reward_viability
            .as_ref()
            .map(|v| v.estimated_hedge_cost)
            .unwrap_or(Decimal::ZERO);

        let estimated_edge = estimated_reward - estimated_hedge_cost;
        let is_viable = estimated_edge >= config.min_edge_threshold;

        (is_viable, estimated_edge, Some(score_share))
    }
}

/// Simplified per-order score for replay (no book mid available).
fn synthetic_order_score(max_spread: Decimal, spread_to_mid: Decimal, size: Decimal) -> Decimal {
    if max_spread <= Decimal::ZERO || spread_to_mid >= max_spread {
        return Decimal::ZERO;
    }
    let ratio = (max_spread - spread_to_mid) / max_spread;
    ratio * ratio * size
}

/// Print a replay summary to the console.
pub fn print_replay_summary(summary: &ReplaySummary) {
    println!(
        "\n=== REPLAY: {} ===",
        summary
            .session_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );

    for detail in &summary.details {
        let orig = if detail.original_would_trade {
            "TRADE"
        } else {
            "PASS"
        };
        let repl = if detail.replayed_would_trade {
            "TRADE"
        } else {
            "PASS"
        };
        let changed = if detail.original_would_trade != detail.replayed_would_trade {
            " [CHANGED]"
        } else {
            ""
        };
        let orig_edge = detail
            .original_edge
            .map(|e| format!("${}", e))
            .unwrap_or_else(|| "N/A".to_string());
        let repl_edge = detail
            .replayed_edge
            .map(|e| format!("${}", e))
            .unwrap_or_else(|| "N/A".to_string());
        let sp = detail
            .score_proxy
            .map(|s| format!("{}", s))
            .unwrap_or_else(|| "N/A".to_string());

        println!(
            "  {} | orig={} repl={} | edge: {} -> {} | score_proxy={} (approx){}",
            detail.market_slug, orig, repl, orig_edge, repl_edge, sp, changed
        );
    }

    let avg_sp = summary
        .avg_score_proxy
        .map(|s| format!("{}", s))
        .unwrap_or_else(|| "N/A".to_string());

    println!(
        "\nSummary: {} markets, {} decision changes, avg score proxy: {} (approx)",
        summary.total_markets, summary.decision_changes, avg_sp
    );
}
