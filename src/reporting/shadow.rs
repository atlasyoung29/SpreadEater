use tracing::info;

use crate::models::{
    CanonicalMarket, DecisionReport, HedgeabilityReport, QuoteCandidate, QuoteSet, QuoteStatus,
    RewardViability,
};
use crate::strategy::ScoreProxyResult;

/// Build a decision report for a single market.
pub fn build_decision_report(
    market: &CanonicalMarket,
    quote_set: &QuoteSet,
    hedge_reports: &[HedgeabilityReport],
    viability: Option<RewardViability>,
    is_viable: bool,
    score_proxy: &ScoreProxyResult,
) -> DecisionReport {
    let mut reasons = Vec::new();

    // Collect rejection reasons from quotes
    for candidate in &quote_set.candidates {
        if matches!(
            candidate.status,
            QuoteStatus::Rejected | QuoteStatus::Suppressed
        ) {
            if let Some(ref reason) = candidate.reason {
                reasons.push(format!("{}: {}", candidate.leg, reason));
            }
        }
    }

    // Collect rejection reasons from hedge reports
    for report in hedge_reports {
        if !report.is_approved {
            if let Some(ref reason) = report.rejection_reason {
                reasons.push(format!("Hedge {}: {}", report.trigger_leg, reason));
            }
        }
    }

    // Check viability
    if !is_viable {
        reasons.push("Reward viability check failed: estimated edge below threshold".to_string());
    }

    // Determine would_trade: at least one approved leg with passing hedgeability and viable reward
    let has_approved_hedgeable_leg = quote_set.candidates.iter().any(|c| {
        c.status == QuoteStatus::Approved
            && hedge_reports
                .iter()
                .any(|h| h.trigger_leg == c.leg && h.is_approved)
    });

    let would_trade = has_approved_hedgeable_leg && is_viable;

    DecisionReport {
        condition_id: market.condition_id.clone(),
        market_slug: market.market_slug.clone(),
        question: market.question.clone(),
        daily_reward_total: market.reward_config.daily_reward_total,
        score_proxy: Some(score_proxy.estimated_share),
        max_spread: market.reward_config.max_spread,
        effective_quote_size: quote_set
            .candidates
            .iter()
            .filter(|c| c.status == QuoteStatus::Approved && c.leg.is_bid())
            .map(|c| c.size)
            .max()
            .or_else(|| {
                quote_set
                    .candidates
                    .iter()
                    .filter(|c| c.status == QuoteStatus::Approved)
                    .map(|c| c.size)
                    .max()
            })
            .unwrap_or_default(),
        candidate_quotes: quote_set.candidates.clone(),
        reward_viability: viability,
        would_trade,
        reasons,
    }
}

/// Print a concise scan result to the log.
/// Only shows market name + rejection reasons (or TRADE if viable).
pub fn log_decision_report(report: &DecisionReport) {
    if report.would_trade {
        info!(
            market = %report.question,
            size = %report.effective_quote_size,
            "TRADE"
        );
    } else {
        let reasons = if report.reasons.is_empty() {
            "no approved legs".to_string()
        } else {
            report.reasons.join("; ")
        };
        info!(
            market = %report.question,
            reasons = %reasons,
            "SKIP"
        );
    }
}

fn format_quotes(quotes: &[QuoteCandidate]) -> String {
    quotes
        .iter()
        .map(|q| {
            format!(
                "  {:>8} | price={:<8} size={:<8} status={:?}{}\n",
                q.leg.to_string(),
                q.price,
                q.size,
                q.status,
                q.reason
                    .as_ref()
                    .map(|r| format!(" ({})", r))
                    .unwrap_or_default()
            )
        })
        .collect()
}

fn format_viability(v: &Option<RewardViability>) -> String {
    match v {
        Some(v) => format!(
            "Reward Viability:\n\
             \x20 Score Share:     {} (approx)\n\
             \x20 Est. Reward:     ${}\n\
             \x20 Est. Hedge Cost: ${}\n\
             \x20 Est. Edge:       ${}\n",
            v.score_share_approx, v.estimated_reward, v.estimated_hedge_cost, v.estimated_edge
        ),
        None => "Reward Viability: not computed\n".to_string(),
    }
}

fn format_reasons(reasons: &[String]) -> String {
    if reasons.is_empty() {
        String::new()
    } else {
        let items: String = reasons.iter().map(|r| format!("  - {}\n", r)).collect();
        format!("Reasons:\n{}", items)
    }
}
