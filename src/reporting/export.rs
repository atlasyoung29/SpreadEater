use anyhow::{Context, Result};
use std::io::Write;
use std::path::Path;

use crate::models::{DecisionReport, QuoteLeg};

pub fn export_session_csv(reports: &[DecisionReport], output: &Path) -> Result<()> {
    let mut f = std::fs::File::create(output)
        .with_context(|| format!("Failed to create {}", output.display()))?;

    // Header
    writeln!(
        f,
        "Market Slug,Question,Daily Reward,Max Spread,Score Share,Would Trade,\
         YES Bid Price,YES Bid Size,YES Bid Status,\
         YES Ask Price,YES Ask Size,YES Ask Status,\
         NO Bid Price,NO Bid Size,NO Bid Status,\
         NO Ask Price,NO Ask Size,NO Ask Status,\
         Est Reward,Est Hedge Cost,Est Edge,Reasons"
    )?;

    for report in reports {
        let find_leg = |leg: QuoteLeg| report.candidate_quotes.iter().find(|c| c.leg == leg);

        let leg_csv = |leg: QuoteLeg| -> String {
            match find_leg(leg) {
                Some(c) => format!("{},{},{:?}", c.price, c.size, c.status),
                None => ",,".to_string(),
            }
        };

        let score = report
            .score_proxy
            .map(|s| s.to_string())
            .unwrap_or_default();

        let (est_reward, est_hedge, est_edge) = match &report.reward_viability {
            Some(v) => (
                v.estimated_reward.to_string(),
                v.estimated_hedge_cost.to_string(),
                v.estimated_edge.to_string(),
            ),
            None => (String::new(), String::new(), String::new()),
        };

        let reasons = report.reasons.join("; ").replace('"', "'");

        writeln!(
            f,
            "\"{}\",\"{}\",{},{},{},{},{},{},{},{},{},{},{},\"{}\"",
            report.market_slug.replace('"', "'"),
            report.question.replace('"', "'"),
            report.daily_reward_total,
            report.max_spread,
            score,
            report.would_trade,
            leg_csv(QuoteLeg::YesBid),
            leg_csv(QuoteLeg::YesAsk),
            leg_csv(QuoteLeg::NoBid),
            leg_csv(QuoteLeg::NoAsk),
            est_reward,
            est_hedge,
            est_edge,
            reasons,
        )?;
    }

    Ok(())
}
