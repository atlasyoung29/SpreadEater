use rust_decimal::Decimal;
use tracing::debug;

use crate::config::StrategyConfig;
use crate::models::{HedgeabilityReport, OrderBookSnapshot, QuoteCandidate, QuoteLeg, QuoteStatus};

/// Compute hedgeability for a quote candidate.
///
/// For a YES BID fill (we bought YES), we hedge by buying NO (walk NO asks).
/// For a YES ASK fill (we sold YES), we hedge by selling NO (walk NO bids).
/// For a NO BID fill (we bought NO), we hedge by buying YES (walk YES asks).
/// For a NO ASK fill (we sold NO), we hedge by selling YES (walk YES bids).
pub fn compute_hedgeability(
    candidate: &QuoteCandidate,
    yes_book: &OrderBookSnapshot,
    no_book: &OrderBookSnapshot,
    config: &StrategyConfig,
) -> HedgeabilityReport {
    let (opposite_book, opposite_token_id, use_asks) = match candidate.leg {
        QuoteLeg::YesBid => (no_book, &no_book.token_id, true), // buy NO asks
        QuoteLeg::YesAsk => (no_book, &no_book.token_id, false), // sell NO bids
        QuoteLeg::NoBid => (yes_book, &yes_book.token_id, true), // buy YES asks
        QuoteLeg::NoAsk => (yes_book, &yes_book.token_id, false), // sell YES bids
    };

    let walk_result = if use_asks {
        opposite_book.walk_asks(candidate.size)
    } else {
        opposite_book.walk_bids(candidate.size)
    };

    let opposite_depth_available = walk_result.filled_size;

    // Compute slippage in bps relative to best price
    let best_price = if use_asks {
        opposite_book.best_ask().map(|l| l.price)
    } else {
        opposite_book.best_bid().map(|l| l.price)
    };

    let slippage_bps = match best_price {
        Some(bp) if bp > Decimal::ZERO => {
            let diff = if walk_result.weighted_avg_price > bp {
                walk_result.weighted_avg_price - bp
            } else {
                bp - walk_result.weighted_avg_price
            };
            (diff / bp) * Decimal::from(10000)
        }
        _ => Decimal::ZERO,
    };

    // Approval checks
    let mut is_approved = true;
    let mut rejection_reason = None;

    if !walk_result.fully_filled {
        is_approved = false;
        rejection_reason = Some(format!(
            "Insufficient opposite depth: need {}, available {}",
            candidate.size, walk_result.filled_size
        ));
    } else if slippage_bps > config.max_slippage_bps {
        is_approved = false;
        rejection_reason = Some(format!(
            "Slippage {} bps exceeds max {} bps",
            slippage_bps, config.max_slippage_bps
        ));
    }

    debug!(
        condition_id = %candidate.condition_id,
        leg = %candidate.leg,
        size = %candidate.size,
        hedgeable = %walk_result.filled_size,
        avg_price = %walk_result.weighted_avg_price,
        slippage_bps = %slippage_bps,
        approved = is_approved,
        "Hedgeability computed"
    );

    HedgeabilityReport {
        condition_id: candidate.condition_id.clone(),
        trigger_leg: candidate.leg,
        candidate_size: candidate.size,
        opposite_token_id: opposite_token_id.clone(),
        opposite_depth_available,
        max_hedgeable_size: walk_result.filled_size,
        weighted_avg_hedge_price: walk_result.weighted_avg_price,
        estimated_hedge_cost: walk_result.total_cost,
        slippage_bps,
        is_approved,
        rejection_reason,
    }
}

/// Update a quote candidate's status based on its hedgeability report.
pub fn apply_hedgeability_gate(candidate: &mut QuoteCandidate, report: &HedgeabilityReport) {
    if candidate.status == QuoteStatus::Rejected {
        return; // Already rejected for other reasons
    }

    if !report.is_approved {
        if candidate.status == QuoteStatus::Approved {
            candidate.status = QuoteStatus::Rejected;
        }
        candidate.reason = report.rejection_reason.clone();
    }
}
