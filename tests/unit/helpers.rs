use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use spreadeater::config::*;
use spreadeater::models::*;
use std::str::FromStr;

/// Convert f64 to Decimal via string to avoid floating-point imprecision.
fn d(v: f64) -> Decimal {
    Decimal::from_str(&format!("{}", v)).unwrap()
}

pub fn make_market(condition_id: &str) -> Market {
    Market {
        condition_id: condition_id.to_string(),
        market_slug: format!("test-market-{}", condition_id),
        question: "Will test pass?".to_string(),
        active: true,
        closed: false,
        archived: false,
        accepting_orders: true,
        is_binary: true,
        neg_risk: false,
        minimum_tick_size: "0.01".to_string(),
        tokens: vec![
            TokenInfo {
                token_id: format!("{}-yes", condition_id),
                outcome: Outcome::Yes,
                last_price: Some(dec!(0.50)),
            },
            TokenInfo {
                token_id: format!("{}-no", condition_id),
                outcome: Outcome::No,
                last_price: Some(dec!(0.50)),
            },
        ],
        reward_config: Some(RewardConfig {
            condition_id: condition_id.to_string(),
            daily_reward_rates: vec![dec!(5.0), dec!(5.0)],
            daily_reward_total: dec!(10.0),
            min_size: dec!(5.0),
            max_spread: dec!(0.04),
        }),
        end_date_iso: None,
        discovered_at: Utc::now(),
    }
}

pub fn make_position(condition_id: &str, yes: f64, no: f64) -> Position {
    let mut p = Position::new(condition_id.to_string());
    p.yes_size = d(yes);
    p.no_size = d(no);
    p
}

pub fn make_price_level(price: f64, size: f64) -> PriceLevel {
    PriceLevel {
        price: d(price),
        size: d(size),
    }
}

pub fn make_orderbook_snapshot(
    token_id: &str,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
) -> OrderBookSnapshot {
    OrderBookSnapshot {
        token_id: token_id.to_string(),
        exchange_ts: Some(Utc::now()),
        ingest_ts: Utc::now(),
        bids: bids
            .into_iter()
            .map(|(p, s)| make_price_level(p, s))
            .collect(),
        asks: asks
            .into_iter()
            .map(|(p, s)| make_price_level(p, s))
            .collect(),
    }
}

pub fn make_reward_config(condition_id: &str, daily_total: f64) -> RewardConfig {
    RewardConfig {
        condition_id: condition_id.to_string(),
        daily_reward_rates: vec![d(daily_total / 2.0); 2],
        daily_reward_total: d(daily_total),
        min_size: dec!(5.0),
        max_spread: dec!(0.04),
    }
}

pub fn make_canonical_market(condition_id: &str) -> CanonicalMarket {
    CanonicalMarket {
        condition_id: condition_id.to_string(),
        market_slug: format!("test-market-{}", condition_id),
        question: "Will test pass?".to_string(),
        yes_token_id: format!("{}-yes", condition_id),
        no_token_id: format!("{}-no", condition_id),
        reward_config: make_reward_config(condition_id, 10.0),
        neg_risk: false,
        tick_size: "0.01".to_string(),
        end_date: None,
        admitted_at: Utc::now(),
        status: MarketStatus::Admitted,
    }
}

pub fn make_quote_candidate(
    condition_id: &str,
    leg: QuoteLeg,
    price: f64,
    size: f64,
    status: QuoteStatus,
) -> QuoteCandidate {
    QuoteCandidate {
        condition_id: condition_id.to_string(),
        leg,
        price: d(price),
        size: d(size),
        status,
        reason: None,
    }
}

#[allow(dead_code)]
pub fn make_risk_config() -> RiskConfig {
    RiskConfig {
        hedge_timeout_secs: 10,
        hedge_exposure_tolerance: dec!(0.5),
        cash_reserve: dec!(50),
    }
}

#[allow(dead_code)]
pub fn make_strategy_config() -> StrategyConfig {
    serde_json::from_str::<StrategyConfig>(
        r#"{
        "max_hedge_cost_bps": "80",
        "max_slippage_bps": "80",
        "default_quote_size": "5",
        "min_edge_threshold": "0.01",
        "quote_drift_bps": "30",
        "bid_depth_pct": "0.50",
        "quote_refresh_secs": 5,
        "score_proxy": {
            "competition_multiplier": "1.5",
            "max_score_share": "0.25",
            "min_score_share": "0.0001",
            "target_score_share": "0.05",
            "calibration_sample_size": 20
        }
    }"#,
    )
    .expect("default strategy config")
}
