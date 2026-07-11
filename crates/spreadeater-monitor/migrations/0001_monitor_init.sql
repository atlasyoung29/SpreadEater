CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    observer_health TEXT NOT NULL DEFAULT 'healthy',
    global_halt BOOLEAN NOT NULL DEFAULT FALSE,
    risk_reason TEXT,
    user_stream_status TEXT,
    user_stream_detail TEXT,
    subscribed_markets BIGINT,
    managed_markets BIGINT,
    order_committed_usd NUMERIC,
    position_committed_usd NUMERIC,
    total_committed_usd NUMERIC,
    max_total_exposure_usd NUMERIC,
    api_balance_usd NUMERIC,
    available_budget_usd NUMERIC,
    competition_multiplier NUMERIC,
    last_calibration_at TIMESTAMPTZ,
    producer_lag_ms BIGINT NOT NULL DEFAULT 0,
    index_lag_ms BIGINT NOT NULL DEFAULT 0,
    last_event_at TIMESTAMPTZ NOT NULL,
    last_recorded_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events_raw (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    schema_version_major INTEGER NOT NULL,
    schema_version_minor INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    priority TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    run_id TEXT NOT NULL,
    cycle_id TEXT,
    trace_id TEXT,
    source_component TEXT NOT NULL,
    mode TEXT NOT NULL,
    condition_id TEXT,
    market_slug TEXT,
    question TEXT,
    order_id TEXT,
    asset_id TEXT,
    hedge_id TEXT,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    run_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    market_slug TEXT,
    question TEXT,
    decision_status TEXT,
    expected_reward_usd_day NUMERIC,
    expected_hedge_cost_usd NUMERIC,
    expected_edge_usd NUMERIC,
    expected_edge_pct NUMERIC,
    committed_capital_usd NUMERIC NOT NULL DEFAULT 0,
    score_share NUMERIC,
    max_hedgeable_size NUMERIC,
    effective_quote_size NUMERIC,
    halted BOOLEAN NOT NULL DEFAULT FALSE,
    halt_reason TEXT,
    open_order_notional_usd NUMERIC NOT NULL DEFAULT 0,
    yes_size NUMERIC NOT NULL DEFAULT 0,
    no_size NUMERIC NOT NULL DEFAULT 0,
    net_exposure NUMERIC NOT NULL DEFAULT 0,
    complete_sets NUMERIC NOT NULL DEFAULT 0,
    is_neutral BOOLEAN NOT NULL DEFAULT FALSE,
    decision_payload JSONB,
    recent_trace_id TEXT,
    last_evaluated_at TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, condition_id)
);

CREATE TABLE IF NOT EXISTS traces (
    trace_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    condition_id TEXT,
    market_slug TEXT,
    question TEXT,
    status TEXT NOT NULL,
    decision_event_id UUID,
    decision_payload JSONB,
    first_event_at TIMESTAMPTZ NOT NULL,
    last_event_at TIMESTAMPTZ NOT NULL,
    last_order_id TEXT,
    last_hedge_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    trace_id TEXT,
    run_id TEXT NOT NULL,
    condition_id TEXT,
    leg TEXT,
    side TEXT,
    price NUMERIC,
    size NUMERIC,
    matched_size NUMERIC NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    origin TEXT,
    role TEXT,
    cancel_reason TEXT,
    replacement_order_id TEXT,
    committed_capital_delta_usd NUMERIC NOT NULL DEFAULT 0,
    token_id TEXT,
    neg_risk BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    trace_id TEXT,
    run_id TEXT NOT NULL,
    condition_id TEXT,
    order_id TEXT,
    price NUMERIC,
    size NUMERIC,
    side TEXT,
    outcome TEXT,
    match_source TEXT,
    fallback_match BOOLEAN NOT NULL DEFAULT FALSE,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS hedges (
    hedge_id TEXT PRIMARY KEY,
    trace_id TEXT,
    run_id TEXT NOT NULL,
    condition_id TEXT,
    trigger_order_id TEXT,
    trigger_leg TEXT,
    fill_size NUMERIC,
    fill_price NUMERIC,
    hedge_token_id TEXT,
    hedge_side TEXT,
    hedge_order_id TEXT,
    result_status TEXT,
    hedge_price NUMERIC,
    failure_reason TEXT,
    latency_ms BIGINT,
    origin TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS neutrality_evaluations (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    trace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    condition_id TEXT,
    pre_yes_size NUMERIC NOT NULL,
    pre_no_size NUMERIC NOT NULL,
    post_yes_size NUMERIC NOT NULL,
    post_no_size NUMERIC NOT NULL,
    residual_exposure NUMERIC NOT NULL,
    complete_sets NUMERIC NOT NULL,
    tolerance NUMERIC NOT NULL,
    is_neutral BOOLEAN NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS cancellations (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    trace_id TEXT,
    condition_id TEXT,
    order_id TEXT NOT NULL,
    replacement_order_id TEXT,
    reason_code TEXT NOT NULL,
    reason_text TEXT,
    old_size NUMERIC,
    new_size NUMERIC,
    capital_delta NUMERIC,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS positions_latest (
    run_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    yes_size NUMERIC NOT NULL DEFAULT 0,
    no_size NUMERIC NOT NULL DEFAULT 0,
    net_exposure NUMERIC NOT NULL DEFAULT 0,
    complete_sets NUMERIC NOT NULL DEFAULT 0,
    is_neutral BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, condition_id)
);

CREATE TABLE IF NOT EXISTS ingestion_offsets (
    file_path TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    byte_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_raw_run_id ON events_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_events_raw_trace_id ON events_raw (trace_id);
CREATE INDEX IF NOT EXISTS idx_events_raw_condition_id ON events_raw (condition_id);
CREATE INDEX IF NOT EXISTS idx_events_raw_event_type ON events_raw (event_type);
CREATE INDEX IF NOT EXISTS idx_events_raw_occurred_at ON events_raw (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_markets_last_event_at ON markets (last_event_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_trace_id ON orders (trace_id);
CREATE INDEX IF NOT EXISTS idx_orders_condition_id ON orders (condition_id);
CREATE INDEX IF NOT EXISTS idx_fills_trace_id ON fills (trace_id);
CREATE INDEX IF NOT EXISTS idx_hedges_trace_id ON hedges (trace_id);
CREATE INDEX IF NOT EXISTS idx_neutrality_trace_id ON neutrality_evaluations (trace_id);
CREATE INDEX IF NOT EXISTS idx_cancellations_reason_code ON cancellations (reason_code);
