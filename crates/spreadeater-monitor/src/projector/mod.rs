use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use sqlx::{Executor, PgPool, Postgres, Row, Transaction};

use spreadeater_core::payloads::{
    CalibrationAdjustedPayload, DecisionEventPayload, FillDetectedPayload, HedgeIntentPayload,
    HedgeResultPayload, MonitorDegradedPayload, NeutralityPayload, OrderCancelledPayload,
    OrderResizedPayload, OrderSubmittedPayload, RiskStateChangedPayload, StatusSnapshotPayload,
    UserStreamStatusChangedPayload,
};
use spreadeater_core::{EventEnvelope, EventType, Priority};

#[derive(Debug, Default)]
pub struct ProjectBatchOutcome {
    pub inserted: usize,
    pub projected_events: Vec<EventEnvelope>,
}

#[derive(Clone)]
pub struct PostgresProjector {
    pool: PgPool,
}

impl PostgresProjector {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn project_batch(&self, events: &[EventEnvelope]) -> Result<ProjectBatchOutcome> {
        let mut tx = self.pool.begin().await?;
        let mut outcome = ProjectBatchOutcome::default();

        for event in events {
            if project_event(&mut tx, event).await? {
                outcome.inserted += 1;
                outcome.projected_events.push(event.clone());
            }
        }

        tx.commit().await?;
        Ok(outcome)
    }

    pub async fn offset_for_file(&self, file_path: &str) -> Result<i64> {
        let offset = sqlx::query_scalar::<_, i64>(
            "SELECT byte_offset FROM ingestion_offsets WHERE file_path = $1",
        )
        .bind(file_path)
        .fetch_optional(&self.pool)
        .await?;

        Ok(offset.unwrap_or(0))
    }

    pub async fn store_offset(
        &self,
        file_path: &str,
        run_id: &str,
        byte_offset: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ingestion_offsets (file_path, run_id, byte_offset, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (file_path) DO UPDATE
            SET run_id = EXCLUDED.run_id,
                byte_offset = EXCLUDED.byte_offset,
                updated_at = NOW()
            "#,
        )
        .bind(file_path)
        .bind(run_id)
        .bind(byte_offset)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn reset_projections(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        tx.execute(
            r#"
            TRUNCATE TABLE
                cancellations,
                neutrality_evaluations,
                hedges,
                fills,
                orders,
                traces,
                positions_latest,
                markets,
                events_raw,
                ingestion_offsets,
                runs
            RESTART IDENTITY
            "#,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn emit_projection_rebuilt(
        &self,
        run_id: &str,
        mode: &str,
        events_processed: usize,
        duration_ms: u128,
    ) -> Result<()> {
        let payload = json!({
            "rebuild_id": format!("rebuild_{}", Utc::now().format("%Y%m%d_%H%M%S")),
            "run_id": run_id,
            "events_processed": events_processed,
            "duration_ms": duration_ms,
            "status": "success"
        });

        let event = EventEnvelope::new(
            EventType::ProjectionRebuilt,
            Priority::High,
            run_id.to_string(),
            "monitor".to_string(),
            mode.to_string(),
            payload,
        );

        let _ = self.project_batch(&[event]).await?;
        Ok(())
    }
}

async fn project_event(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<bool> {
    let inserted = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO events_raw (
            event_id,
            schema_version_major,
            schema_version_minor,
            event_type,
            priority,
            occurred_at,
            recorded_at,
            run_id,
            cycle_id,
            trace_id,
            source_component,
            mode,
            condition_id,
            market_slug,
            question,
            order_id,
            asset_id,
            hedge_id,
            payload
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        ON CONFLICT (event_id) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(event.event_id)
    .bind(i32::from(event.schema_version.major))
    .bind(i32::from(event.schema_version.minor))
    .bind(event.event_type.to_string())
    .bind(priority_name(event.priority))
    .bind(event.occurred_at)
    .bind(event.recorded_at)
    .bind(&event.run_id)
    .bind(&event.cycle_id)
    .bind(&event.trace_id)
    .bind(&event.source_component)
    .bind(&event.mode)
    .bind(&event.condition_id)
    .bind(&event.market_slug)
    .bind(&event.question)
    .bind(&event.order_id)
    .bind(&event.asset_id)
    .bind(&event.hedge_id)
    .bind(&event.payload)
    .fetch_optional(tx.as_mut())
    .await?;

    if inserted.is_none() {
        return Ok(false);
    }

    upsert_run(tx, event).await?;

    if let Some(condition_id) = &event.condition_id {
        ensure_market(tx, &event.run_id, condition_id, event).await?;
    }

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(tx, trace_id, event, trace_status(event.event_type), None).await?;
    }

    match event.event_type {
        EventType::DecisionEvaluated => handle_decision(tx, event).await?,
        EventType::QuoteApproved | EventType::QuoteRejected => handle_quote(tx, event).await?,
        EventType::OrderSubmitted => handle_order_submitted(tx, event).await?,
        EventType::OrderCancelled => handle_order_cancelled(tx, event).await?,
        EventType::OrderResized => handle_order_resized(tx, event).await?,
        EventType::FillDetected => handle_fill_detected(tx, event).await?,
        EventType::HedgeIntentCreated => handle_hedge_intent(tx, event).await?,
        EventType::HedgeDecisionEvaluated => {}
        EventType::HedgeResultRecorded => handle_hedge_result(tx, event).await?,
        EventType::HedgeExitPathRecorded => {}
        EventType::NeutralityEvaluated => handle_neutrality(tx, event).await?,
        EventType::MonitorDegraded => handle_monitor_degraded(tx, event).await?,
        EventType::RiskStateChanged => handle_risk_state_changed(tx, event).await?,
        EventType::UserStreamStatusChanged => handle_user_stream_status_changed(tx, event).await?,
        EventType::StatusSnapshot => handle_status_snapshot(tx, event).await?,
        EventType::CalibrationAdjusted => handle_calibration_adjusted(tx, event).await?,
        EventType::WatchdogVerdict | EventType::WatchdogKillTriggered => {}
        EventType::ProjectionRebuilt => {}
    }

    if let Some(condition_id) = &event.condition_id {
        refresh_market_rollups(tx, &event.run_id, condition_id, event.occurred_at).await?;
    }

    Ok(true)
}

async fn upsert_run(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<()> {
    let producer_lag_ms = (event.recorded_at - event.occurred_at)
        .num_milliseconds()
        .max(0);
    let index_lag_ms = (Utc::now() - event.occurred_at).num_milliseconds().max(0);
    let observer_health = if matches!(event.event_type, EventType::MonitorDegraded) {
        "degraded"
    } else {
        "healthy"
    };

    sqlx::query(
        r#"
        INSERT INTO runs (
            run_id,
            mode,
            started_at,
            observer_health,
            global_halt,
            risk_reason,
            user_stream_status,
            user_stream_detail,
            subscribed_markets,
            managed_markets,
            order_committed_usd,
            position_committed_usd,
            total_committed_usd,
            api_balance_usd,
            available_budget_usd,
            competition_multiplier,
            last_calibration_at,
            producer_lag_ms,
            index_lag_ms,
            last_event_at,
            last_recorded_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4,
            FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            $5, $6, $7, $8, NOW()
        )
        ON CONFLICT (run_id) DO UPDATE
        SET mode = EXCLUDED.mode,
            started_at = LEAST(runs.started_at, EXCLUDED.started_at),
            observer_health = CASE
                WHEN runs.observer_health = 'degraded' OR EXCLUDED.observer_health = 'degraded'
                THEN 'degraded'
                ELSE EXCLUDED.observer_health
            END,
            producer_lag_ms = EXCLUDED.producer_lag_ms,
            index_lag_ms = EXCLUDED.index_lag_ms,
            last_event_at = GREATEST(runs.last_event_at, EXCLUDED.last_event_at),
            last_recorded_at = GREATEST(runs.last_recorded_at, EXCLUDED.last_recorded_at),
            updated_at = NOW()
        "#,
    )
    .bind(&event.run_id)
    .bind(&event.mode)
    .bind(event.occurred_at)
    .bind(observer_health)
    .bind(producer_lag_ms)
    .bind(index_lag_ms)
    .bind(event.occurred_at)
    .bind(event.recorded_at)
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn ensure_market(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &str,
    condition_id: &str,
    event: &EventEnvelope,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO markets (
            run_id,
            condition_id,
            market_slug,
            question,
            last_event_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (run_id, condition_id) DO UPDATE
        SET market_slug = COALESCE(EXCLUDED.market_slug, markets.market_slug),
            question = COALESCE(EXCLUDED.question, markets.question),
            last_event_at = GREATEST(markets.last_event_at, EXCLUDED.last_event_at),
            updated_at = NOW()
        "#,
    )
    .bind(run_id)
    .bind(condition_id)
    .bind(&event.market_slug)
    .bind(&event.question)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn ensure_trace(
    tx: &mut Transaction<'_, Postgres>,
    trace_id: &str,
    event: &EventEnvelope,
    status: &'static str,
    decision_payload: Option<&serde_json::Value>,
) -> Result<()> {
    let market_payload = if let Some(condition_id) = &event.condition_id {
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT decision_payload FROM markets WHERE run_id = $1 AND condition_id = $2",
        )
        .bind(&event.run_id)
        .bind(condition_id)
        .fetch_optional(tx.as_mut())
        .await?
    } else {
        None
    };

    let payload = decision_payload.cloned().or(market_payload);

    sqlx::query(
        r#"
        INSERT INTO traces (
            trace_id,
            run_id,
            condition_id,
            market_slug,
            question,
            status,
            decision_payload,
            first_event_at,
            last_event_at,
            last_order_id,
            last_hedge_id,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
        ON CONFLICT (trace_id) DO UPDATE
        SET run_id = EXCLUDED.run_id,
            condition_id = COALESCE(EXCLUDED.condition_id, traces.condition_id),
            market_slug = COALESCE(EXCLUDED.market_slug, traces.market_slug),
            question = COALESCE(EXCLUDED.question, traces.question),
            status = EXCLUDED.status,
            decision_payload = COALESCE(EXCLUDED.decision_payload, traces.decision_payload),
            first_event_at = LEAST(traces.first_event_at, EXCLUDED.first_event_at),
            last_event_at = GREATEST(traces.last_event_at, EXCLUDED.last_event_at),
            last_order_id = COALESCE(EXCLUDED.last_order_id, traces.last_order_id),
            last_hedge_id = COALESCE(EXCLUDED.last_hedge_id, traces.last_hedge_id),
            updated_at = NOW()
        "#,
    )
    .bind(trace_id)
    .bind(&event.run_id)
    .bind(&event.condition_id)
    .bind(&event.market_slug)
    .bind(&event.question)
    .bind(status)
    .bind(payload)
    .bind(event.occurred_at)
    .bind(event.occurred_at)
    .bind(&event.order_id)
    .bind(&event.hedge_id)
    .execute(tx.as_mut())
    .await?;

    if let Some(condition_id) = &event.condition_id {
        sqlx::query(
            r#"
            UPDATE markets
            SET recent_trace_id = $3,
                last_event_at = GREATEST(last_event_at, $4),
                updated_at = NOW()
            WHERE run_id = $1 AND condition_id = $2
            "#,
        )
        .bind(&event.run_id)
        .bind(condition_id)
        .bind(trace_id)
        .bind(event.occurred_at)
        .execute(tx.as_mut())
        .await?;
    }

    Ok(())
}

async fn handle_decision(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<()> {
    let payload: DecisionEventPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize DecisionEvaluated payload")?;
    let condition_id = event
        .condition_id
        .as_deref()
        .context("DecisionEvaluated missing condition_id")?;

    ensure_market(tx, &event.run_id, condition_id, event).await?;

    sqlx::query(
        r#"
        UPDATE markets
        SET decision_status = $3,
            expected_reward_usd_day = $4,
            expected_hedge_cost_usd = $5,
            expected_edge_usd = $6,
            expected_edge_pct = $7,
            committed_capital_usd = $8,
            score_share = $9,
            max_hedgeable_size = $10,
            effective_quote_size = $11,
            decision_payload = $12,
            last_evaluated_at = $13,
            last_event_at = GREATEST(last_event_at, $13),
            updated_at = NOW()
        WHERE run_id = $1 AND condition_id = $2
        "#,
    )
    .bind(&event.run_id)
    .bind(condition_id)
    .bind(if payload.would_trade {
        "approved"
    } else {
        "rejected"
    })
    .bind(payload.expected_reward_usd_day)
    .bind(payload.expected_hedge_cost_usd)
    .bind(payload.expected_edge_usd)
    .bind(payload.expected_edge_pct)
    .bind(payload.committed_capital_usd.unwrap_or(Decimal::ZERO))
    .bind(payload.score_share)
    .bind(payload.max_hedgeable_size)
    .bind(Some(payload.effective_quote_size))
    .bind(&event.payload)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn handle_quote(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<()> {
    if let Some(trace_id) = &event.trace_id {
        ensure_trace(tx, trace_id, event, trace_status(event.event_type), None).await?;
    }
    Ok(())
}

async fn handle_order_submitted(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: OrderSubmittedPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize OrderSubmitted payload")?;
    let order_id = event
        .order_id
        .as_deref()
        .context("OrderSubmitted missing order_id")?;

    sqlx::query(
        r#"
        INSERT INTO orders (
            order_id,
            trace_id,
            run_id,
            condition_id,
            leg,
            side,
            price,
            size,
            matched_size,
            state,
            origin,
            role,
            committed_capital_delta_usd,
            token_id,
            neg_risk,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'submitted', $10, $11, $12, $13, $14, $15, $15)
        ON CONFLICT (order_id) DO UPDATE
        SET trace_id = COALESCE(EXCLUDED.trace_id, orders.trace_id),
            condition_id = COALESCE(EXCLUDED.condition_id, orders.condition_id),
            leg = COALESCE(EXCLUDED.leg, orders.leg),
            side = COALESCE(EXCLUDED.side, orders.side),
            price = EXCLUDED.price,
            size = EXCLUDED.size,
            matched_size = EXCLUDED.matched_size,
            state = 'submitted',
            origin = COALESCE(EXCLUDED.origin, orders.origin),
            role = COALESCE(EXCLUDED.role, orders.role),
            committed_capital_delta_usd = EXCLUDED.committed_capital_delta_usd,
            token_id = EXCLUDED.token_id,
            neg_risk = EXCLUDED.neg_risk,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(order_id)
    .bind(&event.trace_id)
    .bind(&event.run_id)
    .bind(&event.condition_id)
    .bind(payload.leg)
    .bind(payload.side.clone())
    .bind(payload.price)
    .bind(payload.size)
    .bind(payload.matched_size)
    .bind(payload.origin)
    .bind(payload.role)
    .bind(if payload.side == "BUY" {
        payload.price * (payload.size - payload.matched_size).max(Decimal::ZERO)
    } else {
        Decimal::ZERO
    })
    .bind(payload.token_id)
    .bind(payload.neg_risk)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(tx, trace_id, event, "order_open", None).await?;
    }

    Ok(())
}

async fn handle_order_cancelled(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: OrderCancelledPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize OrderCancelled payload")?;
    let order_id = event
        .order_id
        .as_deref()
        .context("OrderCancelled missing order_id")?;

    sqlx::query(
        r#"
        UPDATE orders
        SET state = 'cancelled',
            cancel_reason = $2,
            updated_at = $3
        WHERE order_id = $1
        "#,
    )
    .bind(order_id)
    .bind(payload.reason_code.code())
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    sqlx::query(
        r#"
        INSERT INTO cancellations (
            event_id,
            run_id,
            trace_id,
            condition_id,
            order_id,
            reason_code,
            reason_text,
            old_size,
            capital_delta,
            occurred_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(event.event_id)
    .bind(&event.run_id)
    .bind(&event.trace_id)
    .bind(&event.condition_id)
    .bind(order_id)
    .bind(payload.reason_code.code())
    .bind(payload.reason_text)
    .bind(payload.old_size)
    .bind(payload.capital_delta)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(tx, trace_id, event, "cancelled", None).await?;
    }

    Ok(())
}

async fn handle_order_resized(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: OrderResizedPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize OrderResized payload")?;
    let prior_order = sqlx::query(
        r#"
        SELECT leg, side, token_id, neg_risk
        FROM orders
        WHERE order_id = $1
        "#,
    )
    .bind(&payload.old_order_id)
    .fetch_optional(tx.as_mut())
    .await?;
    let prior_leg = prior_order
        .as_ref()
        .and_then(|row| row.try_get::<Option<String>, _>("leg").ok())
        .flatten();
    let prior_side = prior_order
        .as_ref()
        .and_then(|row| row.try_get::<Option<String>, _>("side").ok())
        .flatten();
    let prior_token_id = prior_order
        .as_ref()
        .and_then(|row| row.try_get::<Option<String>, _>("token_id").ok())
        .flatten();
    let prior_neg_risk = prior_order
        .as_ref()
        .and_then(|row| row.try_get::<Option<bool>, _>("neg_risk").ok())
        .flatten();

    sqlx::query(
        r#"
        UPDATE orders
        SET state = 'replaced',
            cancel_reason = $2,
            replacement_order_id = $3,
            updated_at = $4
        WHERE order_id = $1
        "#,
    )
    .bind(&payload.old_order_id)
    .bind(payload.reason_code.code())
    .bind(&payload.new_order_id)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    sqlx::query(
        r#"
        INSERT INTO orders (
            order_id,
            trace_id,
            run_id,
            condition_id,
            leg,
            side,
            price,
            size,
            matched_size,
            state,
            committed_capital_delta_usd,
            token_id,
            neg_risk,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'open', $9, $10, $11, $12, $12)
        ON CONFLICT (order_id) DO UPDATE
        SET trace_id = COALESCE(EXCLUDED.trace_id, orders.trace_id),
            condition_id = COALESCE(EXCLUDED.condition_id, orders.condition_id),
            leg = COALESCE(EXCLUDED.leg, orders.leg),
            side = COALESCE(EXCLUDED.side, orders.side),
            price = EXCLUDED.price,
            size = EXCLUDED.size,
            state = 'open',
            committed_capital_delta_usd = EXCLUDED.committed_capital_delta_usd,
            token_id = COALESCE(EXCLUDED.token_id, orders.token_id),
            neg_risk = COALESCE(EXCLUDED.neg_risk, orders.neg_risk),
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&payload.new_order_id)
    .bind(&event.trace_id)
    .bind(&event.run_id)
    .bind(&event.condition_id)
    .bind(prior_leg)
    .bind(prior_side)
    .bind(payload.new_price)
    .bind(payload.new_size)
    .bind((payload.new_price * payload.new_size) - (payload.old_price * payload.old_size))
    .bind(event.asset_id.clone().or(prior_token_id))
    .bind(prior_neg_risk)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    sqlx::query(
        r#"
        INSERT INTO cancellations (
            event_id,
            run_id,
            trace_id,
            condition_id,
            order_id,
            replacement_order_id,
            reason_code,
            reason_text,
            old_size,
            new_size,
            capital_delta,
            occurred_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(event.event_id)
    .bind(&event.run_id)
    .bind(&event.trace_id)
    .bind(&event.condition_id)
    .bind(&payload.old_order_id)
    .bind(&payload.new_order_id)
    .bind(payload.reason_code.code())
    .bind(payload.reason_code.description())
    .bind(payload.old_size)
    .bind(payload.new_size)
    .bind((payload.new_price * payload.new_size) - (payload.old_price * payload.old_size))
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(tx, trace_id, event, "order_resized", None).await?;
    }

    Ok(())
}

async fn handle_fill_detected(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: FillDetectedPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize FillDetected payload")?;

    sqlx::query(
        r#"
        INSERT INTO fills (
            fill_id,
            trace_id,
            run_id,
            condition_id,
            order_id,
            price,
            size,
            side,
            outcome,
            match_source,
            fallback_match,
            occurred_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (fill_id) DO NOTHING
        "#,
    )
    .bind(&payload.trade_id)
    .bind(&event.trace_id)
    .bind(&event.run_id)
    .bind(&event.condition_id)
    .bind(&event.order_id)
    .bind(payload.fill_price)
    .bind(payload.fill_size)
    .bind(&payload.side)
    .bind(&payload.outcome)
    .bind(payload.match_source.clone().or_else(|| {
        if payload.fallback_match {
            Some("fallback_match".to_string())
        } else {
            None
        }
    }))
    .bind(payload.fallback_match)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    if let Some(order_id) = &event.order_id {
        sqlx::query(
            r#"
            UPDATE orders
            SET matched_size = matched_size + $2,
                state = CASE
                    WHEN COALESCE(size, 0) <= matched_size + $2 THEN 'filled'
                    ELSE 'partially_filled'
                END,
                updated_at = $3
            WHERE order_id = $1
            "#,
        )
        .bind(order_id)
        .bind(payload.fill_size)
        .bind(event.occurred_at)
        .execute(tx.as_mut())
        .await?;
    }

    if let Some(condition_id) = event.condition_id.as_deref() {
        let normalized_side = payload.side.trim().to_ascii_uppercase();
        let normalized_outcome = payload.outcome.trim().to_ascii_uppercase();
        let (yes_delta, no_delta) = match (normalized_side.as_str(), normalized_outcome.as_str()) {
            ("BUY", "YES") => (payload.fill_size, Decimal::ZERO),
            ("SELL", "YES") => (-payload.fill_size, Decimal::ZERO),
            ("BUY", "NO") => (Decimal::ZERO, payload.fill_size),
            ("SELL", "NO") => (Decimal::ZERO, -payload.fill_size),
            _ => (Decimal::ZERO, Decimal::ZERO),
        };

        if yes_delta != Decimal::ZERO || no_delta != Decimal::ZERO {
            sqlx::query(
                r#"
                INSERT INTO positions_latest (
                    run_id,
                    condition_id,
                    yes_size,
                    no_size,
                    net_exposure,
                    complete_sets,
                    is_neutral,
                    updated_at
                )
                VALUES (
                    $1,
                    $2,
                    GREATEST($3, 0),
                    GREATEST($4, 0),
                    GREATEST($3, 0) - GREATEST($4, 0),
                    LEAST(GREATEST($3, 0), GREATEST($4, 0)),
                    ABS(GREATEST($3, 0) - GREATEST($4, 0)) <= 0.000001,
                    $5
                )
                ON CONFLICT (run_id, condition_id) DO UPDATE
                SET yes_size = GREATEST(positions_latest.yes_size + $3, 0),
                    no_size = GREATEST(positions_latest.no_size + $4, 0),
                    net_exposure =
                        GREATEST(positions_latest.yes_size + $3, 0)
                        - GREATEST(positions_latest.no_size + $4, 0),
                    complete_sets = LEAST(
                        GREATEST(positions_latest.yes_size + $3, 0),
                        GREATEST(positions_latest.no_size + $4, 0)
                    ),
                    is_neutral = ABS(
                        GREATEST(positions_latest.yes_size + $3, 0)
                        - GREATEST(positions_latest.no_size + $4, 0)
                    ) <= 0.000001,
                    updated_at = $5
                "#,
            )
            .bind(&event.run_id)
            .bind(condition_id)
            .bind(yes_delta)
            .bind(no_delta)
            .bind(event.occurred_at)
            .execute(tx.as_mut())
            .await?;
        }
    }

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(tx, trace_id, event, "filled", None).await?;
    }

    Ok(())
}

async fn handle_hedge_intent(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: HedgeIntentPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize HedgeIntentCreated payload")?;
    let hedge_id = event
        .hedge_id
        .as_deref()
        .context("HedgeIntentCreated missing hedge_id")?;

    sqlx::query(
        r#"
        INSERT INTO hedges (
            hedge_id,
            trace_id,
            run_id,
            condition_id,
            trigger_order_id,
            trigger_leg,
            fill_size,
            fill_price,
            hedge_token_id,
            hedge_side,
            result_status,
            origin,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending', $11, $12, $12)
        ON CONFLICT (hedge_id) DO UPDATE
        SET trace_id = COALESCE(EXCLUDED.trace_id, hedges.trace_id),
            condition_id = COALESCE(EXCLUDED.condition_id, hedges.condition_id),
            trigger_order_id = EXCLUDED.trigger_order_id,
            trigger_leg = EXCLUDED.trigger_leg,
            fill_size = EXCLUDED.fill_size,
            fill_price = EXCLUDED.fill_price,
            hedge_token_id = EXCLUDED.hedge_token_id,
            hedge_side = EXCLUDED.hedge_side,
            origin = COALESCE(EXCLUDED.origin, hedges.origin),
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(hedge_id)
    .bind(&event.trace_id)
    .bind(&event.run_id)
    .bind(&event.condition_id)
    .bind(payload.trigger_order_id)
    .bind(payload.trigger_leg)
    .bind(payload.fill_size)
    .bind(payload.fill_price)
    .bind(payload.hedge_token_id)
    .bind(payload.hedge_side)
    .bind(payload.origin)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(tx, trace_id, event, "hedge_pending", None).await?;
    }

    Ok(())
}

async fn handle_hedge_result(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: HedgeResultPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize HedgeResultRecorded payload")?;
    let hedge_id = event
        .hedge_id
        .as_deref()
        .context("HedgeResultRecorded missing hedge_id")?;

    sqlx::query(
        r#"
        INSERT INTO hedges (
            hedge_id,
            trace_id,
            run_id,
            condition_id,
            hedge_order_id,
            result_status,
            hedge_price,
            failure_reason,
            latency_ms,
            origin,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
        ON CONFLICT (hedge_id) DO UPDATE
        SET hedge_order_id = COALESCE(EXCLUDED.hedge_order_id, hedges.hedge_order_id),
            result_status = EXCLUDED.result_status,
            hedge_price = EXCLUDED.hedge_price,
            failure_reason = EXCLUDED.failure_reason,
            latency_ms = EXCLUDED.latency_ms,
            origin = COALESCE(EXCLUDED.origin, hedges.origin),
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(hedge_id)
    .bind(&event.trace_id)
    .bind(&event.run_id)
    .bind(&event.condition_id)
    .bind(payload.hedge_order_id)
    .bind(payload.result_status.clone())
    .bind(payload.hedge_price)
    .bind(payload.failure_reason)
    .bind(i64::try_from(payload.latency_ms).unwrap_or(i64::MAX))
    .bind(payload.origin)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(
            tx,
            trace_id,
            event,
            if payload.result_status == "success" {
                "hedged"
            } else {
                "hedge_failed"
            },
            None,
        )
        .await?;
    }

    Ok(())
}

async fn handle_neutrality(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: NeutralityPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize NeutralityEvaluated payload")?;
    let trace_id = event
        .trace_id
        .as_deref()
        .context("NeutralityEvaluated missing trace_id")?;
    let condition_id = event
        .condition_id
        .as_deref()
        .context("NeutralityEvaluated missing condition_id")?;

    sqlx::query(
        r#"
        INSERT INTO neutrality_evaluations (
            event_id,
            trace_id,
            run_id,
            condition_id,
            pre_yes_size,
            pre_no_size,
            post_yes_size,
            post_no_size,
            residual_exposure,
            complete_sets,
            tolerance,
            is_neutral,
            occurred_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(event.event_id)
    .bind(trace_id)
    .bind(&event.run_id)
    .bind(condition_id)
    .bind(payload.pre_yes_size)
    .bind(payload.pre_no_size)
    .bind(payload.post_yes_size)
    .bind(payload.post_no_size)
    .bind(payload.residual_exposure)
    .bind(payload.complete_sets)
    .bind(payload.tolerance)
    .bind(payload.is_neutral)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    sqlx::query(
        r#"
        INSERT INTO positions_latest (
            run_id,
            condition_id,
            yes_size,
            no_size,
            net_exposure,
            complete_sets,
            is_neutral,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (run_id, condition_id) DO UPDATE
        SET yes_size = EXCLUDED.yes_size,
            no_size = EXCLUDED.no_size,
            net_exposure = EXCLUDED.net_exposure,
            complete_sets = EXCLUDED.complete_sets,
            is_neutral = EXCLUDED.is_neutral,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&event.run_id)
    .bind(condition_id)
    .bind(payload.post_yes_size)
    .bind(payload.post_no_size)
    .bind(payload.post_yes_size - payload.post_no_size)
    .bind(payload.complete_sets)
    .bind(payload.is_neutral)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    if let Some(trace_id) = &event.trace_id {
        ensure_trace(
            tx,
            trace_id,
            event,
            if payload.is_neutral {
                "neutral"
            } else {
                "exposed"
            },
            None,
        )
        .await?;
    }

    Ok(())
}

async fn handle_monitor_degraded(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: MonitorDegradedPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize MonitorDegraded payload")?;

    sqlx::query(
        r#"
        UPDATE runs
        SET observer_health = 'degraded',
            index_lag_ms = COALESCE($2, index_lag_ms),
            updated_at = NOW()
        WHERE run_id = $1
        "#,
    )
    .bind(&event.run_id)
    .bind(payload.index_lag_ms.map(|value| value as i64))
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn handle_risk_state_changed(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: RiskStateChangedPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize RiskStateChanged payload")?;

    if payload.scope == "global" {
        sqlx::query(
            r#"
            UPDATE runs
            SET global_halt = COALESCE($2, global_halt),
                risk_reason = COALESCE($3, risk_reason),
                updated_at = NOW()
            WHERE run_id = $1
            "#,
        )
        .bind(&event.run_id)
        .bind(payload.global_halt)
        .bind(payload.reason)
        .execute(tx.as_mut())
        .await?;
    } else if let Some(condition_id) = &event.condition_id {
        sqlx::query(
            r#"
            UPDATE markets
            SET halted = $3,
                halt_reason = $4,
                last_event_at = GREATEST(last_event_at, $5),
                updated_at = NOW()
            WHERE run_id = $1 AND condition_id = $2
            "#,
        )
        .bind(&event.run_id)
        .bind(condition_id)
        .bind(payload.status.eq_ignore_ascii_case("halted"))
        .bind(payload.reason)
        .bind(event.occurred_at)
        .execute(tx.as_mut())
        .await?;
    }

    Ok(())
}

async fn handle_user_stream_status_changed(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: UserStreamStatusChangedPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize UserStreamStatusChanged payload")?;

    sqlx::query(
        r#"
        UPDATE runs
        SET user_stream_status = $2,
            user_stream_detail = $3,
            subscribed_markets = COALESCE($4, subscribed_markets),
            updated_at = NOW()
        WHERE run_id = $1
        "#,
    )
    .bind(&event.run_id)
    .bind(payload.status)
    .bind(payload.detail)
    .bind(
        payload
            .subscribed_markets
            .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
    )
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn handle_status_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: StatusSnapshotPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize StatusSnapshot payload")?;

    sqlx::query(
        r#"
        UPDATE runs
        SET managed_markets = $2,
            order_committed_usd = $3,
            position_committed_usd = $4,
            total_committed_usd = $5,
            api_balance_usd = $6,
            available_budget_usd = $7,
            competition_multiplier = $8,
            total_est_daily_usd = COALESCE($9, total_est_daily_usd),
            updated_at = NOW()
        WHERE run_id = $1
        "#,
    )
    .bind(&event.run_id)
    .bind(i64::try_from(payload.managed_markets).unwrap_or(i64::MAX))
    .bind(payload.order_committed_usd)
    .bind(payload.position_committed_usd)
    .bind(payload.total_committed_usd)
    .bind(payload.api_balance_usd)
    .bind(payload.available_budget_usd)
    .bind(payload.competition_multiplier)
    .bind(payload.total_est_daily_usd)
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn handle_calibration_adjusted(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<()> {
    let payload: CalibrationAdjustedPayload = serde_json::from_value(event.payload.clone())
        .context("deserialize CalibrationAdjusted payload")?;

    sqlx::query(
        r#"
        UPDATE runs
        SET competition_multiplier = $2,
            last_calibration_at = $3,
            updated_at = NOW()
        WHERE run_id = $1
        "#,
    )
    .bind(&event.run_id)
    .bind(payload.new_multiplier)
    .bind(event.occurred_at)
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn refresh_market_rollups(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &str,
    condition_id: &str,
    occurred_at: chrono::DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE markets
        SET
            open_order_notional_usd = COALESCE((
                SELECT SUM(
                    CASE
                        WHEN side = 'BUY' AND state IN ('open', 'partially_filled', 'submitted')
                        THEN price * GREATEST(size - matched_size, 0)
                        ELSE 0
                    END
                )
                FROM orders
                WHERE run_id = $1 AND condition_id = $2
            ), 0),
            yes_size = COALESCE((
                SELECT yes_size
                FROM positions_latest
                WHERE run_id = $1 AND condition_id = $2
            ), 0),
            no_size = COALESCE((
                SELECT no_size
                FROM positions_latest
                WHERE run_id = $1 AND condition_id = $2
            ), 0),
            net_exposure = COALESCE((
                SELECT net_exposure
                FROM positions_latest
                WHERE run_id = $1 AND condition_id = $2
            ), 0),
            complete_sets = COALESCE((
                SELECT complete_sets
                FROM positions_latest
                WHERE run_id = $1 AND condition_id = $2
            ), 0),
            is_neutral = COALESCE((
                SELECT is_neutral
                FROM positions_latest
                WHERE run_id = $1 AND condition_id = $2
            ), FALSE),
            last_event_at = GREATEST(last_event_at, $3),
            updated_at = NOW()
        WHERE run_id = $1 AND condition_id = $2
        "#,
    )
    .bind(run_id)
    .bind(condition_id)
    .bind(occurred_at)
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

fn trace_status(event_type: EventType) -> &'static str {
    match event_type {
        EventType::DecisionEvaluated => "decision",
        EventType::QuoteApproved => "quote_approved",
        EventType::QuoteRejected => "quote_rejected",
        EventType::OrderSubmitted => "order_open",
        EventType::OrderResized => "order_resized",
        EventType::OrderCancelled => "cancelled",
        EventType::FillDetected => "filled",
        EventType::HedgeIntentCreated => "hedge_pending",
        EventType::HedgeDecisionEvaluated => "hedge_decision",
        EventType::HedgeResultRecorded => "hedged",
        EventType::HedgeExitPathRecorded => "hedge_exit",
        EventType::NeutralityEvaluated => "neutral",
        EventType::MonitorDegraded => "degraded",
        EventType::RiskStateChanged => "risk_state_changed",
        EventType::UserStreamStatusChanged => "user_stream_status_changed",
        EventType::StatusSnapshot => "status_snapshot",
        EventType::CalibrationAdjusted => "calibration_adjusted",
        EventType::WatchdogVerdict => "watchdog_verdict",
        EventType::WatchdogKillTriggered => "watchdog_kill_triggered",
        EventType::ProjectionRebuilt => "rebuild",
    }
}

fn priority_name(priority: Priority) -> &'static str {
    match priority {
        Priority::Critical => "critical",
        Priority::High => "high",
        Priority::Normal => "normal",
        Priority::Debug => "debug",
    }
}
