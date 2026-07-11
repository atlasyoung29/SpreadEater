use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};

use crate::config::MonitorConfig;
use crate::store::{
    fetch_config_document, fetch_error_logs, fetch_events, fetch_history, fetch_inventory,
    fetch_market_detail, fetch_open_orders, fetch_overview, fetch_trace_detail, fetch_watchlist,
    ErrorLogFilter, EventFilter, HistoryFilter, InventoryFilter, LiveBroadcaster, MarketPageFilter,
    OpenOrdersFilter,
};

#[derive(Clone)]
struct ApiState {
    pool: PgPool,
    broadcaster: LiveBroadcaster,
    web_dist: PathBuf,
    bot_config_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct PolymarketUrlQuery {
    market_slug: String,
}

#[derive(Debug, Deserialize)]
struct GammaMarketResponse {
    slug: String,
    #[serde(default)]
    events: Vec<GammaEventSummary>,
}

#[derive(Debug, Deserialize)]
struct GammaEventSummary {
    slug: String,
}

pub async fn serve_api(
    pool: PgPool,
    broadcaster: LiveBroadcaster,
    config: MonitorConfig,
) -> Result<()> {
    let app = build_app(pool, broadcaster, config.web_dist, config.bot_config_path);
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "monitor API listening");
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn build_app(
    pool: PgPool,
    broadcaster: LiveBroadcaster,
    web_dist: PathBuf,
    bot_config_path: PathBuf,
) -> Router {
    let state = Arc::new(ApiState {
        pool,
        broadcaster,
        web_dist,
        bot_config_path,
    });

    Router::new()
        .route("/api/v1/overview", get(get_overview))
        .route("/api/v1/open-orders", get(get_open_orders))
        .route("/api/v1/inventory", get(get_inventory))
        .route("/api/v1/watchlist", get(get_watchlist))
        .route("/api/v1/history", get(get_history))
        .route("/api/v1/errors", get(get_errors))
        .route("/api/v1/config", get(get_config))
        .route("/api/v1/markets/{condition_id}", get(get_market_detail))
        .route("/api/v1/traces/{trace_id}", get(get_trace_detail))
        .route("/api/v1/events", get(get_events))
        .route("/api/v1/polymarket-url", get(get_polymarket_url))
        .route("/ws/live", get(ws_live))
        .fallback_service(spa_service(state.web_dist.clone()))
        .with_state(state)
}

async fn get_overview(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let overview = fetch_overview(&state.pool)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::service_unavailable("monitor not initialized"))?;
    Ok(Json(
        serde_json::to_value(overview).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct OpenOrdersQuery {
    q: Option<String>,
    status: Option<String>,
    side: Option<String>,
    role: Option<String>,
    halted: Option<bool>,
    page: Option<i64>,
    page_size: Option<i64>,
}

async fn get_open_orders(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<OpenOrdersQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let response = fetch_open_orders(
        &state.pool,
        OpenOrdersFilter {
            q: query.q,
            status: query.status,
            side: query.side,
            role: query.role,
            halted: query.halted,
            page: query.page.unwrap_or(1),
            page_size: query.page_size.unwrap_or(100),
        },
    )
    .await
    .map_err(map_filter_error)?;

    Ok(Json(
        serde_json::to_value(response).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct InventoryQuery {
    q: Option<String>,
    neutrality: Option<bool>,
    has_open_orders: Option<bool>,
    halted: Option<bool>,
    exposure_side: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
}

async fn get_inventory(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<InventoryQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let response = fetch_inventory(
        &state.pool,
        InventoryFilter {
            q: query.q,
            neutrality: query.neutrality,
            has_open_orders: query.has_open_orders,
            halted: query.halted,
            exposure_side: query.exposure_side,
            page: query.page.unwrap_or(1),
            page_size: query.page_size.unwrap_or(100),
        },
    )
    .await
    .map_err(map_filter_error)?;

    Ok(Json(
        serde_json::to_value(response).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct WatchlistQuery {
    q: Option<String>,
    halted: Option<bool>,
    page: Option<i64>,
    page_size: Option<i64>,
}

async fn get_watchlist(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<WatchlistQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let response = fetch_watchlist(
        &state.pool,
        MarketPageFilter {
            q: query.q,
            halted: query.halted,
            page: query.page.unwrap_or(1),
            page_size: query.page_size.unwrap_or(100),
        },
    )
    .await
    .map_err(map_filter_error)?;

    Ok(Json(
        serde_json::to_value(response).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    q: Option<String>,
    category: Option<String>,
    event_type: Option<String>,
    priority: Option<String>,
    run_id: Option<String>,
    trace_id: Option<String>,
    condition_id: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
}

async fn get_history(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let response = fetch_history(
        &state.pool,
        HistoryFilter {
            q: query.q,
            category: query.category,
            event_type: query.event_type,
            priority: query.priority,
            run_id: query.run_id,
            trace_id: query.trace_id,
            condition_id: query.condition_id,
            page: query.page.unwrap_or(1),
            page_size: query.page_size.unwrap_or(100),
        },
    )
    .await
    .map_err(map_filter_error)?;

    Ok(Json(
        serde_json::to_value(response).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct ErrorsQuery {
    q: Option<String>,
    level: Option<String>,
    window_minutes: Option<i64>,
    page: Option<i64>,
    page_size: Option<i64>,
}

async fn get_errors(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<ErrorsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let response = fetch_error_logs(
        &state.pool,
        ErrorLogFilter {
            q: query.q,
            level: query.level,
            window_minutes: query.window_minutes,
            page: query.page.unwrap_or(1),
            page_size: query.page_size.unwrap_or(100),
        },
    )
    .await
    .map_err(map_filter_error)?;

    Ok(Json(
        serde_json::to_value(response).map_err(ApiError::internal)?,
    ))
}

async fn get_config(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let response = fetch_config_document(&state.bot_config_path).map_err(ApiError::internal)?;
    Ok(Json(
        serde_json::to_value(response).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct MarketQuery {
    include_timeline: Option<bool>,
}

async fn get_market_detail(
    State(state): State<Arc<ApiState>>,
    Path(condition_id): Path<String>,
    Query(query): Query<MarketQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let include_timeline = query.include_timeline.unwrap_or(false);
    let market = fetch_market_detail(&state.pool, &condition_id, include_timeline)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("market not found"))?;
    Ok(Json(
        serde_json::to_value(market).map_err(ApiError::internal)?,
    ))
}

async fn get_trace_detail(
    State(state): State<Arc<ApiState>>,
    Path(trace_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let trace = fetch_trace_detail(&state.pool, &trace_id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("trace not found"))?;
    Ok(Json(
        serde_json::to_value(trace).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    trace_id: Option<String>,
    condition_id: Option<String>,
    event_type: Option<String>,
    before_id: Option<i64>,
    limit: Option<i64>,
}

async fn get_events(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let events = fetch_events(
        &state.pool,
        EventFilter {
            trace_id: query.trace_id,
            condition_id: query.condition_id,
            event_type: query.event_type,
            before_id: query.before_id,
            limit: query.limit.unwrap_or(200),
        },
    )
    .await
    .map_err(|error| {
        let message = error.to_string();
        if message.contains("invalid event_type filter") {
            ApiError::bad_request(message)
        } else {
            ApiError::internal(error)
        }
    })?;

    Ok(Json(
        serde_json::to_value(events).map_err(ApiError::internal)?,
    ))
}

async fn get_polymarket_url(
    Query(query): Query<PolymarketUrlQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let url = resolve_polymarket_url(&query.market_slug)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(json!({ "url": url })))
}

async fn ws_live(
    State(state): State<Arc<ApiState>>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    Ok(upgrade.on_upgrade(move |socket| handle_socket(socket, state)))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<ApiState>) {
    if let Ok(Some(overview)) = fetch_overview(&state.pool).await {
        let frame = json!({
            "channel": "overview",
            "payload": overview
        });
        if socket
            .send(Message::Text(frame.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
    }

    let mut receiver = state.broadcaster.subscribe();
    loop {
        tokio::select! {
            message = receiver.recv() => {
                match message {
                    Ok(frame) => {
                        if socket.send(Message::Text(serde_json::to_string(&frame).unwrap_or_else(|_| "{}".to_string()).into())).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => return,
                }
            }
        }
    }
}

async fn resolve_polymarket_url(market_slug: &str) -> Result<Option<String>> {
    if market_slug.trim().is_empty() {
        return Ok(None);
    }

    let fallback = format!("https://polymarket.com/event/{market_slug}");
    let gamma_url = format!("https://gamma-api.polymarket.com/markets/slug/{market_slug}");
    let response = match reqwest::get(&gamma_url).await {
        Ok(response) => response,
        Err(_) => return Ok(Some(fallback)),
    };

    if !response.status().is_success() {
        return Ok(Some(fallback));
    }

    let market: GammaMarketResponse = match response.json().await {
        Ok(market) => market,
        Err(_) => return Ok(Some(fallback)),
    };

    let market_result_slug = market.slug.clone();
    let Some(event_slug) = market.events.into_iter().find_map(|event| {
        if event.slug.trim().is_empty() {
            None
        } else {
            Some(event.slug)
        }
    }) else {
        return Ok(Some(fallback));
    };

    if event_slug == market_result_slug {
        return Ok(Some(fallback));
    }

    Ok(Some(format!(
        "https://polymarket.com/event/{event_slug}/{}",
        market_slug
    )))
}

fn spa_service(web_dist: PathBuf) -> Router {
    let index = web_dist.join("index.html");
    if index.exists() {
        let service = ServeDir::new(&web_dist).not_found_service(ServeFile::new(index));
        Router::new().fallback_service(service)
    } else {
        Router::new().fallback(get(|| async {
            Html(
                r#"<!doctype html><html><body style="font-family:Georgia,serif;padding:32px;background:#f7f0e3;color:#221d1a"><h1>SpreadEater Monitor</h1><p>The SPA build is not present yet. Run <code>npm install</code> and <code>npm run build</code> in <code>crates/spreadeater-monitor/web</code>.</p></body></html>"#,
            )
        }))
    }
}

fn map_filter_error(error: anyhow::Error) -> ApiError {
    let message = error.to_string();
    if message.contains("invalid ") {
        ApiError::bad_request(message)
    } else {
        ApiError::internal(error)
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message
            })),
        )
            .into_response()
    }
}
