use std::io::{self, Stdout};
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use crossterm::event::{self, Event as CEvent, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;
use reqwest::Client;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;

use crate::config::TuiConfig;
use crate::dto::{
    EventListItem, LiveFrame, MarketDetailResponse, MarketSummary, OverviewResponse, PageResponse,
    TraceDetailResponse,
};

pub async fn run_tui(config: TuiConfig) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    let controller = tokio::spawn(controller_loop(config.clone(), command_rx, event_tx));
    command_tx.send(ControllerCommand::Refresh)?;

    let mut app = AppState::default();
    let mut last_draw = Instant::now();

    loop {
        while let Ok(update) = event_rx.try_recv() {
            app.apply(update, &command_tx);
        }

        if last_draw.elapsed() >= Duration::from_millis(100) {
            terminal.draw(|frame| draw_ui(frame, &app))?;
            last_draw = Instant::now();
        }

        if event::poll(Duration::from_millis(50))? {
            if let CEvent::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('j') | KeyCode::Down => app.move_selection(1, &command_tx),
                    KeyCode::Char('k') | KeyCode::Up => app.move_selection(-1, &command_tx),
                    KeyCode::Enter => app.open_trace(&command_tx),
                    KeyCode::Tab => app.toggle_detail_mode(),
                    KeyCode::Char('r') => {
                        let _ = command_tx.send(ControllerCommand::Refresh);
                        let _ = command_tx.send(ControllerCommand::ReconnectWs);
                    }
                    _ => {}
                }
            }
        }
    }

    controller.abort();
    restore_terminal(&mut terminal)?;
    Ok(())
}

#[derive(Default)]
struct AppState {
    overview: Option<OverviewResponse>,
    market_list: Vec<MarketSummary>,
    selected_market: Option<MarketDetailResponse>,
    selected_trace: Option<TraceDetailResponse>,
    alerts: Vec<EventListItem>,
    selected_index: usize,
    selected_condition_id: Option<String>,
    selected_trace_id: Option<String>,
    detail_mode: DetailMode,
    api_error: Option<String>,
    ws_connected: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum DetailMode {
    #[default]
    Market,
    Trace,
}

impl AppState {
    fn apply(
        &mut self,
        update: ControllerEvent,
        command_tx: &mpsc::UnboundedSender<ControllerCommand>,
    ) {
        match update {
            ControllerEvent::Overview(overview) => {
                self.overview = Some(overview);
                self.api_error = None;
            }
            ControllerEvent::MarketList(markets) => {
                let mut should_select = self.market_list.is_empty() && !markets.is_empty();
                if let Some(condition_id) = &self.selected_condition_id {
                    if let Some(index) = markets
                        .iter()
                        .position(|market| &market.condition_id == condition_id)
                    {
                        self.selected_index = index;
                    } else {
                        self.selected_index = 0;
                        self.selected_condition_id =
                            markets.first().map(|market| market.condition_id.clone());
                        self.selected_market = None;
                        self.selected_trace = None;
                        self.selected_trace_id = None;
                        should_select = !markets.is_empty();
                    }
                }
                self.market_list = markets;
                self.api_error = None;
                if should_select {
                    self.selected_index = 0;
                    self.select_current_market(command_tx);
                }
            }
            ControllerEvent::Market(market) => {
                if self
                    .selected_condition_id
                    .as_deref()
                    .is_none_or(|selected| selected == market.condition_id.as_str())
                {
                    self.selected_market = Some(market);
                    self.api_error = None;
                }
            }
            ControllerEvent::Trace(trace) => {
                if self
                    .selected_trace_id
                    .as_deref()
                    .is_none_or(|selected| selected == trace.trace_id.as_str())
                {
                    self.selected_trace_id = Some(trace.trace_id.clone());
                    self.selected_trace = Some(trace);
                    self.detail_mode = DetailMode::Trace;
                    self.api_error = None;
                }
            }
            ControllerEvent::Alert(alert) => {
                self.alerts.insert(0, alert);
                self.alerts.truncate(10);
            }
            ControllerEvent::ApiError(message) => {
                self.api_error = Some(message);
            }
            ControllerEvent::WsStatus(connected) => {
                self.ws_connected = connected;
            }
        }
    }

    fn move_selection(
        &mut self,
        delta: isize,
        command_tx: &mpsc::UnboundedSender<ControllerCommand>,
    ) {
        if self.market_list.is_empty() {
            return;
        }
        let len = self.market_list.len() as isize;
        let current = self.selected_index as isize;
        self.selected_index = (current + delta).rem_euclid(len) as usize;
        self.detail_mode = DetailMode::Market;
        self.selected_trace = None;
        self.selected_trace_id = None;
        self.select_current_market(command_tx);
    }

    fn select_current_market(&mut self, command_tx: &mpsc::UnboundedSender<ControllerCommand>) {
        if let Some(market) = self.market_list.get(self.selected_index) {
            self.selected_condition_id = Some(market.condition_id.clone());
            let _ = command_tx.send(ControllerCommand::LoadMarket(market.condition_id.clone()));
        }
    }

    fn open_trace(&mut self, command_tx: &mpsc::UnboundedSender<ControllerCommand>) {
        if let Some(market) = &self.selected_market {
            if let Some(trace_id) = market.recent_traces.first() {
                self.selected_trace_id = Some(trace_id.clone());
                self.detail_mode = DetailMode::Trace;
                let _ = command_tx.send(ControllerCommand::LoadTrace(trace_id.clone()));
            }
        } else {
            self.detail_mode = DetailMode::Market;
        }
    }

    fn toggle_detail_mode(&mut self) {
        self.detail_mode = if self.detail_mode == DetailMode::Market {
            if self.selected_trace.is_some() {
                DetailMode::Trace
            } else {
                DetailMode::Market
            }
        } else {
            DetailMode::Market
        };
    }
}

enum ControllerCommand {
    Refresh,
    LoadMarket(String),
    LoadTrace(String),
    ReconnectWs,
}

enum ControllerEvent {
    Overview(OverviewResponse),
    MarketList(Vec<MarketSummary>),
    Market(MarketDetailResponse),
    Trace(TraceDetailResponse),
    Alert(EventListItem),
    ApiError(String),
    WsStatus(bool),
}

async fn controller_loop(
    config: TuiConfig,
    mut commands: mpsc::UnboundedReceiver<ControllerCommand>,
    events: mpsc::UnboundedSender<ControllerEvent>,
) {
    let client = Client::new();
    let mut selected_market: Option<String> = None;
    let mut selected_trace: Option<String> = None;
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    let mut ws_task = Some(spawn_ws_task(config.clone(), events.clone()));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(error) = refresh_all(&client, &config, &events, selected_market.as_deref(), selected_trace.as_deref()).await {
                    let _ = events.send(ControllerEvent::ApiError(error.to_string()));
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return;
                };
                match command {
                    ControllerCommand::Refresh => {
                        if let Err(error) = refresh_all(&client, &config, &events, selected_market.as_deref(), selected_trace.as_deref()).await {
                            let _ = events.send(ControllerEvent::ApiError(error.to_string()));
                        }
                    }
                    ControllerCommand::LoadMarket(condition_id) => {
                        selected_market = Some(condition_id.clone());
                        match fetch_market(&client, &config, &condition_id).await {
                            Ok(market) => {
                                let _ = events.send(ControllerEvent::Market(market));
                            }
                            Err(error) => {
                                let _ = events.send(ControllerEvent::ApiError(error.to_string()));
                            }
                        }
                    }
                    ControllerCommand::LoadTrace(trace_id) => {
                        selected_trace = Some(trace_id.clone());
                        match fetch_trace(&client, &config, &trace_id).await {
                            Ok(trace) => {
                                let _ = events.send(ControllerEvent::Trace(trace));
                            }
                            Err(error) => {
                                let _ = events.send(ControllerEvent::ApiError(error.to_string()));
                            }
                        }
                    }
                    ControllerCommand::ReconnectWs => {
                        if let Some(task) = ws_task.take() {
                            task.abort();
                        }
                        ws_task = Some(spawn_ws_task(config.clone(), events.clone()));
                    }
                }
            }
        }
    }
}

async fn refresh_all(
    client: &Client,
    config: &TuiConfig,
    events: &mpsc::UnboundedSender<ControllerEvent>,
    market: Option<&str>,
    trace: Option<&str>,
) -> Result<()> {
    let overview = fetch_overview_http(client, config).await?;
    let _ = events.send(ControllerEvent::Overview(overview));
    let watchlist = fetch_watchlist_http(client, config).await?;
    let _ = events.send(ControllerEvent::MarketList(watchlist.items));

    if let Some(condition_id) = market {
        let market = fetch_market(client, config, condition_id).await?;
        let _ = events.send(ControllerEvent::Market(market));
    }

    if let Some(trace_id) = trace {
        let trace = fetch_trace(client, config, trace_id).await?;
        let _ = events.send(ControllerEvent::Trace(trace));
    }

    Ok(())
}

fn spawn_ws_task(
    config: TuiConfig,
    events: mpsc::UnboundedSender<ControllerEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let ws_url = match config.ws_live_url() {
            Ok(url) => url,
            Err(error) => {
                let _ = events.send(ControllerEvent::ApiError(error.to_string()));
                return;
            }
        };

        let _ = events.send(ControllerEvent::WsStatus(false));
        match connect_async(ws_url.as_str()).await {
            Ok((stream, _)) => {
                let _ = events.send(ControllerEvent::WsStatus(true));
                let (_, mut read) = stream.split();
                use futures_util::StreamExt;

                while let Some(message) = read.next().await {
                    match message {
                        Ok(message) if message.is_text() => {
                            if let Ok(frame) =
                                serde_json::from_str::<LiveFrame>(message.to_text().unwrap_or(""))
                            {
                                match frame.channel.as_str() {
                                    "overview" => {
                                        if let Ok(overview) =
                                            serde_json::from_value::<OverviewResponse>(
                                                frame.payload,
                                            )
                                        {
                                            let _ =
                                                events.send(ControllerEvent::Overview(overview));
                                        }
                                    }
                                    "market" => {
                                        if let Ok(market) =
                                            serde_json::from_value::<MarketDetailResponse>(
                                                frame.payload,
                                            )
                                        {
                                            let _ = events.send(ControllerEvent::Market(market));
                                        }
                                    }
                                    "trace" => {
                                        if let Ok(trace) =
                                            serde_json::from_value::<TraceDetailResponse>(
                                                frame.payload,
                                            )
                                        {
                                            let _ = events.send(ControllerEvent::Trace(trace));
                                        }
                                    }
                                    "alerts" => {
                                        if let Ok(alert) =
                                            serde_json::from_value::<EventListItem>(frame.payload)
                                        {
                                            let _ = events.send(ControllerEvent::Alert(alert));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = events.send(ControllerEvent::ApiError(error.to_string()));
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                let _ = events.send(ControllerEvent::ApiError(error.to_string()));
            }
        }

        let _ = events.send(ControllerEvent::WsStatus(false));
    })
}

async fn fetch_overview_http(client: &Client, config: &TuiConfig) -> Result<OverviewResponse> {
    Ok(client
        .get(format!("{}/api/v1/overview", config.api_base_url))
        .send()
        .await?
        .error_for_status()?
        .json::<OverviewResponse>()
        .await?)
}

async fn fetch_watchlist_http(
    client: &Client,
    config: &TuiConfig,
) -> Result<PageResponse<MarketSummary>> {
    Ok(client
        .get(format!("{}/api/v1/watchlist", config.api_base_url))
        .query(&[("page_size", "250")])
        .send()
        .await?
        .error_for_status()?
        .json::<PageResponse<MarketSummary>>()
        .await?)
}

async fn fetch_market(
    client: &Client,
    config: &TuiConfig,
    condition_id: &str,
) -> Result<MarketDetailResponse> {
    Ok(client
        .get(format!(
            "{}/api/v1/markets/{}",
            config.api_base_url, condition_id
        ))
        .query(&[("include_timeline", "true")])
        .send()
        .await?
        .error_for_status()?
        .json::<MarketDetailResponse>()
        .await?)
}

async fn fetch_trace(
    client: &Client,
    config: &TuiConfig,
    trace_id: &str,
) -> Result<TraceDetailResponse> {
    Ok(client
        .get(format!(
            "{}/api/v1/traces/{}",
            config.api_base_url, trace_id
        ))
        .send()
        .await?
        .error_for_status()?
        .json::<TraceDetailResponse>()
        .await?)
}

fn draw_ui(frame: &mut ratatui::Frame<'_>, app: &AppState) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(12),
            Constraint::Length(8),
        ])
        .split(frame.area());

    draw_header(frame, outer[0], app);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
        .split(outer[1]);

    draw_market_list(frame, body[0], app);
    draw_detail(frame, body[1], app);
    draw_alerts(frame, outer[2], app);
}

fn draw_header(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &AppState) {
    let mut text = vec![Line::from(vec![Span::styled(
        "SpreadEater Monitor",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )])];

    if let Some(overview) = &app.overview {
        let run_status = overview_run_status(overview, app.ws_connected);
        let health_label = if run_status == "stale" && overview.observer_health == "healthy" {
            "stale"
        } else {
            overview.observer_health.as_str()
        };
        text.push(Line::from(format!(
            "run={}  health={}  status={}  lag={}ms  capital_deployed={} / cap={}  watched={}  open_orders={}  ws={}",
            overview.run_id,
            health_label,
            run_status,
            overview.index_lag_ms,
            format_money(overview.committed_capital_usd),
            format_optional_money(overview.max_total_exposure_usd),
            overview.active_markets,
            overview.open_orders,
            if app.ws_connected {
                "online"
            } else {
                "offline"
            }
        )));
    } else {
        text.push(Line::from("Waiting for API overview..."));
    }

    if let Some(error) = &app.api_error {
        text.push(Line::from(Span::styled(
            format!("degraded: {error}"),
            Style::default().fg(Color::Red),
        )));
    }

    let widget = Paragraph::new(text).block(Block::default().borders(Borders::ALL));
    frame.render_widget(widget, area);
}

fn draw_market_list(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &AppState) {
    let items = app
        .market_list
        .iter()
        .map(|market| {
            let title = market
                .question
                .clone()
                .unwrap_or_else(|| market.condition_id.clone());
            let subtitle = format!(
                "{} | ord={} | sh={} | val={} | rew={} | yld={}",
                market
                    .decision_status
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                market.open_order_count,
                format_decimal(market.open_order_share_size),
                format_money(market.open_order_notional_usd),
                format_optional_money(market.expected_reward_usd_day),
                format_optional_yield(
                    market.expected_reward_usd_day,
                    if market.open_order_notional_usd > Decimal::ZERO {
                        Some(market.open_order_notional_usd)
                    } else {
                        None
                    },
                )
            );
            ListItem::new(vec![Line::from(title), Line::from(subtitle)])
        })
        .collect::<Vec<_>>();

    let has_items = !items.is_empty();
    let list = List::new(items)
        .block(
            Block::default()
                .title("Watched Markets")
                .borders(Borders::ALL),
        )
        .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::White));

    let mut state = ListState::default();
    if has_items {
        state.select(Some(app.selected_index));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_detail(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &AppState) {
    let lines = if app.detail_mode == DetailMode::Trace {
        if let Some(trace) = &app.selected_trace {
            let mut lines = vec![
                Line::from(Span::styled(
                    format!("Trace {}", trace.trace_id),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(format!("status: {}", trace.status)),
                Line::from(format!(
                    "market: {}",
                    trace.market.question.clone().unwrap_or_else(|| trace
                        .market
                        .condition_id
                        .clone()
                        .unwrap_or_default())
                )),
                Line::from(format!(
                    "orders={} fills={} hedges={}",
                    trace.orders.len(),
                    trace.fills.len(),
                    trace.hedges.len()
                )),
            ];
            if let Some(decision) = &trace.decision {
                lines.push(Line::from(format!(
                    "edge={} ({}) reward={} yield={} capital={} quote={} would_trade={}",
                    format_optional_money(decision.expected_edge_usd),
                    format_optional_percent(decision.expected_edge_pct),
                    format_optional_money(decision.expected_reward_usd_day),
                    format_optional_yield(
                        decision.expected_reward_usd_day,
                        decision.committed_capital_usd,
                    ),
                    format_optional_money(decision.committed_capital_usd),
                    format_optional_number(decision.effective_quote_size),
                    format_optional_bool(decision.would_trade),
                )));
                if !decision.reasons.is_empty() {
                    lines.push(Line::from(format!(
                        "reasons: {}",
                        decision.reasons.join(" | ")
                    )));
                }
            }
            if let Some(neutrality) = &trace.neutrality {
                lines.push(Line::from(format!(
                    "neutrality: {} residual={} complete_sets={}",
                    neutrality.is_neutral,
                    format_decimal(neutrality.residual_exposure),
                    format_decimal(neutrality.complete_sets)
                )));
            }
            lines.push(Line::from("timeline:"));
            for item in trace.timeline.iter().take(12) {
                lines.push(Line::from(format!(
                    "{}  {}  {}",
                    item.occurred_at.format("%H:%M:%S"),
                    item.event_type,
                    item.reason_code.clone().unwrap_or_default()
                )));
            }
            lines
        } else {
            vec![Line::from(
                "No trace selected. Press Enter on a market to open its latest trace.",
            )]
        }
    } else if let Some(market) = &app.selected_market {
        let mut lines = vec![
            Line::from(Span::styled(
                market
                    .question
                    .clone()
                    .unwrap_or_else(|| market.condition_id.clone()),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(format!(
                "status={} edge={} ({}) reward={} yield={} hedge_cost={}",
                market
                    .decision_status
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                format_optional_money(market.expected_edge_usd),
                format_optional_percent(market.expected_edge_pct),
                format_optional_money(market.expected_reward_usd_day),
                format_optional_yield(
                    market.expected_reward_usd_day,
                    if market.open_order_notional_usd > Decimal::ZERO {
                        Some(market.open_order_notional_usd)
                    } else {
                        Some(market.committed_capital_usd)
                    },
                ),
                format_optional_money(market.expected_hedge_cost_usd)
            )),
            Line::from(format!(
                "capital={} open_orders={} shares={} open_notional={} yes={} no={} exposure={} neutral={}",
                format_money(market.committed_capital_usd),
                market.open_order_count,
                format_decimal(market.open_order_share_size),
                format_money(market.open_order_notional_usd),
                format_decimal(market.yes_size),
                format_decimal(market.no_size),
                format_decimal(market.net_exposure),
                format_optional_bool(Some(market.is_neutral))
            )),
            Line::from(format!(
                "latest_reason={}",
                market
                    .latest_reason
                    .clone()
                    .unwrap_or_else(|| "n/a".to_string())
            )),
            Line::from(format!(
                "recent traces: {}",
                if market.recent_traces.is_empty() {
                    "none".to_string()
                } else {
                    market.recent_traces.join(", ")
                }
            )),
            Line::from("recent events:"),
        ];

        for item in market.recent_events.iter().take(12) {
            lines.push(Line::from(format!(
                "{}  {}  {}",
                item.occurred_at.format("%H:%M:%S"),
                item.event_type,
                item.reason_code.clone().unwrap_or_default()
            )));
        }
        lines
    } else {
        vec![Line::from("Waiting for market detail...")]
    };

    let title = if app.detail_mode == DetailMode::Trace {
        "Trace Detail"
    } else {
        "Market Detail"
    };
    let widget = Paragraph::new(lines)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(widget, area);
}

fn draw_alerts(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &AppState) {
    let mut items = app
        .alerts
        .iter()
        .map(|alert| ListItem::new(format_activity(alert)))
        .collect::<Vec<_>>();

    if items.is_empty() {
        items.push(ListItem::new(if app.ws_connected {
            "No live order / execution activity yet."
        } else {
            "WebSocket offline. Live alerts unavailable."
        }));
    }

    let widget = List::new(items).block(Block::default().title("Activity").borders(Borders::ALL));
    frame.render_widget(widget, area);
}

fn format_activity(item: &EventListItem) -> String {
    let market = item
        .question
        .clone()
        .or_else(|| item.market_slug.clone())
        .or_else(|| item.condition_id.clone())
        .unwrap_or_else(|| "market".to_string());
    match item.event_type.as_str() {
        "order_submitted" => format!(
            "{}  order  {} {} @ {}  {}",
            item.occurred_at.format("%H:%M:%S"),
            payload_text(&item.payload, "side"),
            format_optional_number(payload_decimal(&item.payload, "size")),
            format_optional_money(payload_decimal(&item.payload, "price")),
            market
        ),
        "fill_detected" => format!(
            "{}  fill   {} {} @ {}  {}",
            item.occurred_at.format("%H:%M:%S"),
            payload_text(&item.payload, "outcome"),
            format_optional_number(payload_decimal(&item.payload, "fill_size")),
            format_optional_money(payload_decimal(&item.payload, "fill_price")),
            market
        ),
        _ => format!(
            "{}  {}  {}",
            item.occurred_at.format("%H:%M:%S"),
            item.event_type,
            market
        ),
    }
}

fn format_decimal(value: Decimal) -> String {
    format!("{:.2}", value.round_dp(2))
}

fn format_money(value: Decimal) -> String {
    format!("${}", format_decimal(value))
}

fn format_optional_number(value: Option<Decimal>) -> String {
    value
        .map(format_decimal)
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_money(value: Option<Decimal>) -> String {
    value.map(format_money).unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_percent(value: Option<Decimal>) -> String {
    value
        .map(|value| format!("{}%", format_decimal(value)))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_yield(reward: Option<Decimal>, capital: Option<Decimal>) -> String {
    match (reward, capital) {
        (Some(reward), Some(capital)) if capital > Decimal::ZERO => {
            format!(
                "{}%",
                format_decimal((reward / capital) * Decimal::new(100, 0))
            )
        }
        _ => "n/a".to_string(),
    }
}

fn format_optional_bool(value: Option<bool>) -> String {
    value
        .map(|value| if value { "yes" } else { "no" }.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn overview_run_status(overview: &OverviewResponse, ws_connected: bool) -> &'static str {
    if !ws_connected {
        return "offline";
    }

    let stale_after_secs = (overview.expected_cycle_interval_secs.max(45)) * 2;
    let age_secs = (Utc::now() - overview.last_event_at).num_seconds().max(0);

    if age_secs > stale_after_secs {
        "stale"
    } else {
        "live"
    }
}

fn payload_text(payload: &serde_json::Value, key: &str) -> String {
    payload
        .get(key)
        .map(|value| match value {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        })
        .unwrap_or_else(|| "n/a".to_string())
}

fn payload_decimal(payload: &serde_json::Value, key: &str) -> Option<Decimal> {
    let value = payload.get(key)?;
    match value {
        serde_json::Value::String(text) => Decimal::from_str(text).ok(),
        serde_json::Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Ok(Terminal::new(backend)?)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    #[test]
    fn moving_selection_switches_back_to_market_mode() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut app = AppState {
            overview: Some(sample_overview(2, 1)),
            market_list: vec![sample_market("cond-a"), sample_market("cond-b")],
            selected_index: 0,
            selected_condition_id: Some("cond-a".to_string()),
            selected_trace_id: Some("trace-a".to_string()),
            detail_mode: DetailMode::Trace,
            ..Default::default()
        };

        app.move_selection(1, &tx);

        assert_eq!(app.selected_index, 1);
        assert_eq!(app.selected_condition_id.as_deref(), Some("cond-b"));
        assert_eq!(app.detail_mode, DetailMode::Market);
        assert!(app.selected_trace_id.is_none());
    }

    #[test]
    fn unrelated_market_updates_do_not_replace_selection() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut app = AppState {
            overview: Some(sample_overview(1, 0)),
            market_list: vec![sample_market("cond-a")],
            selected_condition_id: Some("cond-a".to_string()),
            ..Default::default()
        };

        app.apply(ControllerEvent::Market(sample_market_detail("cond-a")), &tx);
        app.apply(ControllerEvent::Market(sample_market_detail("cond-b")), &tx);

        assert_eq!(
            app.selected_market
                .as_ref()
                .map(|market| market.condition_id.as_str()),
            Some("cond-a")
        );
    }

    fn sample_overview(active_markets: i64, open_orders: i64) -> OverviewResponse {
        OverviewResponse {
            run_id: "run-1".to_string(),
            mode: "dry-run".to_string(),
            observer_health: "healthy".to_string(),
            global_halt: false,
            risk_reason: None,
            user_stream_status: Some("connected".to_string()),
            user_stream_detail: None,
            subscribed_markets: Some(active_markets),
            managed_markets: Some(active_markets),
            producer_lag_ms: 0,
            index_lag_ms: 0,
            last_event_at: Utc::now(),
            expected_cycle_interval_secs: 300,
            active_markets,
            open_orders,
            committed_capital_usd: Decimal::ZERO,
            order_committed_usd: Some(Decimal::ZERO),
            position_committed_usd: Some(Decimal::ZERO),
            total_committed_usd: Some(Decimal::ZERO),
            api_balance_usd: Some(Decimal::ZERO),
            available_budget_usd: Some(Decimal::new(1500, 0)),
            competition_multiplier: Some(Decimal::new(12, 1)),
            max_total_exposure_usd: Some(Decimal::new(1500, 0)),
            unhedged_markets: 0,
            open_order_markets: open_orders,
            inventory_markets: 0,
            open_order_reward_usd_day: Decimal::ZERO,
            open_order_notional_usd: Decimal::ZERO,
            open_order_preview: Vec::new(),
            inventory_preview: Vec::new(),
            recent_history: Vec::new(),
            recent_errors: Vec::new(),
            recent_alerts: Vec::new(),
        }
    }

    fn sample_market(condition_id: &str) -> crate::dto::MarketSummary {
        crate::dto::MarketSummary {
            condition_id: condition_id.to_string(),
            market_slug: None,
            question: Some(condition_id.to_string()),
            decision_status: Some("approved".to_string()),
            expected_reward_usd_day: Some(Decimal::new(25, 2)),
            expected_edge_usd: Some(Decimal::new(12, 1)),
            expected_edge_pct: None,
            latest_reason: Some("waiting for tighter edge".to_string()),
            halted: false,
            halt_reason: None,
            open_order_count: 1,
            open_order_share_size: Decimal::new(75, 1),
            open_order_notional_usd: Decimal::new(42, 1),
            yes_size: Decimal::ZERO,
            no_size: Decimal::ZERO,
            net_exposure: Decimal::ZERO,
            complete_sets: Decimal::ZERO,
            is_neutral: true,
            last_event_at: Utc::now(),
        }
    }

    fn sample_market_detail(condition_id: &str) -> MarketDetailResponse {
        MarketDetailResponse {
            condition_id: condition_id.to_string(),
            run_id: "run-1".to_string(),
            market_slug: None,
            question: Some(condition_id.to_string()),
            decision_status: Some("approved".to_string()),
            expected_edge_usd: Some(Decimal::new(12, 1)),
            expected_edge_pct: None,
            expected_reward_usd_day: None,
            expected_hedge_cost_usd: None,
            committed_capital_usd: Decimal::ZERO,
            effective_quote_size: None,
            score_share: None,
            max_hedgeable_size: None,
            latest_reason: Some("waiting for tighter edge".to_string()),
            halted: false,
            halt_reason: None,
            open_order_count: 1,
            open_order_share_size: Decimal::new(75, 1),
            open_order_notional_usd: Decimal::new(42, 1),
            yes_size: Decimal::ZERO,
            no_size: Decimal::ZERO,
            net_exposure: Decimal::ZERO,
            complete_sets: Decimal::ZERO,
            is_neutral: true,
            recent_traces: Vec::new(),
            recent_events: Vec::new(),
        }
    }
}
