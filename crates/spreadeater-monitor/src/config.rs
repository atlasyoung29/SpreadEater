use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub database_url: String,
    pub event_log_dir: PathBuf,
    pub bind: SocketAddr,
    pub web_dist: PathBuf,
    pub bot_config_path: PathBuf,
    pub bot_log_path: PathBuf,
}

impl MonitorConfig {
    pub fn resolve(
        database_url: Option<String>,
        event_log_dir: PathBuf,
        bind: String,
        web_dist: PathBuf,
        bot_config_path: PathBuf,
        bot_log_path: PathBuf,
    ) -> Result<Self> {
        let database_url = database_url
            .or_else(|| env::var("DATABASE_URL").ok())
            .context("DATABASE_URL or --database-url is required")?;

        let bind = bind
            .parse()
            .with_context(|| format!("invalid bind address: {bind}"))?;

        Ok(Self {
            database_url,
            event_log_dir,
            bind,
            web_dist,
            bot_config_path,
            bot_log_path,
        })
    }

    pub fn api_base_url(&self) -> String {
        format!("http://{}", self.bind)
    }
}

#[derive(Debug, Clone)]
pub struct TuiConfig {
    pub api_base_url: String,
}

impl TuiConfig {
    pub fn resolve(api_base_url: String) -> Self {
        Self {
            api_base_url: api_base_url.trim_end_matches('/').to_string(),
        }
    }

    pub fn ws_live_url(&self) -> Result<url::Url> {
        let trimmed = self.api_base_url.trim_end_matches('/');
        let ws_base = if let Some(rest) = trimmed.strip_prefix("http://") {
            format!("ws://{rest}")
        } else if let Some(rest) = trimmed.strip_prefix("https://") {
            format!("wss://{rest}")
        } else {
            format!("ws://{trimmed}")
        };

        url::Url::parse(&format!("{ws_base}/ws/live"))
            .with_context(|| format!("invalid API base URL: {}", self.api_base_url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_uses_ws_for_http_base() {
        let config = TuiConfig::resolve("http://127.0.0.1:8080".to_string());
        assert_eq!(
            config.ws_live_url().unwrap().as_str(),
            "ws://127.0.0.1:8080/ws/live"
        );
    }

    #[test]
    fn ws_url_uses_wss_for_https_base() {
        let config = TuiConfig::resolve("https://monitor.local".to_string());
        assert_eq!(
            config.ws_live_url().unwrap().as_str(),
            "wss://monitor.local/ws/live"
        );
    }
}
