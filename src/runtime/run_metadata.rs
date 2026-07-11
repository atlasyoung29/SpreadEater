use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;

use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunMetadata {
    pub run_id: String,
    pub pid: u32,
    pub mode: String,
    pub started_at: DateTime<Utc>,
    pub events_path: String,
    pub event_log_dir: String,
    pub config_path: String,
    pub config_hash: String,
    pub cash_reserve_usd: Decimal,
}

struct RunMetadataPaths {
    current_run_path: PathBuf,
    run_metadata_path: PathBuf,
    events_path: PathBuf,
    event_log_dir: PathBuf,
}

pub async fn write_startup_run_metadata(
    config: &Config,
    run_id: &str,
    mode: &str,
    started_at: DateTime<Utc>,
    config_path: &str,
) -> Result<RunMetadata> {
    let paths = resolve_run_metadata_paths(&config.observability.event_log_dir, run_id);
    let metadata = build_run_metadata(config, run_id, mode, started_at, config_path, &paths)?;

    if let Some(parent) = paths.current_run_path.parent() {
        fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create current run directory at {}",
                parent.display()
            )
        })?;
    }
    fs::create_dir_all(paths.run_metadata_path.parent().unwrap())
        .await
        .with_context(|| {
            format!(
                "failed to create run metadata directory at {}",
                paths.run_metadata_path.parent().unwrap().display()
            )
        })?;

    write_json_file(&paths.current_run_path, &metadata).await?;
    write_json_file(&paths.run_metadata_path, &metadata).await?;

    Ok(metadata)
}

pub fn config_hash(config: &Config) -> Result<String> {
    let canonical_value = canonicalize_json_value(serde_json::to_value(config)?);
    let canonical_bytes = serde_json::to_vec(&canonical_value)?;
    Ok(hex::encode(Sha256::digest(canonical_bytes)))
}

fn build_run_metadata(
    config: &Config,
    run_id: &str,
    mode: &str,
    started_at: DateTime<Utc>,
    config_path: &str,
    paths: &RunMetadataPaths,
) -> Result<RunMetadata> {
    Ok(RunMetadata {
        run_id: run_id.to_string(),
        pid: std::process::id(),
        mode: mode.to_string(),
        started_at,
        events_path: paths.events_path.to_string_lossy().into_owned(),
        event_log_dir: paths.event_log_dir.to_string_lossy().into_owned(),
        config_path: resolve_absolute_path(Path::new(config_path))
            .to_string_lossy()
            .into_owned(),
        config_hash: config_hash(config)?,
        cash_reserve_usd: config.risk.cash_reserve,
    })
}

fn resolve_run_metadata_paths(event_log_dir: &str, run_id: &str) -> RunMetadataPaths {
    let event_log_dir = resolve_absolute_path(Path::new(event_log_dir));
    let run_dir = event_log_dir.join(run_id);
    let current_run_path = event_log_dir
        .parent()
        .map(|parent| parent.join("current_run.json"))
        .unwrap_or_else(|| PathBuf::from("current_run.json"));

    RunMetadataPaths {
        current_run_path,
        run_metadata_path: run_dir.join("run_metadata.json"),
        events_path: run_dir.join("events.jsonl"),
        event_log_dir,
    }
}

fn resolve_absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

async fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let body = serde_json::to_string_pretty(value)?;
    fs::write(path, body)
        .await
        .with_context(|| format!("failed to write {}", path.display()))
}

fn canonicalize_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut keys: Vec<_> = object.into_iter().collect();
            keys.sort_by(|(left, _), (right, _)| left.cmp(right));

            let mut sorted = serde_json::Map::new();
            for (key, value) in keys {
                sorted.insert(key, canonicalize_json_value(value));
            }
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(canonicalize_json_value)
                .collect::<Vec<_>>(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_hash_is_stable_for_equivalent_configs() {
        let config = Config::default();

        let first = config_hash(&config).unwrap();
        let second = config_hash(&config).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }
}
