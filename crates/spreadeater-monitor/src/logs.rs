use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::time::{sleep, Duration};

use crate::dto::BotErrorLogEntry;
use crate::store::LiveBroadcaster;

const LOG_SCAN_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogEncoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

#[derive(Clone)]
pub struct BotLogTailer {
    pool: PgPool,
    log_path: PathBuf,
    broadcaster: Option<LiveBroadcaster>,
}

struct ParsedBotLogLine {
    parsed_at: Option<DateTime<Utc>>,
    level: Option<String>,
    message: String,
    raw_line: String,
}

impl BotLogTailer {
    pub fn new(pool: PgPool, log_path: PathBuf, broadcaster: Option<LiveBroadcaster>) -> Self {
        Self {
            pool,
            log_path,
            broadcaster,
        }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            if let Err(error) = self.ingest_once().await {
                tracing::error!(?error, path = %self.log_path.display(), "bot log ingest failed");
            }
            sleep(LOG_SCAN_INTERVAL).await;
        }
    }

    pub async fn ingest_once(&self) -> Result<usize> {
        if fs::metadata(&self.log_path).await.is_err() {
            return Ok(0);
        }

        let metadata = fs::metadata(&self.log_path).await?;
        let mut offset = self.current_offset().await?;
        if metadata.len() < offset as u64 {
            offset = 0;
            self.store_offset(0).await?;
        }

        let encoding = detect_log_encoding(&self.log_path).await?;

        if matches!(encoding, LogEncoding::Utf16Le | LogEncoding::Utf16Be) {
            return self.ingest_utf16(offset, encoding).await;
        }

        let file = fs::File::open(&self.log_path).await?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::Start(offset as u64)).await?;

        let mut inserted = 0usize;
        let mut current_offset = offset;
        let mut line = Vec::new();
        let mut strip_bom = offset == 0;

        loop {
            line.clear();
            let bytes = reader.read_until(b'\n', &mut line).await?;
            if bytes == 0 {
                break;
            }

            current_offset += bytes as i64;
            let decoded = decode_utf8ish_line(&line, strip_bom);
            strip_bom = false;
            let parsed = parse_bot_log_line(&decoded);
            if !should_store_log_line(&parsed) {
                continue;
            }

            if let Some(entry) = self.insert_line(current_offset, parsed).await? {
                inserted += 1;
                if let Some(broadcaster) = &self.broadcaster {
                    broadcaster.send("errors", &entry)?;
                }
            }
        }

        if current_offset != offset {
            self.store_offset(current_offset).await?;
        }

        Ok(inserted)
    }

    async fn ingest_utf16(&self, offset: i64, encoding: LogEncoding) -> Result<usize> {
        let mut file = fs::File::open(&self.log_path).await?;
        file.seek(std::io::SeekFrom::Start(offset as u64)).await?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        if bytes.is_empty() {
            return Ok(0);
        }

        let mut inserted = 0usize;
        let mut current_offset = offset;
        let mut line_units = Vec::new();
        let mut line_bytes = 0i64;
        let mut strip_bom = offset == 0;

        for chunk in bytes.chunks_exact(2) {
            let unit = match encoding {
                LogEncoding::Utf16Le => u16::from_le_bytes([chunk[0], chunk[1]]),
                LogEncoding::Utf16Be => u16::from_be_bytes([chunk[0], chunk[1]]),
                LogEncoding::Utf8 => unreachable!(),
            };

            line_units.push(unit);
            line_bytes += 2;

            if unit == b'\n' as u16 {
                let decoded = decode_utf16_line(&line_units, strip_bom);
                strip_bom = false;
                current_offset += line_bytes;
                let parsed = parse_bot_log_line(&decoded);
                if should_store_log_line(&parsed) {
                    if let Some(entry) = self.insert_line(current_offset, parsed).await? {
                        inserted += 1;
                        if let Some(broadcaster) = &self.broadcaster {
                            broadcaster.send("errors", &entry)?;
                        }
                    }
                }
                line_units.clear();
                line_bytes = 0;
            }
        }

        let remainder = bytes.len() % 2;
        if !line_units.is_empty() || remainder > 0 {
            let decoded = decode_utf16_line(&line_units, strip_bom);
            current_offset += line_bytes + remainder as i64;
            let parsed = parse_bot_log_line(&decoded);
            if should_store_log_line(&parsed) {
                if let Some(entry) = self.insert_line(current_offset, parsed).await? {
                    inserted += 1;
                    if let Some(broadcaster) = &self.broadcaster {
                        broadcaster.send("errors", &entry)?;
                    }
                }
            }
        }

        if current_offset != offset {
            self.store_offset(current_offset).await?;
        }

        Ok(inserted)
    }

    async fn current_offset(&self) -> Result<i64> {
        let value = sqlx::query_scalar::<_, i64>(
            "SELECT byte_offset FROM bot_log_offsets WHERE log_path = $1",
        )
        .bind(path_string(&self.log_path))
        .fetch_optional(&self.pool)
        .await?;

        Ok(value.unwrap_or(0))
    }

    async fn store_offset(&self, offset: i64) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bot_log_offsets (log_path, byte_offset, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (log_path) DO UPDATE
            SET byte_offset = EXCLUDED.byte_offset,
                updated_at = NOW()
            "#,
        )
        .bind(path_string(&self.log_path))
        .bind(offset)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn insert_line(
        &self,
        byte_offset: i64,
        parsed: ParsedBotLogLine,
    ) -> Result<Option<BotErrorLogEntry>> {
        let row = sqlx::query(
            r#"
            INSERT INTO bot_error_logs (
                log_path,
                byte_offset,
                parsed_at,
                level,
                message,
                raw_line
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, log_path, byte_offset, parsed_at, level, message, raw_line, created_at
            "#,
        )
        .bind(path_string(&self.log_path))
        .bind(byte_offset)
        .bind(parsed.parsed_at)
        .bind(parsed.level)
        .bind(parsed.message)
        .bind(parsed.raw_line)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| BotErrorLogEntry {
            id: row.get("id"),
            log_path: row.get("log_path"),
            byte_offset: row.get("byte_offset"),
            parsed_at: row.get("parsed_at"),
            level: row.get("level"),
            message: row.get("message"),
            raw_line: row.get("raw_line"),
            created_at: row.get("created_at"),
        }))
    }
}

fn parse_bot_log_line(line: &str) -> ParsedBotLogLine {
    let raw_line = line.trim_end_matches(['\r', '\n']).to_string();
    let mut parts = raw_line.split_whitespace();
    let first = parts.next();
    let second = parts.next();

    let parsed_at = first
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    let level = second.and_then(normalize_level_token);

    if parsed_at.is_some() && level.is_some() {
        let prefix = format!(
            "{} {}",
            first.unwrap_or_default(),
            second.unwrap_or_default()
        );
        let message = raw_line
            .strip_prefix(&prefix)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(raw_line.as_str())
            .to_string();
        return ParsedBotLogLine {
            parsed_at,
            level,
            message,
            raw_line,
        };
    }

    ParsedBotLogLine {
        parsed_at: None,
        level: None,
        message: raw_line.clone(),
        raw_line,
    }
}

fn normalize_level_token(token: &str) -> Option<String> {
    let normalized = token
        .trim_matches(|value: char| !value.is_ascii_alphabetic())
        .to_ascii_lowercase();
    match normalized.as_str() {
        "trace" | "debug" | "info" | "warn" | "warning" | "error" | "critical" => {
            Some(if normalized == "warning" {
                "warn".to_string()
            } else {
                normalized
            })
        }
        _ => None,
    }
}

fn should_store_log_line(parsed: &ParsedBotLogLine) -> bool {
    if parsed.raw_line.trim().is_empty() {
        return false;
    }

    match parsed.level.as_deref() {
        Some("error" | "critical" | "warn") | None => true,
        Some(_) => false,
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

async fn detect_log_encoding(path: &Path) -> Result<LogEncoding> {
    let mut file = fs::File::open(path).await?;
    let mut prefix = [0u8; 4];
    let read = file.read(&mut prefix).await?;
    Ok(detect_log_encoding_from_prefix(&prefix[..read]))
}

fn detect_log_encoding_from_prefix(prefix: &[u8]) -> LogEncoding {
    if prefix.starts_with(&[0xFF, 0xFE]) {
        return LogEncoding::Utf16Le;
    }
    if prefix.starts_with(&[0xFE, 0xFF]) {
        return LogEncoding::Utf16Be;
    }

    if prefix.len() >= 4 {
        let even_nuls = prefix.iter().step_by(2).filter(|&&byte| byte == 0).count();
        let odd_nuls = prefix
            .iter()
            .skip(1)
            .step_by(2)
            .filter(|&&byte| byte == 0)
            .count();

        if odd_nuls >= 2 && even_nuls == 0 {
            return LogEncoding::Utf16Le;
        }
        if even_nuls >= 2 && odd_nuls == 0 {
            return LogEncoding::Utf16Be;
        }
    }

    LogEncoding::Utf8
}

fn decode_utf8ish_line(bytes: &[u8], strip_bom: bool) -> String {
    let mut decoded = String::from_utf8_lossy(bytes).into_owned();
    if strip_bom {
        decoded = decoded.trim_start_matches('\u{feff}').to_string();
    }
    decoded
}

fn decode_utf16_line(units: &[u16], strip_bom: bool) -> String {
    let mut slice = units;
    if strip_bom && matches!(units.first(), Some(0xFEFF)) {
        slice = &units[1..];
    }
    String::from_utf16_lossy(slice)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_utf16_line, detect_log_encoding_from_prefix, parse_bot_log_line,
        should_store_log_line, LogEncoding,
    };

    #[test]
    fn parses_tracing_line_with_timestamp_and_level() {
        let parsed =
            parse_bot_log_line("2026-03-11T16:32:00Z ERROR hedge verification failed for market");
        assert_eq!(parsed.level.as_deref(), Some("error"));
        assert_eq!(parsed.message, "hedge verification failed for market");
        assert!(parsed.parsed_at.is_some());
    }

    #[test]
    fn keeps_unstructured_lines_for_panics() {
        let parsed = parse_bot_log_line("thread 'main' panicked at something bad");
        assert_eq!(parsed.level, None);
        assert_eq!(parsed.message, "thread 'main' panicked at something bad");
        assert!(should_store_log_line(&parsed));
    }

    #[test]
    fn filters_info_lines_out() {
        let parsed = parse_bot_log_line("2026-03-11T16:32:00Z INFO cycle finished");
        assert!(!should_store_log_line(&parsed));
    }

    #[test]
    fn detects_utf16le_bom_logs() {
        assert_eq!(
            detect_log_encoding_from_prefix(&[0xFF, 0xFE, b'e', 0x00]),
            LogEncoding::Utf16Le
        );
    }

    #[test]
    fn decodes_utf16le_line_lossily() {
        let units = [
            0xFEFF,
            '2' as u16,
            '0' as u16,
            '2' as u16,
            '6' as u16,
            '-' as u16,
            '0' as u16,
            '3' as u16,
            '-' as u16,
            '1' as u16,
            '1' as u16,
            'T' as u16,
            '1' as u16,
            '6' as u16,
            ':' as u16,
            '3' as u16,
            '2' as u16,
            ':' as u16,
            '0' as u16,
            '0' as u16,
            'Z' as u16,
            ' ' as u16,
            'E' as u16,
            'R' as u16,
            'R' as u16,
            'O' as u16,
            'R' as u16,
            ' ' as u16,
            'b' as u16,
            'a' as u16,
            'd' as u16,
            '\n' as u16,
        ];
        let decoded = decode_utf16_line(&units, true);
        let parsed = parse_bot_log_line(&decoded);
        assert_eq!(parsed.level.as_deref(), Some("error"));
        assert_eq!(parsed.message, "bad");
    }
}
