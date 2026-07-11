use std::time::Duration;

use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::WatchdogConfig;

/// Verdict from Polymarket status page assessment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusVerdict {
    Healthy,
    Degraded { reason: String },
    Critical { reason: String },
}

/// Instatus summary.json component schema.
#[derive(Debug, Deserialize)]
struct InstatusSummary {
    page: InstatusPage,
    #[serde(default)]
    components: Vec<InstatusComponent>,
    #[serde(default)]
    incidents: Vec<InstatusIncident>,
}

#[derive(Debug, Deserialize)]
struct InstatusPage {
    status: String,
}

#[derive(Debug, Deserialize)]
struct InstatusComponent {
    name: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct InstatusIncident {
    #[serde(default)]
    name: String,
    status: String,
    #[serde(default)]
    components: Vec<String>,
}

/// Polls the Polymarket Instatus status page and emits StatusVerdict.
pub struct StatusPoller {
    config: WatchdogConfig,
    client: reqwest::Client,
}

impl StatusPoller {
    pub fn new(config: WatchdogConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");
        Self { config, client }
    }

    /// Spawn the poller as a tokio task. Returns a receiver for status verdicts.
    pub fn spawn(self) -> mpsc::UnboundedReceiver<StatusVerdict> {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            self.run(tx).await;
        });
        rx
    }

    async fn run(self, tx: mpsc::UnboundedSender<StatusVerdict>) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.status_poll_interval_secs));
        let mut consecutive_failures: u32 = 0;
        const MAX_FAILURES_BEFORE_DEGRADED: u32 = 5;

        loop {
            interval.tick().await;
            match self.poll_once().await {
                Ok(verdict) => {
                    consecutive_failures = 0;
                    debug!(verdict = ?verdict, "Status page polled");
                    if tx.send(verdict).is_err() {
                        debug!("StatusPoller receiver dropped, shutting down");
                        return;
                    }
                }
                Err(e) => {
                    consecutive_failures += 1;
                    warn!(
                        error = %e,
                        consecutive_failures,
                        "Failed to poll status page"
                    );
                    // Status page being unreachable is NOT a Polymarket outage.
                    // Only escalate to Degraded after sustained failures.
                    if consecutive_failures >= MAX_FAILURES_BEFORE_DEGRADED {
                        let verdict = StatusVerdict::Degraded {
                            reason: format!(
                                "Status page unreachable for {} consecutive polls",
                                consecutive_failures
                            ),
                        };
                        if tx.send(verdict).is_err() {
                            return;
                        }
                    } else {
                        // Send Healthy — don't assume outage from status page failure
                        let _ = tx.send(StatusVerdict::Healthy);
                    }
                }
            }
        }
    }

    async fn poll_once(&self) -> anyhow::Result<StatusVerdict> {
        let resp = self.client.get(&self.config.status_page_url).send().await?;

        if !resp.status().is_success() {
            anyhow::bail!("Status page returned HTTP {}", resp.status());
        }

        let summary: InstatusSummary = resp.json().await?;
        Ok(self.evaluate(&summary))
    }

    fn evaluate(&self, summary: &InstatusSummary) -> StatusVerdict {
        // Check critical components for outages
        for component in &summary.components {
            if self.is_critical_component(&component.name) {
                match component.status.as_str() {
                    "MAJOROUTAGE" | "PARTIALOUTAGE" => {
                        return StatusVerdict::Critical {
                            reason: format!("{} is in {} state", component.name, component.status),
                        };
                    }
                    "DEGRADEDPERFORMANCE" => {
                        return StatusVerdict::Degraded {
                            reason: format!(
                                "{} is experiencing degraded performance",
                                component.name
                            ),
                        };
                    }
                    "UNDERMAINTENANCE" => {
                        return StatusVerdict::Degraded {
                            reason: format!("{} is under maintenance", component.name),
                        };
                    }
                    _ => {} // OPERATIONAL or unknown
                }
            }
        }

        // Check active incidents affecting critical components
        for incident in &summary.incidents {
            match incident.status.as_str() {
                "INVESTIGATING" | "IDENTIFIED" => {
                    // Check if incident affects any critical component
                    let affects_critical = incident
                        .components
                        .iter()
                        .any(|c| self.is_critical_component(c));

                    if affects_critical {
                        return StatusVerdict::Critical {
                            reason: format!(
                                "Active incident '{}' (status: {}) affecting critical component",
                                incident.name, incident.status
                            ),
                        };
                    } else {
                        return StatusVerdict::Degraded {
                            reason: format!(
                                "Active incident '{}' (status: {})",
                                incident.name, incident.status
                            ),
                        };
                    }
                }
                "MONITORING" => {
                    return StatusVerdict::Degraded {
                        reason: format!("Incident '{}' being monitored", incident.name),
                    };
                }
                _ => {} // RESOLVED or unknown
            }
        }

        // Check overall page status
        if summary.page.status == "HASISSUES" {
            return StatusVerdict::Degraded {
                reason: "Status page reports issues".to_string(),
            };
        }

        StatusVerdict::Healthy
    }

    fn is_critical_component(&self, name: &str) -> bool {
        self.config
            .critical_components
            .iter()
            .any(|c| c.eq_ignore_ascii_case(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> WatchdogConfig {
        WatchdogConfig::default()
    }

    fn poller() -> StatusPoller {
        StatusPoller::new(test_config())
    }

    fn make_summary(
        page_status: &str,
        components: Vec<(&str, &str)>,
        incidents: Vec<(&str, &str, Vec<&str>)>,
    ) -> InstatusSummary {
        InstatusSummary {
            page: InstatusPage {
                status: page_status.to_string(),
            },
            components: components
                .into_iter()
                .map(|(name, status)| InstatusComponent {
                    name: name.to_string(),
                    status: status.to_string(),
                })
                .collect(),
            incidents: incidents
                .into_iter()
                .map(|(name, status, comps)| InstatusIncident {
                    name: name.to_string(),
                    status: status.to_string(),
                    components: comps.into_iter().map(|s| s.to_string()).collect(),
                })
                .collect(),
        }
    }

    #[test]
    fn healthy_when_all_operational() {
        let p = poller();
        let summary = make_summary(
            "UP",
            vec![
                ("CLOB API", "OPERATIONAL"),
                ("Polygon (RPC)", "OPERATIONAL"),
                ("Website", "OPERATIONAL"),
            ],
            vec![],
        );
        assert_eq!(p.evaluate(&summary), StatusVerdict::Healthy);
    }

    #[test]
    fn critical_on_clob_major_outage() {
        let p = poller();
        let summary = make_summary(
            "HASISSUES",
            vec![
                ("CLOB API", "MAJOROUTAGE"),
                ("Polygon (RPC)", "OPERATIONAL"),
            ],
            vec![],
        );
        assert!(matches!(
            p.evaluate(&summary),
            StatusVerdict::Critical { .. }
        ));
    }

    #[test]
    fn critical_on_polygon_partial_outage() {
        let p = poller();
        let summary = make_summary(
            "HASISSUES",
            vec![
                ("CLOB API", "OPERATIONAL"),
                ("Polygon (RPC)", "PARTIALOUTAGE"),
            ],
            vec![],
        );
        assert!(matches!(
            p.evaluate(&summary),
            StatusVerdict::Critical { .. }
        ));
    }

    #[test]
    fn degraded_on_clob_degraded_performance() {
        let p = poller();
        let summary = make_summary(
            "HASISSUES",
            vec![
                ("CLOB API", "DEGRADEDPERFORMANCE"),
                ("Polygon (RPC)", "OPERATIONAL"),
            ],
            vec![],
        );
        assert!(matches!(
            p.evaluate(&summary),
            StatusVerdict::Degraded { .. }
        ));
    }

    #[test]
    fn critical_on_active_incident_affecting_critical_component() {
        let p = poller();
        let summary = make_summary(
            "HASISSUES",
            vec![
                ("CLOB API", "OPERATIONAL"),
                ("Polygon (RPC)", "OPERATIONAL"),
            ],
            vec![("CLOB API latency issues", "INVESTIGATING", vec!["CLOB API"])],
        );
        assert!(matches!(
            p.evaluate(&summary),
            StatusVerdict::Critical { .. }
        ));
    }

    #[test]
    fn degraded_on_non_critical_incident() {
        let p = poller();
        let summary = make_summary(
            "HASISSUES",
            vec![("CLOB API", "OPERATIONAL"), ("Website", "OPERATIONAL")],
            vec![("Website issue", "INVESTIGATING", vec!["Website"])],
        );
        assert!(matches!(
            p.evaluate(&summary),
            StatusVerdict::Degraded { .. }
        ));
    }

    #[test]
    fn healthy_on_resolved_incident() {
        let p = poller();
        let summary = make_summary(
            "UP",
            vec![
                ("CLOB API", "OPERATIONAL"),
                ("Polygon (RPC)", "OPERATIONAL"),
            ],
            vec![("Past issue", "RESOLVED", vec!["CLOB API"])],
        );
        assert_eq!(p.evaluate(&summary), StatusVerdict::Healthy);
    }

    #[test]
    fn non_critical_component_outage_is_not_critical() {
        let p = poller();
        let summary = make_summary(
            "HASISSUES",
            vec![
                ("CLOB API", "OPERATIONAL"),
                ("Website", "MAJOROUTAGE"),
                ("Polygon (RPC)", "OPERATIONAL"),
            ],
            vec![],
        );
        // Website is not critical, so page-level HASISSUES → Degraded
        assert!(matches!(
            p.evaluate(&summary),
            StatusVerdict::Degraded { .. }
        ));
    }
}
