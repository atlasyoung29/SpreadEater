use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info};

use crate::config::WatchdogConfig;
use crate::monitor::ErrorLogger;
use crate::trading::RiskManager;

/// Trait for kill action — allows mocking in tests.
#[async_trait::async_trait]
pub trait KillAction: Send + Sync {
    async fn execute(&self, reason: &str) -> Result<()>;
}

/// Production kill trigger that shells out to kill_flatten.py.
pub struct KillTrigger {
    config: WatchdogConfig,
    risk_manager: Arc<RiskManager>,
    error_logger: Arc<ErrorLogger>,
}

impl KillTrigger {
    pub fn new(
        config: WatchdogConfig,
        risk_manager: Arc<RiskManager>,
        error_logger: Arc<ErrorLogger>,
    ) -> Self {
        Self {
            config,
            risk_manager,
            error_logger,
        }
    }

    /// Execute the emergency kill+flatten sequence.
    ///
    /// 1. Log the kill decision
    /// 2. Activate global halt (stops new orders immediately)
    /// 3. Shell out to kill_flatten.py (cancels orders + sells positions)
    /// 4. If Python fails to start, exit the process
    pub async fn execute_kill_flatten(&self, reason: &str) {
        // Step 1: Log the kill decision
        error!(reason = %reason, "WATCHDOG: Executing kill+flatten");
        self.error_logger.log_error(
            "CRITICAL",
            &format!("Watchdog kill+flatten triggered: {}", reason),
            None,
        );

        // Step 2: Activate global halt immediately
        self.risk_manager
            .global_halt(&format!("watchdog: {}", reason))
            .await;

        // Step 3: Shell out to kill_flatten.py
        let script = &self.config.kill_flatten_script;
        info!(script = %script, "Spawning kill_flatten.py");

        match tokio::process::Command::new("python")
            .arg(script)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
        {
            Ok(mut child) => {
                // Wait for the script to complete (it will kill us, but just in case)
                match tokio::time::timeout(std::time::Duration::from_secs(120), child.wait()).await
                {
                    Ok(Ok(status)) => {
                        if status.success() {
                            info!("kill_flatten.py completed successfully");
                        } else {
                            error!(
                                exit_code = ?status.code(),
                                "kill_flatten.py exited with error"
                            );
                            self.fallback_exit(reason);
                        }
                    }
                    Ok(Err(e)) => {
                        error!(error = %e, "Failed to wait for kill_flatten.py");
                        self.fallback_exit(reason);
                    }
                    Err(_) => {
                        error!("kill_flatten.py timed out after 120s");
                        self.fallback_exit(reason);
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to spawn kill_flatten.py");
                self.fallback_exit(reason);
            }
        }
    }

    /// Fallback: if Python script fails, force exit the process.
    /// Global halt is already active, so no new orders are being placed.
    /// The sidecar will detect the stale heartbeat and can retry the flatten.
    fn fallback_exit(&self, reason: &str) -> ! {
        error!(
            reason = %reason,
            "Fallback: kill_flatten.py failed, forcing process exit. \
             Sidecar should detect stale heartbeat and retry flatten."
        );
        self.error_logger.log_error(
            "CRITICAL",
            &format!(
                "Watchdog fallback exit — kill_flatten.py failed: {}",
                reason
            ),
            None,
        );
        std::process::exit(1);
    }
}

#[async_trait::async_trait]
impl KillAction for KillTrigger {
    async fn execute(&self, reason: &str) -> Result<()> {
        self.execute_kill_flatten(reason).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Mock kill action for testing.
    struct MockKillAction {
        executed: AtomicBool,
    }

    impl MockKillAction {
        fn new() -> Self {
            Self {
                executed: AtomicBool::new(false),
            }
        }

        fn was_executed(&self) -> bool {
            self.executed.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl KillAction for MockKillAction {
        async fn execute(&self, _reason: &str) -> Result<()> {
            self.executed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn mock_kill_action_records_execution() {
        let mock = MockKillAction::new();
        assert!(!mock.was_executed());
        mock.execute("test").await.unwrap();
        assert!(mock.was_executed());
    }
}
