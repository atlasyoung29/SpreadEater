use spreadeater::watchdog::health::HealthVerdict;
use spreadeater::watchdog::status_poller::StatusVerdict;

#[test]
fn health_verdict_equality() {
    assert_eq!(HealthVerdict::Healthy, HealthVerdict::Healthy);
    assert_ne!(
        HealthVerdict::Healthy,
        HealthVerdict::Degraded {
            reason: "test".to_string(),
        }
    );
    assert_ne!(
        HealthVerdict::Degraded {
            reason: "a".to_string()
        },
        HealthVerdict::Critical {
            reason: "a".to_string()
        },
    );
}

#[test]
fn status_verdict_equality() {
    assert_eq!(StatusVerdict::Healthy, StatusVerdict::Healthy);
    assert_ne!(
        StatusVerdict::Healthy,
        StatusVerdict::Degraded {
            reason: "test".to_string(),
        }
    );
}

#[test]
fn health_verdict_debug_format() {
    let verdict = HealthVerdict::Critical {
        reason: "test reason".to_string(),
    };
    let debug = format!("{:?}", verdict);
    assert!(debug.contains("Critical"));
    assert!(debug.contains("test reason"));
}
