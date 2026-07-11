import { useEffect, useMemo, useState } from "react";
import { fetchOverview, openLiveSocket } from "./api";
import type { LiveFrame, OverviewResponse } from "../types";

const DEFAULT_POLL_INTERVAL_SECS = 300;
const MIN_STALE_AFTER_MS = 90_000;
const OVERVIEW_REFRESH_INTERVAL_MS = 1_000;
const FRESHNESS_TICK_INTERVAL_MS = 1_000;

export function useOverviewLive() {
  const [overview, setOverview] = useState<OverviewResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [socketStatus, setSocketStatus] = useState("connecting");
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    let active = true;
    const applyOverview = (incoming: OverviewResponse) => {
      setOverview((current) => {
        if (!current) {
          return incoming;
        }
        return overviewTimestamp(incoming) >= overviewTimestamp(current) ? incoming : current;
      });
      setError(null);
    };

    const refreshOverview = () => {
      fetchOverview()
        .then((data) => {
          if (active) {
            applyOverview(data);
          }
        })
        .catch((cause: Error) => {
          if (active) {
            setError(cause.message);
          }
        });
    };

    refreshOverview();
    const refreshInterval = window.setInterval(refreshOverview, OVERVIEW_REFRESH_INTERVAL_MS);

    const socket = openLiveSocket((frame: LiveFrame) => {
      if (frame.channel === "overview") {
        applyOverview(frame.payload as OverviewResponse);
      }
    });

    socket.onopen = () => setSocketStatus("live");
    socket.onclose = () => setSocketStatus("offline");
    socket.onerror = () => setSocketStatus("offline");

    return () => {
      active = false;
      window.clearInterval(refreshInterval);
      socket.close();
    };
  }, []);

  useEffect(() => {
    const interval = window.setInterval(() => {
      setNow(Date.now());
    }, FRESHNESS_TICK_INTERVAL_MS);
    return () => {
      window.clearInterval(interval);
    };
  }, []);

  const freshness = useMemo(() => {
    if (!overview) {
      return {
        status: socketStatus,
        label: socketStatus,
        ageMs: null as number | null,
      };
    }

    const lastEventAt = new Date(overview.last_event_at).getTime();
    const ageMs = Number.isFinite(lastEventAt) ? Math.max(now - lastEventAt, 0) : null;
    const staleAfterMs = Math.max(
      (overview.expected_cycle_interval_secs || DEFAULT_POLL_INTERVAL_SECS) * 2_000,
      MIN_STALE_AFTER_MS,
    );

    if (socketStatus === "offline") {
      return { status: "offline", label: "offline", ageMs };
    }
    if (ageMs !== null && ageMs > staleAfterMs) {
      return {
        status: "stale",
        label: `stale ${Math.floor(ageMs / 60_000)}m`,
        ageMs,
      };
    }
    if (socketStatus === "connecting") {
      return { status: "connecting", label: "connecting", ageMs };
    }
    return { status: "live", label: "live", ageMs };
  }, [now, overview, socketStatus]);

  const healthStatus =
    freshness.status === "stale" && overview?.observer_health === "healthy"
      ? "stale"
      : (overview?.observer_health ?? "waiting");

  return {
    overview,
    error,
    liveStatus: freshness.status,
    liveLabel: freshness.label,
    healthStatus,
    lastEventAgeMs: freshness.ageMs,
  };
}

function overviewTimestamp(overview: OverviewResponse) {
  const timestamp = new Date(overview.last_event_at).getTime();
  return Number.isFinite(timestamp) ? timestamp : 0;
}
