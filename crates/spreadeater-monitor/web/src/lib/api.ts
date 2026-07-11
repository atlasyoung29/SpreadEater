import type {
  BotErrorLogEntry,
  ConfigResponse,
  EventListItem,
  LiveFrame,
  MarketDetailResponse,
  MarketSummary,
  OverviewResponse,
  PageResponse,
  TraceDetailResponse,
} from "../types";

const baseUrl = "";
const polymarketUrlCache = new Map<string, Promise<string | null>>();

type QueryValue = string | number | boolean | null | undefined;

async function fetchJson<T>(
  path: string,
  query?: Record<string, QueryValue>,
): Promise<T> {
  const url = new URL(`${baseUrl}${path}`, window.location.origin);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === null || value === undefined || value === "") {
        continue;
      }
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url.pathname + url.search, {
    cache: "no-store",
    headers: {
      "cache-control": "no-cache",
    },
  });
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { error?: string } | null;
    throw new Error(payload?.error ?? `Request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

export function fetchOverview() {
  return fetchJson<OverviewResponse>("/api/v1/overview");
}

export function fetchOpenOrders(query?: Record<string, QueryValue>) {
  return fetchJson<PageResponse<MarketSummary>>("/api/v1/open-orders", query);
}

export function fetchInventory(query?: Record<string, QueryValue>) {
  return fetchJson<PageResponse<MarketSummary>>("/api/v1/inventory", query);
}

export function fetchWatchlist(query?: Record<string, QueryValue>) {
  return fetchJson<PageResponse<MarketSummary>>("/api/v1/watchlist", query);
}

export function fetchHistory(query?: Record<string, QueryValue>) {
  return fetchJson<PageResponse<EventListItem>>("/api/v1/history", query);
}

export function fetchErrors(query?: Record<string, QueryValue>) {
  return fetchJson<PageResponse<BotErrorLogEntry>>("/api/v1/errors", query);
}

export function fetchConfig() {
  return fetchJson<ConfigResponse>("/api/v1/config");
}

export function fetchMarket(conditionId: string) {
  return fetchJson<MarketDetailResponse>(
    `/api/v1/markets/${conditionId}?include_timeline=true`,
  );
}

export function fetchTrace(traceId: string) {
  return fetchJson<TraceDetailResponse>(`/api/v1/traces/${traceId}`);
}

export function resolvePolymarketUrl(marketSlug: string | null | undefined) {
  if (!marketSlug) {
    return Promise.resolve<string | null>(null);
  }
  const cached = polymarketUrlCache.get(marketSlug);
  if (cached) {
    return cached;
  }

  const request = fetchJson<{ url: string | null }>(
    "/api/v1/polymarket-url",
    { market_slug: marketSlug },
  )
    .then((payload) => payload.url)
    .catch(() => null);
  polymarketUrlCache.set(marketSlug, request);
  return request;
}

export function openLiveSocket(onMessage: (frame: LiveFrame) => void) {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws/live`);
  socket.onmessage = (event) => {
    try {
      onMessage(JSON.parse(event.data) as LiveFrame);
    } catch {
      // ignore malformed frames
    }
  };
  return socket;
}
