import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { EventTimeline } from "../components/EventTimeline";
import { PolymarketLink } from "../components/PolymarketLink";
import { StatPlate } from "../components/StatPlate";
import { fetchMarket, openLiveSocket } from "../lib/api";
import {
  formatBoolean,
  formatIdentifier,
  formatMoney,
  formatNumber,
  formatPercent,
  formatYieldPercent,
} from "../lib/format";
import type { LiveFrame, MarketDetailResponse } from "../types";

export function MarketPage() {
  const { conditionId = "" } = useParams();
  const [market, setMarket] = useState<MarketDetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const rewardYield = formatYieldPercent(
    market?.expected_reward_usd_day,
    Number(market?.open_order_notional_usd ?? 0) > 0
      ? market?.open_order_notional_usd
      : market?.committed_capital_usd,
  );

  useEffect(() => {
    let active = true;
    fetchMarket(conditionId)
      .then((data) => {
        if (active) {
          setMarket(data);
          setError(null);
        }
      })
      .catch((cause: Error) => {
        if (active) {
          setError(cause.message);
        }
      });

    const socket = openLiveSocket((frame: LiveFrame) => {
      if (frame.channel !== "market") {
        return;
      }
      const payload = frame.payload as MarketDetailResponse;
      if (payload.condition_id === conditionId) {
        setMarket(payload);
      }
    });

    return () => {
      active = false;
      socket.close();
    };
  }, [conditionId]);

  return (
    <section className="page">
      {error ? <div className="error-banner">{error}</div> : null}

      <section className="section-card section-card--headline">
        <div>
          <p className="eyebrow">market detail</p>
          <h1>{market?.question ?? market?.market_slug ?? conditionId}</h1>
          <div className="detail-link-row">
            <PolymarketLink
              marketSlug={market?.market_slug}
              className="detail-link"
              fallbackToSpan
            >
              {formatIdentifier(market?.condition_id ?? conditionId)}
            </PolymarketLink>
            <span className="detail-link">{market?.market_slug ?? "unlinked market"}</span>
          </div>
        </div>
        <div className="trace-chip-row">
          {market?.recent_traces.map((traceId) => (
            <Link key={traceId} to={`/traces/${traceId}`} className="trace-chip">
              {formatIdentifier(traceId, 8, 4)}
            </Link>
          ))}
        </div>
      </section>

      <div className="stat-grid">
        <StatPlate label="Decision" value={market?.decision_status ?? "n/a"} accent="signal" />
        <StatPlate
          label="Expected Edge"
          value={formatMoney(market?.expected_edge_usd)}
          accent="edge"
          meta={formatPercent(market?.expected_edge_pct)}
        />
        <StatPlate
          label="Open Orders"
          value={market?.open_order_count ?? 0}
          meta={`${formatNumber(market?.open_order_share_size)} shares`}
        />
        <StatPlate
          label="Reward / Day"
          value={formatMoney(market?.expected_reward_usd_day)}
          accent="reward"
          meta={`yield/day ${rewardYield}`}
        />
        <StatPlate
          label="Open Notional"
          value={formatMoney(market?.open_order_notional_usd)}
          accent="ink"
        />
      </div>

      <section className="detail-board">
        <article className="detail-panel">
          <p className="eyebrow">position</p>
          <h2>Exposure board</h2>
          <dl className="metric-table">
            <div className="metric-emphasis metric-emphasis--edge">
              <dt>edge %</dt>
              <dd>{formatPercent(market?.expected_edge_pct)}</dd>
            </div>
            <div>
              <dt>capital deployed</dt>
              <dd>{formatMoney(market?.committed_capital_usd)}</dd>
            </div>
            <div>
              <dt>yes size</dt>
              <dd>{formatNumber(market?.yes_size)}</dd>
            </div>
            <div>
              <dt>no size</dt>
              <dd>{formatNumber(market?.no_size)}</dd>
            </div>
            <div>
              <dt>net exposure</dt>
              <dd>{formatNumber(market?.net_exposure)}</dd>
            </div>
            <div>
              <dt>complete sets</dt>
              <dd>{formatNumber(market?.complete_sets)}</dd>
            </div>
            <div>
              <dt>quote size</dt>
              <dd>{formatNumber(market?.effective_quote_size)}</dd>
            </div>
            <div className="metric-emphasis metric-emphasis--reward">
              <dt>reward / day</dt>
              <dd className="metric-stack">
                <span>{formatMoney(market?.expected_reward_usd_day)}</span>
                <span className="metric-stack__sub">yield/day {rewardYield}</span>
              </dd>
            </div>
            <div>
              <dt>neutral</dt>
              <dd>{formatBoolean(market?.is_neutral)}</dd>
            </div>
          </dl>
          <p className="detail-note">
            {market?.latest_reason ?? "No fresh block reason has been projected for this market."}
          </p>
        </article>

        <article className="detail-panel">
          <p className="eyebrow">live tape</p>
          <h2>Recent events</h2>
          <EventTimeline items={market?.recent_events ?? []} />
        </article>
      </section>
    </section>
  );
}
