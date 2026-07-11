import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { EventTimeline } from "../components/EventTimeline";
import { PolymarketLink } from "../components/PolymarketLink";
import { StatPlate } from "../components/StatPlate";
import { fetchTrace, openLiveSocket } from "../lib/api";
import {
  formatBoolean,
  formatIdentifier,
  formatMoney,
  formatNumber,
  formatPercent,
  formatYieldPercent,
} from "../lib/format";
import type { LiveFrame, TraceDetailResponse } from "../types";

export function TracePage() {
  const { traceId = "" } = useParams();
  const [trace, setTrace] = useState<TraceDetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const rewardYield = formatYieldPercent(
    trace?.decision?.expected_reward_usd_day,
    trace?.decision?.committed_capital_usd,
  );

  useEffect(() => {
    let active = true;
    fetchTrace(traceId)
      .then((data) => {
        if (active) {
          setTrace(data);
          setError(null);
        }
      })
      .catch((cause: Error) => {
        if (active) {
          setError(cause.message);
        }
      });

    const socket = openLiveSocket((frame: LiveFrame) => {
      if (frame.channel !== "trace") {
        return;
      }
      const payload = frame.payload as TraceDetailResponse;
      if (payload.trace_id === traceId) {
        setTrace(payload);
      }
    });

    return () => {
      active = false;
      socket.close();
    };
  }, [traceId]);

  return (
    <section className="page">
      {error ? <div className="error-banner">{error}</div> : null}

      <section className="section-card section-card--headline">
        <div>
          <p className="eyebrow">trace detail</p>
          <h1>{trace?.market.question ?? formatIdentifier(trace?.trace_id ?? traceId, 12, 6)}</h1>
          <div className="detail-link-row">
            {trace?.market.condition_id ? (
              <Link to={`/markets/${trace.market.condition_id}`} className="detail-link">
                {formatIdentifier(trace.market.condition_id)}
              </Link>
            ) : null}
            <PolymarketLink marketSlug={trace?.market.market_slug} className="detail-link">
              polymarket
            </PolymarketLink>
            <span className="detail-link">{formatIdentifier(trace?.trace_id ?? traceId, 10, 6)}</span>
          </div>
        </div>
      </section>

      <div className="stat-grid">
        <StatPlate label="Status" value={trace?.status ?? "n/a"} accent="signal" />
        <StatPlate label="Orders" value={trace?.orders.length ?? 0} />
        <StatPlate label="Fills" value={trace?.fills.length ?? 0} />
        <StatPlate label="Hedges" value={trace?.hedges.length ?? 0} accent="ink" />
      </div>

      <section className="detail-board">
        <article className="detail-panel">
          <p className="eyebrow">decision snapshot</p>
          <h2>Expected economics</h2>
          <dl className="metric-table">
            <div>
              <dt>would trade</dt>
              <dd>{formatBoolean(trace?.decision?.would_trade)}</dd>
            </div>
            <div className="metric-emphasis metric-emphasis--edge">
              <dt>expected edge</dt>
              <dd>{formatMoney(trace?.decision?.expected_edge_usd)}</dd>
            </div>
            <div className="metric-emphasis metric-emphasis--edge">
              <dt>edge %</dt>
              <dd>{formatPercent(trace?.decision?.expected_edge_pct)}</dd>
            </div>
            <div className="metric-emphasis metric-emphasis--reward">
              <dt>reward / day</dt>
              <dd className="metric-stack">
                <span>{formatMoney(trace?.decision?.expected_reward_usd_day)}</span>
                <span className="metric-stack__sub">yield/day {rewardYield}</span>
              </dd>
            </div>
            <div>
              <dt>capital deployed</dt>
              <dd>{formatMoney(trace?.decision?.committed_capital_usd)}</dd>
            </div>
            <div>
              <dt>quote size</dt>
              <dd>{formatNumber(trace?.decision?.effective_quote_size)}</dd>
            </div>
          </dl>
          <p className="detail-note">
            {trace?.decision?.reasons.join(" | ") || "No decision blockers were recorded."}
          </p>
          {trace?.neutrality ? (
            <div className="neutrality-panel">
              <strong>Neutrality verdict</strong>
              <p>
                {trace.neutrality.is_neutral ? "neutral" : "not neutral"} with residual{" "}
                {formatNumber(trace.neutrality.residual_exposure)} and complete sets{" "}
                {formatNumber(trace.neutrality.complete_sets)}
              </p>
            </div>
          ) : null}
        </article>

        <article className="detail-panel">
          <p className="eyebrow">timeline</p>
          <h2>Lifecycle tape</h2>
          <EventTimeline items={trace?.timeline ?? []} />
        </article>
      </section>
    </section>
  );
}
