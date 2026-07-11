import { Link } from "react-router-dom";
import { ErrorTable } from "../components/ErrorTable";
import { HistoryTable } from "../components/HistoryTable";
import { MarketTable } from "../components/MarketTable";
import { StatPlate } from "../components/StatPlate";
import { formatAgeMs, formatMoney } from "../lib/format";
import { useOverviewLive } from "../lib/useOverviewLive";

export function OverviewPage() {
  const {
    overview,
    error,
    healthStatus,
    lastEventAgeMs,
    liveLabel,
    liveStatus,
  } = useOverviewLive();

  const lastEventChip =
    lastEventAgeMs === null ? "last event n/a" : `last event ${formatAgeMs(lastEventAgeMs)} ago`;

  return (
    <section className="page page--ops">
      <section className="run-strip">
        <div className="run-strip__identity">
          <p className="eyebrow">run status</p>
          <strong>{overview?.run_id ?? "waiting"}</strong>
          <span>{overview?.mode ?? "n/a"}</span>
        </div>
        <div className="run-strip__chips">
          <span className={`status-pill status-pill--${healthStatus}`}>{healthStatus}</span>
          <span className={`status-pill status-pill--${liveStatus}`}>{liveLabel}</span>
          <span className="status-pill">{lastEventChip}</span>
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="stat-grid stat-grid--overview">
        <StatPlate
          label="Watched Markets"
          value={overview?.active_markets ?? 0}
          accent="signal"
        />
        <StatPlate label="Open Orders" value={overview?.open_orders ?? 0} accent="edge" />
        <StatPlate
          label="Capital Deployed"
          value={formatMoney(overview?.open_order_notional_usd)}
          meta={`Cap ${formatMoney(overview?.max_total_exposure_usd)}`}
          accent="ink"
        />
        <StatPlate
          label="Reward / Day"
          value={formatMoney(overview?.open_order_reward_usd_day)}
          accent="reward"
        />
        <StatPlate
          label="Inventory Markets"
          value={overview?.inventory_markets ?? 0}
          accent="neutral"
        />
        <StatPlate
          label="Available Budget"
          value={formatMoney(overview?.available_budget_usd)}
          meta={`API ${formatMoney(overview?.api_balance_usd)}`}
          accent="signal"
        />
      </section>

      <div className="ops-preview-grid">
        <section className="section-card section-card--dense">
          <div className="section-card__header">
            <div>
              <p className="eyebrow">open orders</p>
              <h2>Live order board</h2>
            </div>
            <div className="section-card__meta">
              <span>{overview?.open_order_markets ?? 0} markets</span>
              <Link to="/open-orders" className="inline-link">
                full tab
              </Link>
            </div>
          </div>
          <MarketTable
            rows={overview?.open_order_preview ?? []}
            mode="preview-orders"
            emptyLabel="No resting orders."
          />
        </section>

        <section className="section-card section-card--dense">
          <div className="section-card__header">
            <div>
              <p className="eyebrow">inventory</p>
              <h2>Position snapshot</h2>
            </div>
            <div className="section-card__meta">
              <span>{overview?.inventory_markets ?? 0} markets</span>
              <Link to="/inventory" className="inline-link">
                full tab
              </Link>
            </div>
          </div>
          <MarketTable
            rows={overview?.inventory_preview ?? []}
            mode="preview-inventory"
            emptyLabel="No inventory recorded."
          />
        </section>

        <section className="section-card section-card--dense">
          <div className="section-card__header">
            <div>
              <p className="eyebrow">history</p>
              <h2>Recent audit</h2>
            </div>
            <div className="section-card__meta">
              <span>{overview?.recent_history.length ?? 0} newest events</span>
              <Link to="/history" className="inline-link">
                full tab
              </Link>
            </div>
          </div>
          <HistoryTable rows={overview?.recent_history ?? []} emptyLabel="No audit rows yet." />
        </section>

        <section className="section-card section-card--dense">
          <div className="section-card__header">
            <div>
              <p className="eyebrow">errors</p>
              <h2>Recent runtime issues</h2>
            </div>
            <div className="section-card__meta">
              <span>{overview?.recent_errors.length ?? 0} newest lines</span>
              <Link to="/errors" className="inline-link">
                full tab
              </Link>
            </div>
          </div>
          <ErrorTable rows={overview?.recent_errors ?? []} emptyLabel="No error lines captured." />
        </section>
      </div>
    </section>
  );
}
