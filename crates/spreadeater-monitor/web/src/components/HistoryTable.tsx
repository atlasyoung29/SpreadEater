import { Link } from "react-router-dom";
import type { EventListItem } from "../types";
import { formatIdentifier, formatTimestamp } from "../lib/format";

export function HistoryTable({
  rows,
  emptyLabel,
}: {
  rows: EventListItem[];
  emptyLabel: string;
}) {
  if (rows.length === 0) {
    return <div className="table-empty">{emptyLabel}</div>;
  }

  return (
    <div className="data-table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>time</th>
            <th>event</th>
            <th>priority</th>
            <th>market</th>
            <th>trace</th>
            <th>order</th>
            <th>reason</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((item) => (
            <tr key={item.id}>
              <td>{formatTimestamp(item.occurred_at)}</td>
              <td>
                <span className={`tone-pill tone-pill--${priorityTone(item.priority)}`}>
                  {item.event_type}
                </span>
              </td>
              <td>{item.priority}</td>
              <td className="market-cell">
                {item.condition_id ? (
                  <Link to={`/markets/${item.condition_id}`} className="market-cell__title">
                    {item.question ?? item.market_slug ?? item.condition_id}
                  </Link>
                ) : (
                  <span className="market-cell__title">
                    {item.question ?? item.market_slug ?? "n/a"}
                  </span>
                )}
                <div className="market-cell__meta">
                  {item.condition_id ? <span>{formatIdentifier(item.condition_id)}</span> : null}
                  {item.market_slug ? <span>{item.market_slug}</span> : null}
                </div>
              </td>
              <td>
                {item.trace_id ? (
                  <Link to={`/traces/${item.trace_id}`}>{formatIdentifier(item.trace_id)}</Link>
                ) : (
                  "n/a"
                )}
              </td>
              <td>{item.order_id ? formatIdentifier(item.order_id) : "n/a"}</td>
              <td>{historyReason(item)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function historyReason(item: EventListItem) {
  return (
    item.reason_code ??
    item.order_cancel_reason ??
    (typeof item.payload.reason === "string" ? item.payload.reason : null) ??
    (typeof item.payload.degraded_reason === "string"
      ? item.payload.degraded_reason
      : null) ??
    "n/a"
  );
}

function priorityTone(priority: string) {
  switch (priority.toLowerCase()) {
    case "critical":
      return "danger";
    case "high":
      return "warn";
    default:
      return "muted";
  }
}
