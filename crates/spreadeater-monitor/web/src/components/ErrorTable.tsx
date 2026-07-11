import type { BotErrorLogEntry } from "../types";
import { formatIdentifier, formatTimestamp, normalizeLevel } from "../lib/format";

export function ErrorTable({
  rows,
  emptyLabel,
}: {
  rows: BotErrorLogEntry[];
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
            <th>level</th>
            <th>message</th>
            <th>source</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((item) => {
            const level = normalizeLevel(item.level);
            return (
              <tr key={item.id}>
                <td>{formatTimestamp(item.parsed_at ?? item.created_at)}</td>
                <td>
                  <span className={`tone-pill tone-pill--${levelTone(level)}`}>{level}</span>
                </td>
                <td className="error-cell">
                  <div>{item.message}</div>
                  {item.raw_line !== item.message ? (
                    <div className="row-note row-note--wrap">{item.raw_line}</div>
                  ) : null}
                </td>
                <td>
                  <div>{item.log_path}</div>
                  <div className="row-note">offset {formatIdentifier(String(item.byte_offset), 6, 0)}</div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function levelTone(level: string) {
  switch (level) {
    case "error":
    case "critical":
      return "danger";
    case "warn":
      return "warn";
    default:
      return "muted";
  }
}
