import type { ConfigResponse } from "../types";
import { formatTimestamp } from "../lib/format";

export function ConfigView({ config }: { config: ConfigResponse }) {
  const flattened = flattenConfig(config.value);

  return (
    <div className="config-grid">
      <section className="section-card section-card--dense">
        <div className="section-card__header">
          <div>
            <p className="eyebrow">config file</p>
            <h2>Live config snapshot</h2>
          </div>
          <div className="section-card__meta">
            <span>{config.path}</span>
            <span>updated {formatTimestamp(config.last_modified_at)}</span>
          </div>
        </div>
        <pre className="config-pre">{JSON.stringify(config.value, null, 2)}</pre>
      </section>

      <section className="section-card section-card--dense">
        <div className="section-card__header">
          <div>
            <p className="eyebrow">flattened</p>
            <h2>Every key/value</h2>
          </div>
          <div className="section-card__meta">
            <span>{flattened.length} entries</span>
          </div>
        </div>
        <div className="data-table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>key</th>
                <th>value</th>
              </tr>
            </thead>
            <tbody>
              {flattened.map((entry) => (
                <tr key={entry.key}>
                  <td className="config-key">{entry.key}</td>
                  <td className="config-value">{entry.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function flattenConfig(
  value: unknown,
  prefix = "",
): Array<{ key: string; value: string }> {
  if (Array.isArray(value)) {
    return value.flatMap((item, index) =>
      flattenConfig(item, prefix ? `${prefix}[${index}]` : `[${index}]`),
    );
  }

  if (value !== null && typeof value === "object") {
    return Object.entries(value).flatMap(([key, nested]) =>
      flattenConfig(nested, prefix ? `${prefix}.${key}` : key),
    );
  }

  return [
    {
      key: prefix || "(root)",
      value: value === null ? "null" : typeof value === "string" ? value : JSON.stringify(value),
    },
  ];
}
