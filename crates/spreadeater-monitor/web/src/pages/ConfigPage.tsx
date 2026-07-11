import { ConfigView } from "../components/ConfigView";
import { fetchConfig } from "../lib/api";
import { usePagedResource } from "../lib/usePagedResource";

export function ConfigPage() {
  const resource = usePagedResource({
    queryKey: "config",
    autoRefresh: false,
    loader: fetchConfig,
  });

  return (
    <section className="page page--ops">
      {resource.error ? <div className="error-banner">{resource.error}</div> : null}
      {resource.loading && !resource.data ? (
        <section className="section-card section-card--dense">
          <div className="table-empty">Loading config...</div>
        </section>
      ) : null}
      {resource.data ? <ConfigView config={resource.data} /> : null}
    </section>
  );
}
