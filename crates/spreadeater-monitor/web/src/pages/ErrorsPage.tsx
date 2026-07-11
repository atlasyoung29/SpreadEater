import { useEffect, useState } from "react";
import { ErrorTable } from "../components/ErrorTable";
import { PaginationControls } from "../components/PaginationControls";
import { fetchErrors, openLiveSocket } from "../lib/api";
import { usePagedResource } from "../lib/usePagedResource";
import type { BotErrorLogEntry, LiveFrame, PageResponse } from "../types";

export function ErrorsPage() {
  const [q, setQ] = useState("");
  const [level, setLevel] = useState("");
  const [windowMinutes, setWindowMinutes] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const [liveItems, setLiveItems] = useState<BotErrorLogEntry[]>([]);

  const query = {
    q,
    level,
    window_minutes: windowMinutes,
    page,
    page_size: pageSize,
  };

  const resource = usePagedResource({
    queryKey: JSON.stringify(query),
    autoRefresh: page === 1,
    loader: () => fetchErrors(query),
  });

  useEffect(() => {
    if (page !== 1) {
      return;
    }

    const socket = openLiveSocket((frame: LiveFrame) => {
      if (frame.channel !== "errors") {
        return;
      }

      const entry = frame.payload as BotErrorLogEntry;
      setLiveItems((current) => {
        if (current.some((item) => item.id === entry.id)) {
          return current;
        }
        return [entry, ...current].slice(0, 25);
      });
    });

    return () => {
      socket.close();
    };
  }, [page]);

  useEffect(() => {
    setLiveItems([]);
  }, [q, level, windowMinutes, page, pageSize]);

  const rows = mergeRows(resource.data, liveItems, page);

  return (
    <section className="page page--ops">
      <section className="section-card section-card--dense">
        <div className="section-card__header">
          <div>
            <p className="eyebrow">errors</p>
            <h1 className="section-card__title">Runtime error capture</h1>
          </div>
          <div className="section-card__meta">
            <span>tailed from redirected bot logs</span>
            {page > 1 ? (
              <button type="button" className="inline-link inline-link--button" onClick={resource.refresh}>
                refresh this page
              </button>
            ) : null}
          </div>
        </div>

        <div className="toolbar toolbar--sticky">
          <label className="toolbar-field toolbar-field--search">
            <span>search</span>
            <input
              value={q}
              onChange={(event) => {
                setPage(1);
                setQ(event.target.value);
              }}
              placeholder="error message text"
            />
          </label>
          <label className="toolbar-field">
            <span>level</span>
            <select value={level} onChange={(event) => { setPage(1); setLevel(event.target.value); }}>
              <option value="">all</option>
              <option value="error">error</option>
              <option value="critical">critical</option>
              <option value="warn">warn</option>
              <option value="unknown">unknown</option>
            </select>
          </label>
          <label className="toolbar-field">
            <span>window</span>
            <select
              value={windowMinutes}
              onChange={(event) => {
                setPage(1);
                setWindowMinutes(event.target.value);
              }}
            >
              <option value="">all time</option>
              <option value="15">15m</option>
              <option value="60">1h</option>
              <option value="360">6h</option>
              <option value="1440">24h</option>
            </select>
          </label>
        </div>

        {resource.error ? <div className="error-banner">{resource.error}</div> : null}
        {resource.loading && !resource.data ? <div className="table-empty">Loading error logs...</div> : null}

        {resource.data ? (
          <>
            <PaginationControls
              page={page}
              pageSize={pageSize}
              total={resource.data.total}
              onPageChange={setPage}
              onPageSizeChange={(size) => {
                setPage(1);
                setPageSize(size);
              }}
            />
            <ErrorTable rows={rows} emptyLabel="No errors matched the current filters." />
          </>
        ) : null}
      </section>
    </section>
  );
}

function mergeRows(
  page: PageResponse<BotErrorLogEntry> | null,
  liveItems: BotErrorLogEntry[],
  currentPage: number,
) {
  if (!page) {
    return liveItems;
  }
  if (currentPage !== 1 || liveItems.length === 0) {
    return page.items;
  }

  const seen = new Set<number>();
  const merged = [...liveItems, ...page.items].filter((item) => {
    if (seen.has(item.id)) {
      return false;
    }
    seen.add(item.id);
    return true;
  });
  return merged;
}
