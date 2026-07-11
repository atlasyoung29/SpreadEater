import { useEffect, useState } from "react";

interface UsePagedResourceOptions<T> {
  queryKey: string;
  autoRefresh: boolean;
  loader: () => Promise<T>;
  refreshIntervalMs?: number;
}

export function usePagedResource<T>({
  queryKey,
  autoRefresh,
  loader,
  refreshIntervalMs = 3_000,
}: UsePagedResourceOptions<T>) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);

  useEffect(() => {
    let active = true;
    setLoading(true);

    loader()
      .then((value) => {
        if (!active) {
          return;
        }
        setData(value);
        setError(null);
      })
      .catch((cause: Error) => {
        if (!active) {
          return;
        }
        setError(cause.message);
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [queryKey, revision]);

  useEffect(() => {
    if (!autoRefresh) {
      return;
    }

    const interval = window.setInterval(() => {
      setRevision((current) => current + 1);
    }, refreshIntervalMs);

    return () => {
      window.clearInterval(interval);
    };
  }, [autoRefresh, refreshIntervalMs]);

  return {
    data,
    error,
    loading,
    refresh() {
      setRevision((current) => current + 1);
    },
  };
}
