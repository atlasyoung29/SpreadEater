import { type MouseEvent, type ReactNode, useEffect, useState } from "react";
import { resolvePolymarketUrl } from "../lib/api";
import { fallbackPolymarketUrl } from "../lib/polymarket";

export function PolymarketLink({
  marketSlug,
  className,
  fallbackToSpan = false,
  children,
}: {
  marketSlug: string | null | undefined;
  className?: string;
  fallbackToSpan?: boolean;
  children: ReactNode;
}) {
  const [href, setHref] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    if (!marketSlug) {
      setHref(null);
      return () => {
        active = false;
      };
    }

    resolvePolymarketUrl(marketSlug).then((url) => {
      if (active) {
        setHref(url);
      }
    });

    return () => {
      active = false;
    };
  }, [marketSlug]);

  async function handleClick(event: MouseEvent<HTMLAnchorElement>) {
    if (!marketSlug || href) {
      return;
    }
    event.preventDefault();
    const resolved = await resolvePolymarketUrl(marketSlug);
    if (!resolved) {
      return;
    }
    setHref(resolved);
    window.open(resolved, "_blank", "noopener,noreferrer");
  }

  if (!marketSlug) {
    return fallbackToSpan ? <span className={className}>{children}</span> : null;
  }

  return (
    <a
      href={href ?? fallbackPolymarketUrl(marketSlug) ?? "#"}
      onClick={handleClick}
      target="_blank"
      rel="noreferrer"
      className={className}
    >
      {children}
    </a>
  );
}
