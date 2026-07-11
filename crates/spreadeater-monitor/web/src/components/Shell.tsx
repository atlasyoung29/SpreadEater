import type { PropsWithChildren } from "react";
import { useEffect, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/", label: "Overview" },
  { to: "/open-orders", label: "Open Orders" },
  { to: "/inventory", label: "Inventory" },
  { to: "/history", label: "History" },
  { to: "/errors", label: "Errors" },
  { to: "/watchlist", label: "Watchlist" },
  { to: "/config", label: "Config" },
];

export function Shell({ children }: PropsWithChildren) {
  const location = useLocation();
  const [theme, setTheme] = useState<"dark" | "light">(() => readStoredTheme());

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem("spreadeater-monitor-theme", theme);
  }, [theme]);

  return (
    <div className="shell">
      <header className="masthead masthead--ops">
        <div className="masthead__brand">
          <NavLink to="/" className="brand-mark">
            SpreadEater Monitor
          </NavLink>
          <span className="brand-subtitle">operator console</span>
        </div>

        <nav className="masthead__nav masthead__nav--ops">
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/"}
              className={({ isActive }) =>
                `status-chip ${isActive ? "status-chip--active" : ""}`
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="masthead__status">
          <button
            type="button"
            className={`theme-toggle theme-toggle--${theme}`}
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
          >
            <span className="theme-toggle__label">{theme}</span>
            <span className="theme-toggle__track">
              <span className="theme-toggle__thumb" />
            </span>
          </button>
          <div className="status-chip">localhost</div>
          <div className="status-path">{location.pathname}</div>
        </div>
      </header>
      <main className="page-shell">{children}</main>
    </div>
  );
}

function readStoredTheme(): "dark" | "light" {
  if (typeof window === "undefined") {
    return "dark";
  }

  const stored = window.localStorage.getItem("spreadeater-monitor-theme");
  return stored === "light" ? "light" : "dark";
}
