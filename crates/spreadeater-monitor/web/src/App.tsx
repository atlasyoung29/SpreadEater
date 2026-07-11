import { Route, Routes } from "react-router-dom";
import { Shell } from "./components/Shell";
import { ConfigPage } from "./pages/ConfigPage";
import { ErrorsPage } from "./pages/ErrorsPage";
import { HistoryPage } from "./pages/HistoryPage";
import { InventoryPage } from "./pages/InventoryPage";
import { MarketPage } from "./pages/MarketPage";
import { OpenOrdersPage } from "./pages/OpenOrdersPage";
import { OverviewPage } from "./pages/OverviewPage";
import { TracePage } from "./pages/TracePage";
import { WatchlistPage } from "./pages/WatchlistPage";

export default function App() {
  return (
    <Shell>
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/open-orders" element={<OpenOrdersPage />} />
        <Route path="/inventory" element={<InventoryPage />} />
        <Route path="/history" element={<HistoryPage />} />
        <Route path="/errors" element={<ErrorsPage />} />
        <Route path="/watchlist" element={<WatchlistPage />} />
        <Route path="/config" element={<ConfigPage />} />
        <Route path="/markets/:conditionId" element={<MarketPage />} />
        <Route path="/traces/:traceId" element={<TracePage />} />
      </Routes>
    </Shell>
  );
}
