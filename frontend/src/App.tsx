import { useEffect, useState } from "react";
import { fetchHealth, type Health } from "./api/health";

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchHealth().then(setHealth).catch((e: Error) => setError(e.message));
  }, []);

  return (
    <main className="min-h-screen bg-neutral-950 text-neutral-100 p-8 font-sans">
      <h1 className="text-2xl font-semibold">FantasyFootballHelper</h1>
      <p className="mt-2 text-neutral-400">Phase 0 — foundation</p>
      <section className="mt-6 rounded-lg border border-neutral-800 p-4">
        <h2 className="text-sm uppercase tracking-wide text-neutral-500">API health</h2>
        {health && (
          <p className="mt-2">
            {health.status} · v{health.version} · season {health.season}
          </p>
        )}
        {error && <p className="mt-2 text-red-400">unreachable: {error}</p>}
        {!health && !error && <p className="mt-2 text-neutral-500">checking…</p>}
      </section>
    </main>
  );
}
