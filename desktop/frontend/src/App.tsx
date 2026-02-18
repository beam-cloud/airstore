import { useEffect, useMemo, useRef, useState, type ButtonHTMLAttributes } from "react";

import { desktopAPI, type DesktopEntry, type DesktopStatus } from "./client";

const NAV_LINKS = ["/", "/skills", "/sources", "/tools", "/tasks", "/memory"] as const;
const NAV_LABELS: Record<string, string> = {
  "/": "Root",
  "/skills": "Skills",
  "/sources": "Sources",
  "/tools": "Tools",
  "/tasks": "Tasks",
  "/memory": "Memory",
};

function normalizePath(v: string): string {
  const p = `/${v}`.replace(/\/+/g, "/");
  return p === "/" ? p : p.replace(/\/$/, "");
}

function parentPath(v: string): string {
  const parts = normalizePath(v).split("/").filter(Boolean);
  parts.pop();
  return parts.length ? `/${parts.join("/")}` : "/";
}

function joinPath(base: string, name: string): string {
  return normalizePath(`${normalizePath(base)}/${name.trim().replace(/^\/+/, "")}`);
}

function crumbs(path: string): Array<{ label: string; path: string }> {
  const parts = normalizePath(path).split("/").filter(Boolean);
  const out: Array<{ label: string; path: string }> = [{ label: "Root", path: "/" }];
  let cur = "";
  for (const p of parts) {
    cur += `/${p}`;
    out.push({ label: p, path: cur });
  }
  return out;
}

function sortedEntries(entries: DesktopEntry[]): DesktopEntry[] {
  return [...entries].sort((a, b) => {
    if (a.is_dir !== b.is_dir) return a.is_dir ? -1 : 1;
    return a.name.localeCompare(b.name);
  });
}

function folderTitle(path: string): string {
  const parts = normalizePath(path).split("/").filter(Boolean);
  return parts.length ? parts[parts.length - 1] : "Root";
}

function fmtSize(bytes?: number): string {
  if (!bytes || bytes <= 0) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = bytes;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u++;
  }
  return `${v >= 10 ? v.toFixed(0) : v.toFixed(1)} ${units[u]}`;
}

function fmtDate(iso?: string): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : "Operation failed";
}

function matchesFilter(entry: DesktopEntry, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return (
    entry.name.toLowerCase().includes(q) ||
    entry.path.toLowerCase().includes(q) ||
    (entry.type ?? "").toLowerCase().includes(q)
  );
}

function titleCase(value: string): string {
  if (!value) return "";
  return value[0].toUpperCase() + value.slice(1).toLowerCase();
}

function FolderIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" className="inline-block shrink-0 align-[-3px]">
      <path
        d="M2 3.5A1.5 1.5 0 013.5 2H6l1.5 2H13a1 1 0 011 1v7.5a1.5 1.5 0 01-1.5 1.5h-9A1.5 1.5 0 012 12.5V3.5z"
        fill="#0091ff"
      />
    </svg>
  );
}

function FileIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="inline-block shrink-0 align-[-3px]">
      <path
        d="M4.5 1.5h4.586a1 1 0 01.707.293l3.414 3.414a1 1 0 01.293.707V13.5a1 1 0 01-1 1h-8a1 1 0 01-1-1v-11a1 1 0 011-1z"
        fill="#f3f3f3"
        stroke="#bdbdbd"
        strokeWidth="0.75"
      />
      <path d="M9 1.5v3a1 1 0 001 1h3" stroke="#bdbdbd" strokeWidth="0.75" fill="none" />
    </svg>
  );
}

function ChevronIcon() {
  return (
    <svg width="7" height="10" viewBox="0 0 7 10" fill="none" className="inline-block align-[-1px]">
      <path d="M1.5 1L5.5 5L1.5 9" stroke="#bdbdbd" strokeWidth="1.25" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

type ButtonVariant = "default" | "primary" | "danger" | "quiet";

function ActionButton({
  variant = "default",
  className = "",
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: ButtonVariant }) {
  const base =
    "inline-flex h-8 items-center justify-center rounded-md border px-3 text-[12px] font-medium transition-colors disabled:pointer-events-none disabled:opacity-35";
  const styles: Record<ButtonVariant, string> = {
    default: "border-gray-6 bg-white text-gray-12 hover:bg-gray-3 active:bg-gray-4",
    primary: "border-blue-6 bg-blue-3 text-blue-11 hover:bg-blue-4 active:bg-blue-5",
    danger: "border-gray-6 bg-white text-red-11 hover:bg-red-3 active:bg-red-3",
    quiet: "border-transparent bg-transparent text-gray-11 hover:border-gray-6 hover:bg-gray-3",
  };
  return <button className={`${base} ${styles[variant]} ${className}`} {...rest} />;
}

function KindLabel({ entry }: { entry: DesktopEntry }) {
  if (entry.is_dir) return <span className="text-gray-11">Folder</span>;
  return <span className="text-gray-11">{titleCase(entry.type ?? "File")}</span>;
}

export default function App() {
  const [status, setStatus] = useState<DesktopStatus | null>(null);
  const [path, setPath] = useState("/");
  const [entries, setEntries] = useState<DesktopEntry[]>([]);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<DesktopEntry | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const navigateSeqRef = useRef(0);

  const noWs = status !== null && !status.workspace_id;
  const off = noWs;
  const filterActive = filter.trim().length > 0;
  const visible = useMemo(
    () => sortedEntries(entries).filter((entry) => matchesFilter(entry, filter)),
    [entries, filter],
  );
  const canActOnSelection = !off && !loading && selected !== null;

  async function act(fn: () => Promise<void>) {
    setError("");
    try {
      await fn();
    } catch (e) {
      setError(errMsg(e));
    }
  }

  async function refreshStatus() {
    setStatus(await desktopAPI.status());
  }

  async function navigate(next: string) {
    const seq = navigateSeqRef.current + 1;
    navigateSeqRef.current = seq;
    setLoading(true);
    try {
      const r = await desktopAPI.list(normalizePath(next));
      if (navigateSeqRef.current !== seq) return;
      setPath(normalizePath(r.path));
      setEntries(r.entries ?? []);
      setSelected(null);
    } finally {
      if (navigateSeqRef.current === seq) {
        setLoading(false);
      }
    }
  }

  async function goUp() {
    if (path === "/" || off) return;
    await navigate(parentPath(path));
  }

  function clearFilter() {
    setFilter("");
  }

  async function createFolder() {
    const name = window.prompt("Folder name");
    if (!name?.trim()) return;
    await desktopAPI.mkdir(joinPath(path, name));
    await navigate(path);
  }

  async function renameSelected() {
    if (!selected) return;
    const next = window.prompt("Rename to", selected.name);
    if (!next?.trim()) return;
    await desktopAPI.rename(selected.path, joinPath(parentPath(selected.path), next));
    await navigate(path);
  }

  async function deleteSelected() {
    if (!selected) return;
    if (!window.confirm(`Delete ${selected.path}?`)) return;
    await desktopAPI.remove(selected.path);
    await navigate(path);
  }

  useEffect(() => {
    void act(async () => {
      await refreshStatus();
      await navigate("/");
    });
    const id = setInterval(() => void act(refreshStatus), 5000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (!selected) return;
    if (!visible.some((entry) => entry.path === selected.path)) {
      setSelected(null);
    }
  }, [selected, visible]);

  return (
    <div className="flex h-screen select-none bg-gray-1 text-gray-12">
      <aside className="flex w-[220px] shrink-0 flex-col border-r border-gray-6 bg-gray-2">
        <div className="px-4 pt-4 pb-3">
          <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-gray-9">Airstore</div>
          <div className="mt-1 text-[16px] font-semibold">All Files</div>
        </div>

        <nav className="flex-1 px-2 pb-2">
          <div className="mb-1 px-2.5 pt-2 text-[10px] font-semibold uppercase tracking-[0.1em] text-gray-9">
            Shortcuts
          </div>
          {NAV_LINKS.map((link) => {
            const active = path === link && !filterActive;
            return (
              <button
                key={link}
                disabled={off}
                onClick={() => void act(() => navigate(link))}
                className={`mb-px w-full rounded-md px-2.5 py-[5px] text-left text-[13px] transition-colors disabled:pointer-events-none disabled:opacity-35 ${
                  active
                    ? "bg-blue-3 font-medium text-blue-11"
                    : "text-gray-11 hover:bg-gray-3 hover:text-gray-12"
                }`}
              >
                {NAV_LABELS[link]}
              </button>
            );
          })}
        </nav>

        <div className="border-t border-gray-6 px-4 py-2.5">
          <div className="flex items-center gap-1.5 text-[11px] text-gray-11">
            <span className={`h-[6px] w-[6px] rounded-full ${status?.mounted ? "bg-green-9" : "bg-gray-8"}`} />
            {status?.mount_state ?? "unknown"}
          </div>
          {status?.workspace_id && (
            <div className="mt-1 truncate font-mono text-[10px] text-gray-9" title={status.workspace_id}>
              {status.workspace_id.slice(0, 12)}…
            </div>
          )}
        </div>
      </aside>

      <main className="flex min-w-0 flex-1 flex-col bg-white">
        <header className="border-b border-gray-6">
          <div className="flex items-start justify-between gap-3 px-5 py-4">
            <div className="min-w-0">
              <h1 className="truncate text-[20px] leading-tight font-semibold text-gray-12">{folderTitle(path)}</h1>
              <div className="mt-1 flex min-w-0 flex-wrap items-center gap-0.5 text-[12px] text-gray-11">
                {crumbs(path).map((c, i, all) => (
                  <span key={c.path} className="flex min-w-0 items-center gap-0.5">
                    {i > 0 && (
                      <span className="mx-0.5">
                        <ChevronIcon />
                      </span>
                    )}
                    <button
                      disabled={off}
                      className={`truncate transition-colors hover:text-blue-11 disabled:pointer-events-none disabled:opacity-35 ${
                        i === all.length - 1 ? "font-medium text-gray-12" : "text-gray-11"
                      }`}
                      onClick={() => void act(() => navigate(c.path))}
                    >
                      {c.label}
                    </button>
                  </span>
                ))}
              </div>
            </div>

            <div className="flex items-center gap-1.5">
              <ActionButton disabled={off} onClick={() => void act(() => navigate(path))}>
                Refresh
              </ActionButton>
              <ActionButton variant="primary" disabled={off} onClick={() => void act(createFolder)}>
                New Folder
              </ActionButton>
            </div>
          </div>

          <div className="flex items-center justify-between gap-2 border-t border-gray-6 bg-gray-2/50 px-5 py-2">
            <div className="flex items-center gap-1.5">
              <ActionButton variant="quiet" disabled={off || path === "/"} onClick={() => void act(goUp)}>
                Up
              </ActionButton>

              <ActionButton disabled={!canActOnSelection} onClick={() => void act(renameSelected)}>
                Rename
              </ActionButton>
              <ActionButton variant="danger" disabled={!canActOnSelection} onClick={() => void act(deleteSelected)}>
                Delete
              </ActionButton>

              <ActionButton
                disabled={loading}
                onClick={() =>
                  void act(async () => {
                    await desktopAPI.toggleMount();
                    await refreshStatus();
                  })
                }
              >
                {status?.mounted ? "Unmount" : "Mount"}
              </ActionButton>
              <ActionButton
                onClick={() =>
                  void act(async () => {
                    await desktopAPI.openFolder();
                  })
                }
              >
                Show in Finder
              </ActionButton>
            </div>

            <div className="flex items-center gap-1.5">
              <input
                value={filter}
                disabled={off}
                onChange={(e) => setFilter(e.target.value)}
                placeholder="Search this folder"
                className="h-8 w-60 rounded-md border border-gray-6 bg-gray-1 px-2 text-[12px] text-gray-12 outline-none placeholder:text-gray-9 focus:border-blue-9 focus:ring-1 focus:ring-blue-9/25 disabled:pointer-events-none disabled:opacity-35"
              />
              {filterActive && (
                <ActionButton variant="quiet" onClick={clearFilter}>
                  Clear
                </ActionButton>
              )}
            </div>
          </div>
        </header>

        {noWs && (
          <div className="border-b border-yellow-9/30 bg-yellow-3 px-3 py-1.5 text-[12px] text-yellow-11">
            No workspace found — use a workspace token or run{" "}
            <code className="rounded bg-yellow-9/10 px-1 font-mono text-[11px]">airstore login</code>.
          </div>
        )}

        <div className="border-b border-gray-6 bg-gray-2 px-3 py-1 text-[11px] text-gray-11">
          {visible.length} item{visible.length !== 1 ? "s" : ""}
          {filterActive ? ` • filtered by "${filter.trim()}"` : ""}
        </div>

        <div className="relative min-h-0 flex-1 overflow-auto">
          {visible.length === 0 && !loading ? (
            <div className="flex items-center justify-center py-16 text-[13px] text-gray-9">
              {filterActive ? "No matches" : "Empty folder"}
            </div>
          ) : (
            <table className="w-full text-[13px]">
              <thead className="sticky top-0 z-10 bg-gray-2">
                <tr className="text-left text-[11px] font-medium text-gray-11">
                  <th className="border-b border-gray-6 px-3 py-1.5">Name</th>
                  <th className="w-44 border-b border-gray-6 px-3 py-1.5">Date Modified</th>
                  <th className="w-24 border-b border-gray-6 px-3 py-1.5 text-right">Size</th>
                  <th className="w-24 border-b border-gray-6 px-3 py-1.5">Type</th>
                </tr>
              </thead>
              <tbody>
                {visible.map((entry) => {
                  const sel = selected?.path === entry.path;
                  return (
                    <tr
                      key={entry.path}
                      className={`cursor-default border-b border-gray-6/40 transition-colors ${
                        sel ? "bg-blue-3/70" : "hover:bg-gray-3/60"
                      }`}
                      onClick={() => setSelected(entry)}
                      onDoubleClick={() => {
                        if (!entry.is_dir) return;
                        void act(() => navigate(entry.path));
                      }}
                    >
                      <td className="px-3 py-[7px]">
                        <span className="mr-2 inline-flex">{entry.is_dir ? <FolderIcon /> : <FileIcon />}</span>
                        <span className={sel ? "font-medium text-blue-12" : ""}>{entry.name}</span>
                      </td>
                      <td className="px-3 py-[7px] text-gray-11">{fmtDate(entry.modified_at)}</td>
                      <td className="px-3 py-[7px] text-right tabular-nums text-gray-11">{fmtSize(entry.size)}</td>
                      <td className="px-3 py-[7px]">
                        <KindLabel entry={entry} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}

          {loading ? <div className="pointer-events-none absolute inset-x-0 top-0 h-[2px] bg-blue-9/60" /> : null}

          {loading && entries.length === 0 ? (
            <div className="absolute inset-0 flex items-center justify-center text-[13px] text-gray-9">Loading…</div>
          ) : null}
        </div>

        {error && (
          <div className="border-t border-red-9/20 bg-red-3 px-3 py-1.5 text-[12px] text-red-11">{error}</div>
        )}

        <footer className="flex items-center justify-between border-t border-gray-6 bg-gray-2 px-3 py-1 text-[11px] text-gray-11">
          <span className="truncate">{selected ? `Selected: ${selected.path}` : "Select a file or folder"}</span>
          <span className="ml-4 shrink-0 font-mono text-[10px] text-gray-9">{status?.gateway_http_addr ?? "—"}</span>
        </footer>
      </main>
    </div>
  );
}
