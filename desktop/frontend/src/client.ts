export type DesktopEntry = {
  name: string;
  path: string;
  is_dir: boolean;
  size?: number;
  modified_at?: string;
  type?: string;
};

export type DesktopStatus = {
  mounted: boolean;
  mount_state: string;
  mount_point: string;
  gateway_addr: string;
  gateway_http_addr: string;
  workspace_id?: string;
  window_visible: boolean;
  autostart_enabled: boolean;
  last_error?: string;
};

type ListResponse = {
  path: string;
  entries: DesktopEntry[];
};

type SearchResponse = {
  query: string;
  results: DesktopEntry[];
};

function errorMessage(payload: unknown): string {
  if (payload && typeof payload === "object" && "error" in payload && typeof payload.error === "string") {
    return payload.error;
  }
  if (payload && typeof payload === "object" && "message" in payload && typeof payload.message === "string") {
    return payload.message;
  }
  return "Request failed";
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });

  let payload: unknown = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    throw new Error(errorMessage(payload));
  }
  return payload as T;
}

export const desktopAPI = {
  status: () => request<DesktopStatus>("/api/desktop/status"),
  list: (path: string) => request<ListResponse>(`/api/desktop/fs/list?path=${encodeURIComponent(path)}`),
  search: (query: string, limit = 50) =>
    request<SearchResponse>(`/api/desktop/fs/search?q=${encodeURIComponent(query)}&limit=${limit}`),
  mkdir: (path: string) => request<{ ok: true }>("/api/desktop/fs/mkdir", { method: "POST", body: JSON.stringify({ path }) }),
  rename: (oldPath: string, newPath: string) =>
    request<{ ok: true }>("/api/desktop/fs/rename", {
      method: "POST",
      body: JSON.stringify({ old_path: oldPath, new_path: newPath }),
    }),
  remove: (path: string) =>
    request<{ ok: true }>(`/api/desktop/fs/delete?path=${encodeURIComponent(path)}`, { method: "DELETE" }),
  toggleMount: () => request<{ ok: true }>("/api/desktop/mount/toggle", { method: "POST" }),
  openFolder: () => request<{ ok: true }>("/api/desktop/open-folder", { method: "POST" }),
};
