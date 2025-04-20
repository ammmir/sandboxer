interface SandboxData {
  id: string;
  label?: string;
  image?: string;
  interactive?: boolean;
  [key: string]: any;
}

export class Sandbox {
  constructor(
    private client: Sandboxer,
    public id: string,
    public label?: string,
    public image?: string,
    public interactive?: boolean,
    private _data: any = {}
  ) {}

  async exec(code: string): Promise<any> {
    const res = await this.client.request(`/sandboxes/${this.id}/execute`, {
      method: "POST",
      json: { code },
    });
    return await res.json();
  }

  async fork(label: string): Promise<Sandbox> {
    const res = await this.client.request(`/sandboxes/${this.id}/fork`, {
      method: "POST",
      json: { label },
    });
    const data = await res.json() as SandboxData;
    return new Sandbox(this.client, data.id, data.label, data.image, data.interactive, data);
  }

  async terminate(): Promise<void> {
    await this.client.request(`/sandboxes/${this.id}`, { method: "DELETE" });
  }

  async tree(): Promise<any> {
    const res = await this.client.request(`/sandboxes/${this.id}/tree`);
    return await res.json();
  }

  async coalesce(finalSandbox: Sandbox): Promise<Sandbox> {
    const res = await this.client.request(
      `/sandboxes/${this.id}/coalesce/${finalSandbox.id}`,
      { method: "POST" }
    );
    const data = await res.json() as SandboxData;
    return new Sandbox(this.client, data.id, data.label, data.image, data.interactive, data);
  }

  async logs(limit = 1000): Promise<any> {
    const params = new URLSearchParams({ limit: limit.toString() });
    const res = await this.client.request(`/sandboxes/${this.id}/execution-logs?${params}`);
    return await res.json();
  }

  async *streamLogs(limit = 1000): AsyncGenerator<string> {
    const params = new URLSearchParams({ limit: limit.toString() });
    const res = await this.client.request(`/sandboxes/${this.id}/execution-logs?${params}`);
    const reader = res.body?.getReader();
    const decoder = new TextDecoder();

    if (!reader) throw new Error("Streaming not supported");

    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (line.trim()) yield line;
      }
    }
    if (buffer.trim()) yield buffer.trim();
  }

  async proxy(path = ""): Promise<Response> {
    return await this.client.request(`/sandboxes/${this.id}/proxy/${path}`);
  }
}

export class Sandboxer {
  constructor(private baseUrl: string, private token: string) {}

  async create(image: string, label: string, interactive = false): Promise<Sandbox> {
    const res = await this.request("/sandboxes", {
      method: "POST",
      json: { image, label, interactive },
    });
    const data = await res.json() as SandboxData;
    return new Sandbox(this, data.id, data.label, data.image, data.interactive, data);
  }

  async request(path: string, opts: RequestInit & { json?: any } = {}): Promise<Response> {
    const headers: any = {
      "Authorization": `Bearer ${this.token}`,
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    };

    const body = opts.json ? JSON.stringify(opts.json) : undefined;

    const res = await fetch(`${this.baseUrl}${path}`, {
      method: opts.method ?? "GET",
      headers,
      body,
    });

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Request failed: ${res.status} ${res.statusText} - ${err}`);
    }

    return res;
  }
}
