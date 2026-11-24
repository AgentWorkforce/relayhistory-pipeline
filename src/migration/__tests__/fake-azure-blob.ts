/**
 * An in-memory Azure Blob container for the strict Azure eraser's and the versioning probe's tests, reached through
 * an injected fetch (no socket). It models what the eraser depends on, following the service's documented behaviour
 * (and what Azurite 3.38 shows for versioning):
 *   - versioning on: every put creates a version (x-ms-version-id); deleting the base blob makes the current version
 *     a previous version (bytes stay); DELETE ?versionid=<current> is refused (OperationNotAllowedOnRootBlob);
 *   - soft delete on: a deleted version or snapshot stays listed with Deleted=true (recoverable) until
 *     deletetype=permanent removes it, which needs the account's permanent delete (else 409);
 *   - snapshots; If-Match on the base delete (412); a base delete with live snapshots is 409 SnapshotsPresent;
 *   - List Blobs include=versions,deleted,snapshots in name order with an opaque NextMarker;
 *   - Shared Key: each request's Authorization is recomputed with the account key and must match (else 403).
 * `hook` can answer any request first (3xx, 403, 5xx, a concurrent write); `requests` records each one.
 */
import { azureSharedKeyAuthorization } from "../../../packages/object-store/src/azure.ts";

export interface FakeRecord {
  key: string;
  kind: "version" | "snapshot" | "base";
  /** Version id, snapshot time, or "" for an unversioned base blob. */
  id: string;
  current: boolean;
  deleted: boolean;
  etag: string;
  bytes: Uint8Array;
  seq: number;
}

export interface FakeAzureOptions {
  account?: string;
  container?: string;
  accountKey?: string;
  versioning?: boolean;
  softDelete?: boolean;
  permanentDelete?: boolean;
  /** Omit x-ms-version-id even with versioning (a malformed / non-versioned answer). */
  versionHeader?: (id: string) => string | null;
}

export const FAKE_ENDPOINT = "http://127.0.0.1:1/fxacct";

export class FakeAzureBlob {
  readonly account: string;
  readonly container: string;
  readonly accountKey: string;
  versioning: boolean;
  softDelete: boolean;
  permanentDelete: boolean;
  records: FakeRecord[] = [];
  /** Every request, with its fetch redirect mode (the eraser and the probe must send "manual"). */
  requests: { method: string; url: URL; headers: Headers; redirect: RequestRedirect | undefined }[] = [];
  hook?: (req: { method: string; url: URL; headers: Headers }) => Response | undefined | Promise<Response | undefined>;
  #clock = Date.parse("2026-11-23T10:00:00Z");
  #seq = 0;
  readonly #opts: FakeAzureOptions;

  constructor(opts: FakeAzureOptions = {}) {
    this.#opts = opts;
    this.account = opts.account ?? "fxacct";
    this.container = opts.container ?? "fx-container";
    this.accountKey = opts.accountKey ?? Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64");
    this.versioning = opts.versioning ?? true;
    this.softDelete = opts.softDelete ?? false;
    this.permanentDelete = opts.permanentDelete ?? false;
  }

  #stamp(): string {
    this.#clock += 1;
    return new Date(this.#clock).toISOString().replace(/\.(\d{3})Z$/, ".$10000Z");
  }

  #etag(): string {
    return `0x${(0x1d000000000000 + ++this.#seq).toString(16).toUpperCase()}`;
  }

  /** Seed or write a blob as a put would. Returns the new version id ("" when unversioned). */
  put(key: string, body = "x"): string {
    const bytes = new TextEncoder().encode(body);
    if (this.versioning) {
      for (const r of this.records) if (r.key === key && r.kind === "version") r.current = false;
      const id = this.#stamp();
      this.records.push({ key, kind: "version", id, current: true, deleted: false, etag: this.#etag(), bytes, seq: ++this.#seq });
      return id;
    }
    this.records = this.records.filter((r) => !(r.key === key && r.kind === "base" && !r.deleted));
    this.records.push({ key, kind: "base", id: "", current: true, deleted: false, etag: this.#etag(), bytes, seq: ++this.#seq });
    return "";
  }

  snapshot(key: string): string {
    const src = this.records.find((r) => r.key === key && r.current && !r.deleted);
    if (!src) throw new Error("no current blob");
    const id = this.#stamp();
    this.records.push({ ...src, kind: "snapshot", id, current: false, seq: ++this.#seq });
    return id;
  }

  /** Every recoverable copy of a key (live or soft-deleted). */
  copies(key: string): FakeRecord[] {
    return this.records.filter((r) => r.key === key);
  }

  #sorted(): FakeRecord[] {
    const rank = (r: FakeRecord) => (r.kind === "snapshot" ? 0 : r.kind === "version" ? 1 : 2);
    return [...this.records].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : rank(a) - rank(b) || a.seq - b.seq));
  }

  readonly fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(String(input));
    const method = init?.method ?? "GET";
    const headers = new Headers(init?.headers);
    const req = { method, url, headers, redirect: init?.redirect };
    this.requests.push(req);
    const hooked = await this.hook?.(req);
    if (hooked) return hooked;
    const signedWith = headers.get("authorization") ?? "";
    const expected = await azureSharedKeyAuthorization(this.account, this.accountKey, method, url, headers,
      init?.body ? (init.body as Uint8Array).length : 0);
    if (signedWith !== expected) return new Response("<Error><Code>AuthenticationFailed</Code></Error>", { status: 403 });
    const base = `/fxacct/${this.container}`;
    if (!url.pathname.startsWith(base)) return new Response(null, { status: 404 });
    const key = decodeURIComponent(url.pathname.slice(base.length + 1));
    const q = url.searchParams;
    if (method === "GET" && q.get("comp") === "list") return this.#list(q);
    if (method === "PUT") return this.#put(key, headers, init?.body as Uint8Array | undefined);
    if (method === "DELETE") return this.#delete(key, q, headers);
    return new Response(null, { status: 405 });
  }) as typeof fetch;

  #put(key: string, headers: Headers, body?: Uint8Array): Response {
    if (headers.get("if-none-match") === "*" && this.records.some((r) => r.key === key && r.current && !r.deleted))
      return new Response(null, { status: 409 });
    const id = this.put(key, new TextDecoder().decode(body ?? new Uint8Array()));
    const rec = this.records[this.records.length - 1]!;
    const h = new Headers({ etag: `"${rec.etag}"` });
    const header = this.#opts.versionHeader ? this.#opts.versionHeader(id) : id || null;
    if (header) h.set("x-ms-version-id", header);
    return new Response(null, { status: 201, headers: h });
  }

  #delete(key: string, q: URLSearchParams, headers: Headers): Response {
    const permanent = q.get("deletetype") === "permanent";
    const versionId = q.get("versionid");
    const snap = q.get("snapshot");
    const ok = () => new Response(null, { status: 202 });
    const drop = (r: FakeRecord) => { this.records = this.records.filter((x) => x !== r); };
    if (versionId !== null || snap !== null) {
      const r = this.records.find((x) => x.key === key && x.kind === (snap !== null ? "snapshot" : "version") && x.id === (snap ?? versionId));
      if (!r) return new Response(null, { status: 404 });
      if (permanent) {
        if (!r.deleted || !this.permanentDelete) return new Response("<Error><Code>PermanentDeleteNotAllowed</Code></Error>", { status: 409 });
        drop(r);
        return ok();
      }
      if (r.kind === "version" && r.current) return new Response("<Error><Code>OperationNotAllowedOnRootBlob</Code></Error>", { status: 403 });
      if (r.deleted) return new Response(null, { status: 404 });
      if (this.softDelete) r.deleted = true;
      else drop(r);
      return ok();
    }
    const current = this.records.find((x) => x.key === key && x.current && !x.deleted);
    if (!current) return new Response(null, { status: 404 });
    const ifMatch = headers.get("if-match");
    if (ifMatch && ifMatch.replace(/^"|"$/g, "") !== current.etag) return new Response(null, { status: 412 });
    if (this.records.some((x) => x.key === key && x.kind === "snapshot" && !x.deleted)) return new Response("<Error><Code>SnapshotsPresent</Code></Error>", { status: 409 });
    if (current.kind === "version") current.current = false;
    else if (this.softDelete) { current.deleted = true; current.current = false; }
    else drop(current);
    return ok();
  }

  #list(q: URLSearchParams): Response {
    const prefix = q.get("prefix") ?? "";
    const max = Number(q.get("maxresults") ?? 5000);
    const marker = q.get("marker");
    const include = new Set((q.get("include") ?? "").split(","));
    let all = this.#sorted().filter((r) => r.key.startsWith(prefix))
      .filter((r) => (r.deleted ? include.has("deleted") : true))
      .filter((r) => (r.kind === "snapshot" ? include.has("snapshots") : true))
      .filter((r) => (r.kind === "version" && !r.current ? include.has("versions") : true));
    if (marker) {
      const after = JSON.parse(Buffer.from(marker, "base64").toString()) as { key: string; seq: number; kind: string };
      const order = this.#sorted();
      const pos = order.findIndex((r) => r.seq === after.seq);
      const anchor = pos >= 0 ? order.slice(0, pos + 1).map((r) => r.seq) : null;
      all = anchor ? all.filter((r) => !anchor.includes(r.seq)) : all.filter((r) => r.key > after.key);
    }
    const page = all.slice(0, max);
    const truncated = all.length > max;
    const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    const blob = (r: FakeRecord) => `<Blob><Name>${esc(r.key)}</Name>`
      + (r.kind === "snapshot" ? `<Snapshot>${r.id}</Snapshot>` : "")
      + (r.kind === "version" ? `<VersionId>${r.id}</VersionId>` : "")
      + (r.kind === "version" && r.current ? "<IsCurrentVersion>true</IsCurrentVersion>" : "")
      + (r.deleted ? "<Deleted>true</Deleted>" : "")
      + `<Properties><Last-Modified>${new Date(this.#clock).toUTCString()}</Last-Modified><Etag>${r.etag}</Etag><Content-Length>${r.bytes.length}</Content-Length></Properties></Blob>`;
    const last = page[page.length - 1];
    const next = truncated && last ? Buffer.from(JSON.stringify({ key: last.key, seq: last.seq, kind: last.kind })).toString("base64") : "";
    const xml = `﻿<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="${FAKE_ENDPOINT}" ContainerName="${this.container}">`
      + `<Prefix>${esc(prefix)}</Prefix>${marker ? `<Marker>${marker}</Marker>` : ""}<MaxResults>${max}</MaxResults><Blobs>${page.map(blob).join("")}</Blobs>`
      + (next ? `<NextMarker>${next}</NextMarker>` : "<NextMarker />") + "</EnumerationResults>";
    return new Response(xml, { status: 200, headers: { "content-type": "application/xml" } });
  }
}
