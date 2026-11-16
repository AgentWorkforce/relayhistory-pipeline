/** In-memory S3 and DynamoDB behind `fetch`, with switches for the broken behaviours the harness must catch. */

export interface FakeS3Options {
  /** Treat `If-None-Match: *` as absent: a create becomes an overwrite. */
  ignoreIfNoneMatch?: boolean;
  /** Treat `If-Match` as absent: a stale CAS succeeds. */
  ignoreIfMatch?: boolean;
  /** Answer any `If-Match` PUT with 501. */
  rejectIfMatch?: boolean;
}

interface Stored {
  body: Uint8Array;
  etag: string;
  meta: [string, string][];
  modified: Date;
}

const xmlError = (status: number, code: string) => new Response(`<Error><Code>${code}</Code></Error>`, { status });

export class FakeS3 {
  readonly objects = new Map<string, Stored>();
  readonly requests: { method: string; key: string; status: number }[] = [];
  #etag = 0;
  constructor(readonly options: FakeS3Options = {}) {}

  readonly fetch: typeof fetch = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    const url = new URL(typeof input === "string" || input instanceof URL ? input : input.url);
    const method = (init?.method ?? "GET").toUpperCase();
    const headers = new Headers(init?.headers);
    // Path-style (`/bucket/key`), as the object-store package sends with an endpoint override.
    const path = decodeURIComponent(url.pathname).replace(/^\//, "");
    const slash = path.indexOf("/");
    const k = slash < 0 ? "" : path.slice(slash + 1);
    // Reading the body yields, like a real upload; the check-and-set below is then atomic.
    const body = init?.body ? new Uint8Array(await new Response(init.body as BodyInit).arrayBuffer()) : new Uint8Array();
    const res = this.#handle(method, url, k, headers, body);
    this.requests.push({ method, key: k, status: res.status });
    return res;
  }) as typeof fetch;

  #handle(method: string, url: URL, k: string, headers: Headers, body: Uint8Array): Response {
    if (method === "GET" && url.searchParams.get("list-type") === "2") {
      const prefix = url.searchParams.get("prefix") ?? "";
      const max = Number(url.searchParams.get("max-keys") ?? 1000);
      const after = url.searchParams.get("continuation-token") ?? "";
      const keys = [...this.objects.keys()].filter((x) => x.startsWith(prefix) && x > after).sort();
      const page = keys.slice(0, max);
      const truncated = keys.length > max;
      return new Response(
        `<ListBucketResult><IsTruncated>${truncated}</IsTruncated>${page.map((x) => `<Contents><Key>${x}</Key><Size>${this.objects.get(x)!.body.length}</Size><ETag>${this.objects.get(x)!.etag}</ETag><LastModified>${this.objects.get(x)!.modified.toISOString()}</LastModified></Contents>`).join("")}${truncated ? `<NextContinuationToken>${page[page.length - 1]}</NextContinuationToken>` : ""}</ListBucketResult>`,
        { status: 200 },
      );
    }
    const existing = this.objects.get(k);
    if (method === "PUT") {
      if (headers.get("if-none-match") === "*" && existing && !this.options.ignoreIfNoneMatch) return xmlError(412, "PreconditionFailed");
      const ifMatch = headers.get("if-match");
      if (ifMatch !== null) {
        if (this.options.rejectIfMatch) return xmlError(501, "NotImplemented");
        if (!this.options.ignoreIfMatch) {
          if (!existing) return xmlError(404, "NoSuchKey");
          if (existing.etag !== ifMatch) return xmlError(412, "PreconditionFailed");
        }
      }
      const etag = `"${++this.#etag}"`;
      const meta: [string, string][] = [];
      headers.forEach((v, n) => {
        if (n.startsWith("x-amz-meta-")) meta.push([n, v]);
      });
      this.objects.set(k, { body, etag, meta, modified: new Date() });
      return new Response(null, { status: 200, headers: { etag } });
    }
    if (method === "DELETE") {
      this.objects.delete(k);
      return new Response(null, { status: 204 });
    }
    if (!existing) return method === "HEAD" ? new Response(null, { status: 404 }) : xmlError(404, "NoSuchKey");
    const out = new Headers({ etag: existing.etag, "content-length": String(existing.body.length), "last-modified": existing.modified.toUTCString() });
    for (const [n, v] of existing.meta) out.set(n, v);
    return new Response(method === "HEAD" ? null : (existing.body as Uint8Array<ArrayBuffer>), { status: 200, headers: out });
  }
}

type Attr = { S?: string; N?: string };
type Row = Record<string, Attr>;

/** The DynamoDB calls the harness makes, evaluated for exactly the expressions it sends. */
export class FakeDynamo {
  readonly rows = new Map<string, Row>();
  readonly tokens = new Map<string, string>();
  calls = 0;

  #id = (key: Row) => `${key.pk!.S}|${key.sk!.S}`;

  #check(row: Row | undefined, expression: string | undefined, values: Record<string, Attr>): boolean {
    if (!expression) return true;
    if (expression === "attribute_not_exists(pk)") return row === undefined;
    if (expression === "attribute_not_exists(seq)") return row?.seq === undefined;
    if (expression === "seq = :prev") return row?.seq?.N === values[":prev"]!.N;
    throw new Error(`fake dynamo: unknown condition ${expression}`);
  }

  #update(key: Row, expression: string, values: Record<string, Attr>): void {
    const row = this.rows.get(this.#id(key)) ?? { ...key };
    const set = /^SET (\w+) = (:\w+)$/.exec(expression);
    const add = /^ADD (\w+) (:\w+)$/.exec(expression);
    if (set) row[set[1]!] = values[set[2]!]!;
    else if (add) row[add[1]!] = { N: String(Number(row[add[1]!]?.N ?? 0) + Number(values[add[2]!]!.N)) };
    else throw new Error(`fake dynamo: unknown update ${expression}`);
    this.rows.set(this.#id(key), row);
  }

  readonly fetch: typeof fetch = (async (_input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    this.calls++;
    const operation = new Headers(init?.headers).get("x-amz-target")!.split(".")[1]!;
    const input = JSON.parse(new TextDecoder().decode(init!.body as Uint8Array)) as Record<string, any>;
    const fail = (type: string, extra: Record<string, unknown> = {}) => new Response(JSON.stringify({ __type: `com.amazonaws.dynamodb.v20120810#${type}`, ...extra }), { status: 400 });
    const ok = (body: unknown = {}) => new Response(JSON.stringify(body), { status: 200 });
    if (operation === "GetItem") {
      const row = this.rows.get(this.#id(input.Key));
      return ok(row ? { Item: row } : {});
    }
    if (operation === "UpdateItem") {
      this.#update(input.Key, input.UpdateExpression, input.ExpressionAttributeValues ?? {});
      return ok();
    }
    if (operation === "Query") {
      const pk = input.ExpressionAttributeValues[":pk"].S as string;
      const sk = input.ExpressionAttributeValues[":sk"]?.S as string | undefined;
      return ok({ Items: [...this.rows.values()].filter((r) => r.pk!.S === pk && (sk === undefined || r.sk!.S!.startsWith(sk))) });
    }
    if (operation === "TransactWriteItems") {
      const token = input.ClientRequestToken as string | undefined;
      const fingerprint = JSON.stringify(input.TransactItems);
      if (token && this.tokens.has(token)) return this.tokens.get(token) === fingerprint ? ok() : fail("IdempotentParameterMismatchException");
      const actions = (input.TransactItems as Record<string, any>[]).map((t) => (t.Update ? { kind: "update" as const, ...t.Update } : { kind: "put" as const, ...t.Put }));
      const reasons = actions.map((a) => {
        const key: Row = a.kind === "update" ? a.Key : { pk: a.Item.pk, sk: a.Item.sk };
        return this.#check(this.rows.get(this.#id(key)), a.ConditionExpression, a.ExpressionAttributeValues ?? {}) ? "None" : "ConditionalCheckFailed";
      });
      if (reasons.some((r) => r !== "None")) return fail("TransactionCanceledException", { CancellationReasons: reasons.map((Code) => ({ Code })) });
      for (const a of actions) {
        if (a.kind === "update") this.#update(a.Key, a.UpdateExpression, a.ExpressionAttributeValues ?? {});
        else this.rows.set(this.#id(a.Item), a.Item);
      }
      if (token) this.tokens.set(token, fingerprint);
      return ok();
    }
    return fail("UnknownOperationException");
  }) as typeof fetch;
}
