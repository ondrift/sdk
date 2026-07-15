/**
 * Drift SDK for Node.js Atomic functions.
 *
 * Provides:
 *   - run(handler): Entry point (deployed or local mode).
 *   - Backbone helpers: secret, cache, nosql, queue, blob, lock.
 *   - log(msg): Writes to stderr (captured by runner).
 *   - httpRequest(): Outbound HTTP from within a function.
 *
 * Uses only built-in APIs (process, fetch, http). Zero dependencies.
 */

"use strict";

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

function run(handler) {
  if (process.env.DRIFT_RUNTIME) {
    _runDeployed(handler);
  } else {
    _runLocal(handler);
  }
}

function _runDeployed(handler) {
  let data = "";
  process.stdin.on("data", (chunk) => (data += chunk));
  process.stdin.on("end", async () => {
    try {
      const req = JSON.parse(data);
      if (typeof req.query === "string") {
        const parsed = {};
        for (const pair of req.query.split("&")) {
          if (!pair) continue;
          const eq = pair.indexOf("=");
          const k = eq < 0 ? pair : pair.slice(0, eq);
          const v = eq < 0 ? "" : pair.slice(eq + 1);
          parsed[decodeURIComponent(k)] = decodeURIComponent(v);
        }
        req.query = parsed;
      }
      const resp = await handler(req);
      process.stdout.write(JSON.stringify(resp));
    } catch (err) {
      process.stdout.write(
        JSON.stringify({ status: 500, message: String(err), payload: null })
      );
    }
  });
}

function _runLocal(handler) {
  const http = require("http");
  const port = parseInt(process.env.PORT || "8080", 10);

  const server = http.createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;

    let parsed = null;
    if (body) {
      try {
        parsed = JSON.parse(body);
      } catch {
        parsed = body;
      }
    }

    const url = new URL(req.url, `http://localhost:${port}`);
    const headers = {};
    for (const [k, v] of Object.entries(req.headers)) {
      headers[k] = Array.isArray(v) ? v[0] : v;
    }

    const funcReq = {
      path:url.pathname,
      headers,
      query: url.search ? url.search.slice(1) : "",
      body: parsed,
    };

    try {
      const resp = await handler(funcReq);
      const out = JSON.stringify(resp);
      res.writeHead(resp.status || 200, { "Content-Type": "application/json" });
      res.end(out);
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: 500, message: String(err) }));
    }
  });

  server.listen(port, () => {
    process.stderr.write(`drift-sdk: local server starting on :${port}\n`);
  });
}

// ---------------------------------------------------------------------------
// Backbone transport
// ---------------------------------------------------------------------------

let _backboneUrl = null;
let _backboneAgent = null;
let _backboneRequestOpts = null;

function _getBackboneUrl() {
  if (_backboneUrl === null) {
    _backboneUrl = process.env.BACKBONE_URL || "";
  }
  return _backboneUrl;
}

// _initBackboneTransport parses BACKBONE_URL once and caches a
// node http.Agent + the per-request options that route to it.
// BACKBONE_URL is an http://host:port TCP URL.
function _initBackboneTransport(rawUrl) {
  const http = require("http");
  const u = new URL(rawUrl);
  _backboneAgent = new http.Agent({ keepAlive: true, maxSockets: 10 });
  _backboneRequestOpts = {
    host: u.hostname,
    port: u.port || 80,
    agent: _backboneAgent,
  };
}

// extraHeaders is an optional plain object merged in ahead of Content-Type —
// today its one caller is Deed.Pocket, whose routes are JWT-gated and need
// an Authorization: Bearer header (see _callAuth below).
function _backboneRequest(method, path, body, contentType, extraHeaders) {
  const http = require("http");
  if (_backboneRequestOpts === null) _initBackboneTransport(_getBackboneUrl());

  return new Promise((resolve, reject) => {
    const headers = Object.assign({}, extraHeaders);
    if (body) headers["Content-Type"] = contentType || "application/json";
    const opts = Object.assign({}, _backboneRequestOpts, {
      method,
      path: "/" + path,
      headers,
    });
    const req = http.request(opts, (resp) => {
      const chunks = [];
      resp.on("data", (c) => chunks.push(c));
      resp.on("end", () => {
        resolve({ status: resp.statusCode, body: Buffer.concat(chunks) });
      });
    });
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

async function _call(method, path, body) {
  const base = _getBackboneUrl();
  if (!base) return _callLocal(method, path, body);

  const payload = body !== undefined && body !== null ? JSON.stringify(body) : null;
  const { status, body: respBody } = await _backboneRequest(method, path, payload);
  // A non-2xx response body is an ERROR, not data — matches Go's
  // callBackboneHTTP and Deed's _callChecked below. Without this, a
  // 404/409/etc body (e.g. Secret.get on an undeclared name, a lock
  // conflict) was returned to the caller as if it were the value.
  if (status >= 400) {
    const text = respBody.toString("utf8").trim();
    throw new Error(`drift: backbone ${method} ${path}: HTTP ${status}${text ? `: ${text}` : ""}`);
  }
  if (status === 204 || respBody.length === 0) return null;
  const text = respBody.toString("utf8");
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function _callRaw(method, path, dataBytes, contentType) {
  const base = _getBackboneUrl();
  if (!base) {
    _store.blobs[path] = dataBytes;
    return null;
  }
  const { status, body: respBody } = await _backboneRequest(
    method,
    path,
    dataBytes,
    contentType || "application/octet-stream",
  );
  if (status >= 400) {
    throw new Error(`blob op ${method} ${path}: HTTP ${status} ${respBody.toString("utf8")}`);
  }
  // Preserve the previous fetch-based ArrayBuffer return shape.
  return respBody.buffer.slice(respBody.byteOffset, respBody.byteOffset + respBody.byteLength);
}

// ---------------------------------------------------------------------------
// Deed transport
// ---------------------------------------------------------------------------
//
// Deed has its own listener/port now (DEED_URL) — a separate connection
// pool from Backbone's, mirroring _backboneUrl/_backboneAgent/
// _backboneRequest exactly (sharing one Agent between the two would pool
// sockets to the wrong port on every call that alternates between Backbone
// and Deed). Like the generic `_call` above (which now applies the same
// status >= 400 check), these treat a non-2xx response as an error rather
// than handing the caller whatever body came back — the "a get on a
// missing item is an error, not a silent null" convention every Backbone/
// Deed primitive shares (mirrors Go's callBackboneHTTP, applied uniformly).

let _deedUrl = null;
let _deedAgent = null;
let _deedRequestOpts = null;

function _getDeedUrl() {
  if (_deedUrl === null) {
    _deedUrl = process.env.DEED_URL || "";
  }
  return _deedUrl;
}

function _initDeedTransport(rawUrl) {
  const http = require("http");
  const u = new URL(rawUrl);
  _deedAgent = new http.Agent({ keepAlive: true, maxSockets: 10 });
  _deedRequestOpts = {
    host: u.hostname,
    port: u.port || 80,
    agent: _deedAgent,
  };
}

function _deedRequest(method, path, body, contentType, extraHeaders) {
  const http = require("http");
  if (_deedRequestOpts === null) _initDeedTransport(_getDeedUrl());

  return new Promise((resolve, reject) => {
    const headers = Object.assign({}, extraHeaders);
    if (body) headers["Content-Type"] = contentType || "application/json";
    const opts = Object.assign({}, _deedRequestOpts, {
      method,
      path: "/" + path,
      headers,
    });
    const req = http.request(opts, (resp) => {
      const chunks = [];
      resp.on("data", (c) => chunks.push(c));
      resp.on("end", () => {
        resolve({ status: resp.statusCode, body: Buffer.concat(chunks) });
      });
    });
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

async function _callChecked(method, path, body) {
  const base = _getDeedUrl();
  if (!base) {
    throw new Error("drift: deed requires a running slice (DEED_URL) — not available in local dev");
  }
  const payload = body !== undefined && body !== null ? JSON.stringify(body) : null;
  const { status, body: respBody } = await _deedRequest(method, path, payload);
  if (status >= 400) {
    throw new Error(`deed ${method} ${path}: HTTP ${status} ${respBody.toString("utf8")}`);
  }
  if (status === 204 || respBody.length === 0) return null;
  const text = respBody.toString("utf8");
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

// _callAuth is _callChecked with a bearer token attached — used only by
// Deed.Pocket, whose routes are JWT-gated (unlike every other loopback-open
// Backbone/Deed primitive). Local dev (no DEED_URL) has no JWT
// verification to check a token against, so — same as KeyAuth/JWT already
// — this call isn't available without a running slice.
async function _callAuth(method, path, token, body) {
  const base = _getDeedUrl();
  if (!base) {
    throw new Error("drift: deed pocket requires a running slice (DEED_URL) — not available in local dev");
  }
  const payload = body !== undefined && body !== null ? JSON.stringify(body) : null;
  const { status, body: respBody } = await _deedRequest(
    method, path, payload, "application/json", { Authorization: `Bearer ${token}` },
  );
  if (status >= 400) {
    throw new Error(`deed ${method} ${path}: HTTP ${status} ${respBody.toString("utf8")}`);
  }
  if (status === 204 || respBody.length === 0) return null;
  const text = respBody.toString("utf8");
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

// In-memory backbone for local dev.
const _store = {
  nosql: {},
  cache: {},
  queues: {},
  blobs: {},
  locks: {},
  nextId: 0,
};

function _callLocal(method, path, body) {
  const [basePath, qs] = path.split("?", 2);
  const query = {};
  if (qs) {
    for (const pair of qs.split("&")) {
      const [k, v] = pair.split("=", 2);
      query[decodeURIComponent(k)] = decodeURIComponent(v || "");
    }
  }

  // NoSQL
  if (basePath === "write" && method === "POST") {
    const col = (body && body.collection) || "default";
    if (!_store.nosql[col]) _store.nosql[col] = {};
    _store.nextId++;
    const key = String(_store.nextId);
    _store.nosql[col][key] = body;
    return { key };
  }
  if (basePath === "read" && method === "GET") {
    const col = query.collection || "default";
    return (_store.nosql[col] || {})[query.key] || null;
  }
  if (basePath === "nosql/list" && method === "GET") {
    const col = query.collection || "default";
    const docs = _store.nosql[col] || {};
    const results = [];
    for (const doc of Object.values(docs)) {
      if (query.field && String(doc[query.field]) !== query.value) continue;
      results.push(doc);
    }
    return results;
  }
  if (basePath === "nosql/drop" && method === "POST") {
    delete _store.nosql[query.collection];
    return null;
  }

  // Cache
  if (basePath === "cache/set" && method === "POST") {
    _store.cache[(body && body.key) || ""] = body && body.value;
    return null;
  }
  if (basePath === "cache/get" && method === "GET") {
    return _store.cache[query.key] !== undefined ? _store.cache[query.key] : null;
  }
  if (basePath === "cache/del") {
    delete _store.cache[query.key];
    return null;
  }

  // Queue
  if (basePath === "queue/push" && method === "POST") {
    const name = (body && body.queue) || "";
    if (!_store.queues[name]) _store.queues[name] = [];
    _store.queues[name].push(body && body.body);
    return null;
  }
  if (basePath === "queue/pop" && method === "POST") {
    const name = (body && body.queue) || "";
    const q = _store.queues[name] || [];
    if (q.length === 0) return null;
    return q.shift();
  }

  // Blob
  if (basePath === "blob/put" && method === "POST") {
    _store.blobs[(body && body.name) || ""] = body && body.data;
    return null;
  }
  if (basePath === "blob/get" && method === "GET") {
    return _store.blobs[query.name] !== undefined ? _store.blobs[query.name] : null;
  }

  // Secret — in local dev, read from environment variables (loaded from .env by the CLI)
  if (basePath === "secret/get" && method === "GET") {
    return process.env[query.name] || null;
  }

  // Lock
  if (basePath === "lock/acquire" && method === "POST") {
    const name = (body && body.name) || "";
    if (_store.locks[name]) return null;
    _store.nextId++;
    const token = `local-lock-${_store.nextId}`;
    _store.locks[name] = token;
    return { token };
  }
  if (basePath === "lock/release" && method === "POST") {
    delete _store.locks[(body && body.name) || ""];
    return null;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Secret
// ---------------------------------------------------------------------------

// Read order:
//   1. Persistent-worker per-request store (globalThis.__driftSecrets) —
//      populated by the lang server's AsyncLocalStorage from req.secrets.
//   2. DRIFT_SECRET_<NAME> env var — set by the runner for native subprocess
//      invocations.
//   3. Backbone HTTP — local dev only; production /secret/get is SAT-guarded.
const secret = {
  get: async (name) => {
    const store = globalThis.__driftSecrets && globalThis.__driftSecrets.getStore();
    if (store && Object.prototype.hasOwnProperty.call(store, name)) {
      return store[name];
    }
    const envVal = process.env["DRIFT_SECRET_" + name.toUpperCase()];
    if (envVal !== undefined) return envVal;
    const resp = await _call("GET", `secret/get?name=${encodeURIComponent(name)}`);
    return typeof resp === "string" ? resp : resp ? JSON.stringify(resp) : "";
  },
  set: (name, value) => _call("POST", "secret/set", { name, value }),
  delete: (name) => _call("DELETE", `secret/delete?name=${encodeURIComponent(name)}`),
};

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

const cache = {
  get: (key) => _call("GET", `cache/get?key=${encodeURIComponent(key)}`),
  set: (key, value, ttl) => {
    const payload = { key, value };
    if (ttl > 0) payload.ttl = ttl;
    return _call("POST", "cache/set", payload);
  },
  delete: (key) => _call("DELETE", `cache/del?key=${encodeURIComponent(key)}`),
};

// ---------------------------------------------------------------------------
// NoSQL
// ---------------------------------------------------------------------------

const nosql = {
  collection: (name) => ({
    insert: async (doc) => {
      const payload = { collection: name, ...(typeof doc === "object" ? doc : { data: doc }) };
      const resp = await _call("POST", "write", payload);
      return (resp && resp.key) || "";
    },
    read: (key) =>
      _call("GET", `read?collection=${encodeURIComponent(name)}&key=${encodeURIComponent(key)}`),
    get: async (id) => {
      const path = `nosql/list?collection=${encodeURIComponent(name)}&field=_id&value=${encodeURIComponent(id)}`;
      const rows = await _call("GET", path);
      const arr = Array.isArray(rows) ? rows : [];
      return arr[0] || null;
    },
    delete: (key) =>
      _call("POST", `nosql/delete?collection=${encodeURIComponent(name)}&key=${encodeURIComponent(key)}`),
    list: (filter) => {
      let path = `nosql/list?collection=${encodeURIComponent(name)}`;
      if (filter) {
        for (const [k, v] of Object.entries(filter)) {
          path += `&field=${encodeURIComponent(k)}&value=${encodeURIComponent(v)}`;
        }
      }
      return _call("GET", path).then((r) => (Array.isArray(r) ? r : []));
    },
    drop: () => _call("POST", `nosql/drop?collection=${encodeURIComponent(name)}`),
  }),
};

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

function queue(name) {
  return {
    push: (body) => _call("POST", "queue/push", { queue: name, body }),
    pop: () => _call("POST", "queue/pop", { queue: name }),
  };
}

// ---------------------------------------------------------------------------
// Blob
// ---------------------------------------------------------------------------

function _splitBucketKey(name) {
  const i = name.indexOf("/");
  if (i < 0) return ["default", name];
  return [name.slice(0, i), name.slice(i + 1)];
}

const blob = {
  put: async (name, data, contentType) => {
    const [bucket, key] = _splitBucketKey(name);
    const path = `blob/put?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`;
    const bytes = Buffer.isBuffer(data) || data instanceof Uint8Array
      ? data
      : Buffer.from(typeof data === "string" ? data : JSON.stringify(data));
    return _callRaw("POST", path, bytes, contentType);
  },
  get: async (name) => {
    const [bucket, key] = _splitBucketKey(name);
    const path = `blob/get?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`;
    const base = _getBackboneUrl();
    if (!base) return _store.blobs[path] || null;
    const resp = await fetch(`${base}/${path}`);
    if (!resp.ok) {
      const text = (await resp.text()).trim();
      throw new Error(`drift: backbone GET ${path}: HTTP ${resp.status}${text ? `: ${text}` : ""}`);
    }
    return Buffer.from(await resp.arrayBuffer());
  },
};

// ---------------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------------

const lock = {
  acquire: async (name, ttl) => {
    const resp = await _call("POST", "lock/acquire", { name, ttl });
    return (resp && resp.token) || "";
  },
  release: (name, token) => _call("POST", "lock/release", { name, token }),
};

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg) {
  process.stderr.write(String(msg) + "\n");
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

// Default 30-second timeout on outbound calls. A function calling a hung
// remote shouldn't hold an Atomic invocation open longer than this; the
// runner's per-invocation deadline is the absolute ceiling. Pass
// `{timeoutMs: N}` in opts to override.
async function httpRequest(method, url, headers, body, opts) {
  const reqOpts = { method, headers: headers || {} };
  if (body !== undefined && body !== null) {
    reqOpts.body = typeof body === "string" ? body : JSON.stringify(body);
  }
  const timeoutMs = (opts && opts.timeoutMs) || 30000;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  reqOpts.signal = ctrl.signal;
  try {
    const resp = await fetch(url, reqOpts);
    const data = await resp.arrayBuffer();
    return { status: resp.status, body: Buffer.from(data) };
  } finally {
    clearTimeout(t);
  }
}

// Exports collected at the bottom of the file (single canonical
// `module.exports = {...}` block) once every symbol is declared.

// ─── SSE (Server-Sent Events) ────────────────────────────────────────────────

/**
 * Entry point for SSE streaming functions.
 *
 * Usage:
 *   // @atomic http=get:events auth=none stream=sse
 *   const drift = require("@drift/sdk");
 *   drift.runSSE(async (req, emit) => {
 *     for (let i = 0; i < 10; i++) {
 *       emit("counter", { value: i });
 *       await new Promise(r => setTimeout(r, 1000));
 *     }
 *   });
 */
function runSSE(handler) {
  if (process.env.DRIFT_RUNTIME) {
    let data = "";
    process.stdin.on("data", (chunk) => (data += chunk));
    process.stdin.on("end", async () => {
      const req = JSON.parse(data || "{}");
      const emit = (event, payload) => {
        if (event) process.stdout.write(`event: ${event}\n`);
        process.stdout.write(`data: ${JSON.stringify(payload)}\n\n`);
      };
      await handler(req, emit);
    });
    return;
  }
  _runLocalSSE(handler);
}

function _runLocalSSE(handler) {
  const http = require("http");
  const port = parseInt(process.env.PORT || "8080", 10);

  const server = http.createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;

    let parsed = null;
    if (body) {
      try {
        parsed = JSON.parse(body);
      } catch {
        parsed = body;
      }
    }

    const url = new URL(req.url, `http://localhost:${port}`);
    const headers = {};
    for (const [k, v] of Object.entries(req.headers)) {
      headers[k] = Array.isArray(v) ? v[0] : v;
    }

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });

    const funcReq = {
      path:url.pathname,
      headers,
      query: url.search ? url.search.slice(1) : "",
      body: parsed,
    };

    const emit = (event, payload) => {
      if (event) res.write(`event: ${event}\n`);
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    let closed = false;
    req.on("close", () => {
      closed = true;
    });

    try {
      await handler(funcReq, emit);
    } catch (err) {
      if (!closed) {
        res.write(`event: error\ndata: ${JSON.stringify({ error: String(err) })}\n\n`);
      }
    }
    if (!closed) res.end();
  });

  server.listen(port, () => {
    process.stderr.write(`drift-sdk: local SSE server starting on :${port}\n`);
  });
}

// ─── WebSocket ───────────────────────────────────────────────────────────────

/**
 * Entry point for WebSocket functions.
 *
 * Usage:
 *   // @atomic http=get:chat auth=none stream=ws
 *   const drift = require("@drift/sdk");
 *   drift.runWS(async (req, conn) => {
 *     while (true) {
 *       const msg = await conn.read();
 *       if (msg === null) break;
 *       conn.write({ echo: msg });
 *     }
 *   });
 */
function runWS(handler) {
  if (process.env.DRIFT_RUNTIME) {
    const readline = require("readline");
    const rl = readline.createInterface({ input: process.stdin });
    const lines = [];
    let resolve = null;

    rl.on("line", (line) => {
      if (resolve) {
        const r = resolve;
        resolve = null;
        r(line);
      } else {
        lines.push(line);
      }
    });
    rl.on("close", () => {
      if (resolve) {
        const r = resolve;
        resolve = null;
        r(null);
      }
    });

    const readLine = () =>
      new Promise((r) => {
        if (lines.length > 0) r(lines.shift());
        else resolve = r;
      });

    // First line is the initial request.
    readLine().then(async (firstLine) => {
      const req = firstLine ? JSON.parse(firstLine) : {};
      const conn = {
        read: async () => {
          const line = await readLine();
          if (line === null) return null;
          try {
            return JSON.parse(line);
          } catch {
            return line;
          }
        },
        write: (data) => {
          process.stdout.write(JSON.stringify(data) + "\n");
        },
        writeRaw: (msg) => {
          process.stdout.write(msg + "\n");
        },
      };
      await handler(req, conn);
    });
  }
}

// ---------------------------------------------------------------------------
// Exports — single canonical assignment. Every public symbol declared
// above lives in this block. If you add a new public symbol, add it
// here too.
// ---------------------------------------------------------------------------

// ─── SQL ────────────────────────────────────────────────────────────────────
//
// Per-slice SQLite databases addressed by name. Wire shape: one JSON envelope
// per call ({db, sql, args, tx?}). See docs/memos/backbone-sql.md.
//
//   const db = drift.sql('clinic');
//   const rows = await db.query('SELECT * FROM appointments WHERE slot >= ?', [from]);
//   await db.execute('INSERT INTO appointments(...) VALUES(?, ?)', ['alice', '10:00']);
//   await db.transaction(async (tx) => {
//     await tx.execute('UPDATE appointments SET status=? WHERE id=?', ['confirmed', 7]);
//   });
//

function _sqlRows(resp) {
  const cols = (resp && resp.columns) || [];
  const rows = (resp && resp.rows) || [];
  return rows.map(r => {
    const o = {};
    for (let i = 0; i < cols.length; i++) o[cols[i]] = r[i];
    return o;
  });
}

function sql(name) {
  return {
    async query(sqlText, args = []) {
      const resp = await _backboneRequest('POST', '/sql/query',
        Buffer.from(JSON.stringify({ db: name, sql: sqlText, args })), 'application/json');
      return _sqlRows(JSON.parse(resp.toString('utf8') || '{}'));
    },
    async execute(sqlText, args = []) {
      const resp = await _backboneRequest('POST', '/sql/execute',
        Buffer.from(JSON.stringify({ db: name, sql: sqlText, args })), 'application/json');
      return JSON.parse(resp.toString('utf8') || '{}');
    },
    async begin() {
      const resp = await _backboneRequest('POST', '/sql/begin',
        Buffer.from(JSON.stringify({ db: name })), 'application/json');
      const { tx } = JSON.parse(resp.toString('utf8') || '{}');
      return _sqlTx(name, tx);
    },
    async transaction(fn) {
      const tx = await this.begin();
      try {
        const out = await fn(tx);
        await tx.commit();
        return out;
      } catch (e) {
        try { await tx.rollback(); } catch (_) { /* ignore */ }
        throw e;
      }
    },
  };
}

function _sqlTx(db, token) {
  return {
    async query(sqlText, args = []) {
      const resp = await _backboneRequest('POST', '/sql/query',
        Buffer.from(JSON.stringify({ db, sql: sqlText, args, tx: token })), 'application/json');
      return _sqlRows(JSON.parse(resp.toString('utf8') || '{}'));
    },
    async execute(sqlText, args = []) {
      const resp = await _backboneRequest('POST', '/sql/execute',
        Buffer.from(JSON.stringify({ db, sql: sqlText, args, tx: token })), 'application/json');
      return JSON.parse(resp.toString('utf8') || '{}');
    },
    async commit() {
      await _backboneRequest('POST', '/sql/commit',
        Buffer.from(JSON.stringify({ tx: token })), 'application/json');
    },
    async rollback() {
      await _backboneRequest('POST', '/sql/rollback',
        Buffer.from(JSON.stringify({ tx: token })), 'application/json');
    },
  };
}

// ─── Realtime (backbone.realtime) ─────────────────────────────────────────────
//
// Server-originated pub/sub. Subscribers connect over WebSocket at the Canvas
// route /realtime/<name>; a function fans a message out to all of them with
// backbone.realtime.channel(name).publish(msg).
const realtime = {
  channel: (name) => ({
    publish: async (msg) => {
      const resp = await _call("POST", "realtime/publish", { channel: name, message: msg });
      return (resp && resp.recipients) || 0;
    },
    presence: async () => {
      const resp = await _call("GET", `realtime/presence?channel=${encodeURIComponent(name)}`);
      return (resp && resp.present) || 0;
    },
  }),
};

// ================================================================
// Deed — identity, verified. The fourth pillar alongside Atomic /
// Backbone / Canvas — a peer subsystem, not a Backbone primitive, with its
// own loopback listener (DEED_URL) separate from Backbone's
// (BACKBONE_URL).
//
//   deed.keyauth / jwt / vault / link / pocket
//
// KeyAuth: passwordless Ed25519 device-key auth. JWT: general-purpose
// HS256 sign/verify (KeyAuth mints its own tokens through it). Vault: an
// account-key-wrapped keyring. Link: multi-device attestation /
// enrollment / revocation. Pocket: E2EE per-identity app data, JWT-gated.
//
// Not to be confused with cross-slice calling (slice(name) / callerSlice,
// further below) — that's inter-slice networking, a different, still-
// hypothetical future pillar. deed.link enrolls another DEVICE for the
// same identity; it has nothing to do with calling another SLICE.
// ================================================================

// ─── KeyAuth (Deed.keyauth) ────────────────────────────────────────────────────
//
// Passwordless Ed25519 device-key auth. uid = the public key. challenge mints a
// one-time nonce; the client signs the canonical {domain,nonce,pubkey}; verify
// checks the signature and returns THIS slice's session JWT (sub = pubkey). The
// client half (keygen + recovery phrase) is the @ondrift/keyauth library.
const keyauth = {
  challenge: async (pubkey) => {
    const resp = await _callChecked("POST", "keyauth/challenge", { pubkey });
    return (resp && resp.nonce) || "";
  },
  verify: async (pubkey, sig, domain) => {
    const resp = await _callChecked("POST", "keyauth/verify", { pubkey, sig, domain });
    return (resp && resp.token) || "";
  },
};

// ─── JWT (Deed.jwt) ─────────────────────────────────────────────────────────────
//
// HS256 minting + verification, signed with the slice's per-slice JKey. The
// signing key never leaves the slice's backbone process; all operations flow
// through loopback HTTP to backbone /jwt/{sign,verify,slice-id}.
//
// Design: internal/todo/slice-jwt-primitive.md.

class JWTError extends Error {
  constructor(reason) {
    super(`jwt verify: ${reason}`);
    this.name = "JWTError";
    this.reason = reason;
  }
}

const jwt = {
  /**
   * Sign a JWT with the slice's HS256 JKey.
   *
   * `claims.exp` is required. `iat`, `iss`, and `jti` are auto-set when
   * unset. `claims.custom` is a plain object of app-specific claims that
   * the platform never inspects.
   */
  async issue(claims = {}) {
    const body = {};
    for (const k of ["sub", "iat", "exp", "nbf", "iss", "aud", "jti", "custom"]) {
      if (claims[k] !== undefined && claims[k] !== null) body[k] = claims[k];
    }
    const resp = await _callChecked("POST", "jwt/sign", body);
    return resp && resp.token ? resp.token : null;
  },

  /**
   * Validate a token. Resolves with the parsed claims object on success;
   * throws JWTError on validation failure with a stable wire `reason`.
   */
  async verify(token, opts = {}) {
    const body = { token };
    if (opts.audience) body.audience = opts.audience;
    if (opts.allowedIssuer) body.allowed_issuer = opts.allowedIssuer;
    const resp = await _callChecked("POST", "jwt/verify", body);
    if (!resp || typeof resp !== "object") throw new JWTError("internal_error");
    if (!resp.valid) throw new JWTError(resp.reason || "internal_error");
    return resp.claims || {};
  },

  /** The slice's auto-set issuer string ("drift-slice-<user>-<slice>"). */
  async sliceId() {
    const resp = await _callChecked("GET", "jwt/slice-id");
    return resp && resp.slice_id ? resp.slice_id : "";
  },
};

// ─── Vault (Deed.vault) ─────────────────────────────────────────────────────────
//
// Zero-knowledge recovery store: opaque, user-scoped, append-only. The client
// encrypts the blob under a key derived from its recovery phrase (which the
// slice NEVER sees), so Drift stores the backup but cannot read it. Backed by
// Deed's own dedicated routes (deed/vault/*) — replaces the old generic-NoSQL-
// collection implementation entirely; no Driftfile declaration needed.
const vault = {
  /** Appends an opaque encrypted backup blob for uid (append-only — a new version each call). */
  put: (uid, blob) => _callChecked("POST", "deed/vault/put", { uid, blob }),

  /**
   * Returns the most recent backup blob for uid. Throws if uid has never
   * written one — same "missing is an error, not a silent null" convention
   * as Pocket.get.
   */
  get: async (uid) => {
    const resp = await _callChecked("GET", `deed/vault/get?uid=${encodeURIComponent(uid)}`);
    if (!resp || typeof resp.blob === "undefined") {
      throw new Error("drift: vault.get: no vault entry for uid");
    }
    return resp.blob;
  },
};

// ─── Link (Deed.link) ───────────────────────────────────────────────────────────
//
// Multi-device continuity: generalizes the enroll/attest/revoke pattern so an
// identity's KeyAuth session can move to a second, third, ... device. The
// signature parameters below (sig, attestingPubkey, ...) are produced entirely
// client-side — this SDK only forwards them, the same way keyauth.verify
// forwards a signature it never computes itself. The one rule the whole design
// rests on: Deed verifies, it never decides — a device is only ever added on
// the strength of a signature from a device already active in the identity's
// registry.
//
// Not to be confused with cross-slice calling (slice(name)/callerSlice, further
// below) — this Link enrolls a DEVICE for one identity, it does not call
// another slice.
const link = {
  /**
   * Starts a device-linking session for a not-yet-enrolled device's pubkey
   * (usually carried in a QR code alongside the pubkey). Returns a session ID
   * for an already-active device to present to attest.
   */
  begin: async (pubkey) => {
    const resp = await _callChecked("POST", "deed/link/begin", { pubkey });
    return (resp && resp.session_id) || "";
  },

  /**
   * Has an already-active device vouch for the session's pending device. sig
   * is the client's signature over the canonical {domain,identity,new_pubkey}
   * message — computed client-side, never by this SDK.
   */
  attest: (identity, sessionId, attestingPubkey, sig) =>
    _callChecked("POST", "deed/link/attest", {
      identity, session_id: sessionId, attesting_pubkey: attestingPubkey, sig,
    }),

  /**
   * Polls a session the new device started with begin, returning whether an
   * active device has attested it yet: `{status, identity?}` — identity is
   * only set once status === "attested".
   */
  complete: async (sessionId) =>
    (await _callChecked("POST", "deed/link/complete", { session_id: sessionId })) || {},

  /**
   * Deactivates targetPubkey in identity's device registry. Any currently-
   * active device may revoke another (or itself); revokingPubkey is the
   * device doing the revoking, sig its signature over the canonical
   * {domain,identity,target_pubkey} message.
   */
  revoke: (identity, targetPubkey, revokingPubkey, sig) =>
    _callChecked("POST", "deed/link/revoke", {
      identity, target_pubkey: targetPubkey, revoking_pubkey: revokingPubkey, sig,
    }),
};

// ─── Pocket (Deed.pocket) ────────────────────────────────────────────────────────
//
// An app's actual data — E2EE, content-keyed, following an identity across
// every device Link has enrolled. The crypto work happens entirely
// client-side before anything reaches this primitive; Pocket never encrypts or
// decrypts the payload itself. Every call takes `token` explicitly (the JWT
// keyauth.verify returned) rather than holding hidden session state —
// matching the rest of this SDK's stateless posture inside an Atomic function
// invocation. The token's sub is the only identity a call can read or write
// under; there is no way to name a different one.
const pocket = {
  /** Stores blob under key for whichever identity token resolves to. */
  set: (token, key, blob) => _callAuth("POST", "deed/pocket/set", token, { key, blob }),

  /** Returns the blob stored under key for token's identity. Throws if no such key exists. */
  get: async (token, key) => {
    const resp = await _callAuth("GET", `deed/pocket/get?key=${encodeURIComponent(key)}`, token);
    if (!resp || typeof resp.blob === "undefined") {
      throw new Error("drift: pocket.get: no such key");
    }
    return resp.blob;
  },

  /** Removes key for token's identity. Throws if no such key exists. */
  delete: (token, key) => _callAuth("POST", "deed/pocket/delete", token, { key }),

  /** Returns every key stored under token's identity — never another identity's, even by guessing. */
  list: async (token) => {
    const resp = await _callAuth("GET", "deed/pocket/list", token);
    return Array.isArray(resp) ? resp : [];
  },
};

// ─── Deed — the fourth pillar, a peer of Backbone ───────────────────────────────
//
// The single entrypoint for every identity primitive. See the module header
// comment above for the full rationale and the slice(name) disambiguation.
const deed = { keyauth, jwt, vault, link, pocket };

// ─── Backbone — the B of the sacred A·B·C triad ───────────────────────────────
//
// The single entrypoint for every stateful primitive. Nothing stateful lives at
// the top level; the triad is the namespace for everything under it. (Identity
// — keyauth/jwt/vault/link/pocket — lives under `deed`, above, a peer of
// Backbone rather than one of its primitives.)
const backbone = { secret, cache, nosql, queue, blob, lock, sql, realtime };

// ─── Slice-to-slice linking (top-level; inter-slice networking — a different,
// still-hypothetical future pillar, NOT the same as deed.link device
// enrollment above) ─────────────────────────────────────────────────────────

function _linkEnvName(name) {
  return "DRIFT_LINK_" + name.toUpperCase().replace(/[^A-Z0-9]/g, "_") + "_URL";
}

// slice(name) returns a client for a slice you're LINKED to (drift slice link).
// The call travels in-cluster and carries this slice's identity (X-Drift-Slice).
function slice(name) {
  const resolveURL = (path) => {
    const baseUrl = process.env[_linkEnvName(name)];
    if (!baseUrl) throw new Error(`drift: not linked to slice "${name}" — run \`drift slice link add ${name}\``);
    return baseUrl.replace(/\/+$/, "") + "/" + String(path).replace(/^\/+/, "");
  };
  const request = (method, path, headers, body) =>
    httpRequest(method, resolveURL(path), Object.assign({ "X-Drift-Slice": process.env.DRIFT_SLICE || "" }, headers || {}), body);
  return {
    request,
    get: (path) => request("GET", path, null, null),
    post: (path, body) => request("POST", path, { "Content-Type": "application/json" }, body),
  };
}

// callerSlice returns the linked slice that called this request, or "" if the
// request didn't arrive over a slice-to-slice link (case-insensitive header).
function callerSlice(req) {
  const h = (req && req.headers) || {};
  for (const k of Object.keys(h)) {
    if (k.toLowerCase() === "x-drift-slice") return h[k];
  }
  return "";
}

// env returns an environment variable value ("" if unset).
function env(key) {
  return process.env[key] || "";
}

// ---------------------------------------------------------------------------
// Exports — single canonical assignment. State primitives live under
// `backbone` (the sacred triad); identity primitives live under `deed` (the
// fourth pillar); Atomic entrypoints, utilities, and cross-slice linking
// (a different, still-hypothetical future pillar) stay top-level.
// ---------------------------------------------------------------------------

module.exports = {
  run, runSSE, runWS,
  log, httpRequest, env,
  backbone,
  deed,
  slice, callerSlice,
  JWTError,
};
