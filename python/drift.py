"""
Drift SDK for Python Atomic functions.

This single-file SDK provides:
  - run(handler): Entry point that dispatches to deployed or local mode.
  - Backbone helpers: secret, cache, nosql, queue, blob, lock.
  - log(msg): Writes to stderr (captured by the runner as function logs).
  - http_request(): Outbound HTTP from within a function.

All backbone helpers use only stdlib (urllib.request) -- zero external dependencies.
"""

import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request  # outbound user HTTP only — backbone uses http.client below
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(handler):
    """Entry point for Drift Atomic functions.

    In deployed mode (DRIFT_RUNTIME is set): reads JSON from stdin,
    calls handler, writes JSON to stdout.

    In local dev mode: starts an HTTP server on PORT (default 8080).
    """
    if os.environ.get("DRIFT_RUNTIME"):
        _run_deployed(handler)
    else:
        _run_local(handler)


def _run_deployed(handler):
    req = json.loads(sys.stdin.read())
    if isinstance(req, dict) and isinstance(req.get("query"), str):
        parsed = {}
        for pair in req["query"].split("&"):
            if not pair:
                continue
            if "=" in pair:
                k, v = pair.split("=", 1)
            else:
                k, v = pair, ""
            parsed[urllib.parse.unquote_plus(k)] = urllib.parse.unquote_plus(v)
        req["query"] = parsed
    resp = handler(req)
    sys.stdout.write(json.dumps(resp))
    sys.stdout.flush()


def _run_local(handler):
    port = int(os.environ.get("PORT", "8080"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self._handle()

        def do_POST(self):
            self._handle()

        def do_PUT(self):
            self._handle()

        def do_DELETE(self):
            self._handle()

        def _handle(self):
            content_length = int(self.headers.get("Content-Length", 0))
            body = None
            if content_length > 0:
                raw = self.rfile.read(content_length)
                try:
                    body = json.loads(raw)
                except json.JSONDecodeError:
                    body = raw.decode("utf-8", errors="replace")

            parsed = urllib.parse.urlparse(self.path)
            headers = {k: self.headers[k] for k in self.headers}

            req = {
                "path":parsed.path,
                "headers": headers,
                "query": parsed.query,
                "body": body,
            }

            resp = handler(req)
            status = resp.get("status", 200)
            out = json.dumps(resp).encode("utf-8")

            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(out)

        def log_message(self, fmt, *args):
            sys.stderr.write(f"drift-sdk: {fmt % args}\n")

    server = HTTPServer(("", port), Handler)
    sys.stderr.write(f"drift-sdk: local server starting on :{port}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


# ---------------------------------------------------------------------------
# Backbone transport
# ---------------------------------------------------------------------------

_backbone_url = None


def _get_backbone_url():
    global _backbone_url
    if _backbone_url is None:
        _backbone_url = os.environ.get("BACKBONE_URL", "")
    return _backbone_url


_deed_url = None


def _get_deed_url():
    global _deed_url
    if _deed_url is None:
        _deed_url = os.environ.get("DEED_URL", "")
    return _deed_url


# Persistent HTTP connection to the backbone.
#
# Why this exists: the previous implementation opened a fresh TCP
# connection for every backbone call (urllib.request.urlopen does not
# pool). On a localhost loopback that is ~0.2ms of avoidable handshake
# per call, which adds up on a hot read-heavy function. Reusing one
# http.client.HTTPConnection across calls halves the per-call
# overhead.
#
# Concurrency: the Python language server is single-threaded
# (slice/atomic/lang_server.go uses HTTPServer, not Threading*), so
# backbone calls inside a single Python process are serialised. The
# lock is paranoia-only against future changes to the lang-server
# threading model.
_conn = None
_conn_target = None  # tuple (host, port) — re-created if BACKBONE_URL changes
_conn_lock = threading.Lock()


def _ensure_conn(base_url):
    """Create or reuse a persistent backbone connection. Returns
    (connection, path_prefix). path_prefix is the URL path component
    of BACKBONE_URL (usually empty).

    BACKBONE_URL is an http://host:port TCP URL.
    """
    global _conn, _conn_target
    parsed = urllib.parse.urlparse(base_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 80
    target = (host, port)
    if _conn is None or _conn_target != target:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
        _conn = http.client.HTTPConnection(host, port, timeout=30)
        _conn_target = target
    return _conn, parsed.path or ""


# Deed gets its own persistent connection, separate from Backbone's — it
# lives on its own port (DEED_URL) now, and sharing one connection between
# the two would thrash _ensure_conn's target check on every call that
# alternates between them (e.g. a handler that reads a Secret then calls
# Deed.Vault.Get).
_deed_conn = None
_deed_conn_target = None
_deed_conn_lock = threading.Lock()


def _ensure_deed_conn(base_url):
    """Create or reuse a persistent Deed connection — same pattern as
    _ensure_conn, kept separate since Deed and Backbone are different ports."""
    global _deed_conn, _deed_conn_target
    parsed = urllib.parse.urlparse(base_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 80
    target = (host, port)
    if _deed_conn is None or _deed_conn_target != target:
        if _deed_conn is not None:
            try:
                _deed_conn.close()
            except Exception:
                pass
        _deed_conn = http.client.HTTPConnection(host, port, timeout=30)
        _deed_conn_target = target
    return _deed_conn, parsed.path or ""


def _call(method, path, body=None):
    """Call backbone via HTTP (deployed) or return None (local dev fallback).

    Uses a persistent HTTP connection — see _ensure_conn for rationale.
    On connection-reset / remote-disconnected the call retries once
    against a fresh connection before propagating the error.
    """
    global _conn
    base = _get_backbone_url()
    if not base:
        return _call_local(method, path, body)

    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    with _conn_lock:
        for attempt in range(2):
            conn, prefix = _ensure_conn(base)
            try:
                conn.request(method, f"{prefix}/{path}", body=data, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                if 200 <= resp.status < 300:
                    if not raw:
                        return None
                    try:
                        return json.loads(raw)
                    except (json.JSONDecodeError, ValueError):
                        return raw.decode("utf-8", errors="replace")
                # Non-2xx: raise the same exception type the previous
                # urllib-based code raised, so callers that catch
                # HTTPError keep working.
                raise urllib.error.HTTPError(
                    f"{base}/{path}", resp.status, resp.reason or "", resp.getheaders(), None
                )
            except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
                # The persistent connection died — close + retry once
                # with a fresh connection. After the retry, propagate.
                try:
                    conn.close()
                except Exception:
                    pass
                _conn = None
                if attempt == 0:
                    continue
                raise


# In-memory backbone for local dev (matches Go SDK behavior).
_local_store = {
    "nosql": {},
    "cache": {},
    "queues": {},
    "blobs": {},
    "locks": {},
    "next_id": 0,
}


def _call_local(method, path, body=None):
    """In-memory backbone for local development."""
    s = _local_store
    base_path = path.split("?")[0]
    query = {}
    if "?" in path:
        query = dict(urllib.parse.parse_qsl(path.split("?", 1)[1]))

    # NoSQL
    if base_path == "write" and method == "POST":
        col = (body or {}).get("collection", "default")
        if col not in s["nosql"]:
            s["nosql"][col] = {}
        s["next_id"] += 1
        key = str(s["next_id"])
        s["nosql"][col][key] = body
        return {"key": key}

    if base_path == "read" and method == "GET":
        col = query.get("collection", "default")
        key = query.get("key", "")
        return s["nosql"].get(col, {}).get(key)

    if base_path == "nosql/list" and method == "GET":
        col = query.get("collection", "default")
        docs = s["nosql"].get(col, {})
        field = query.get("field")
        value = query.get("value")
        results = []
        for doc in docs.values():
            if field and str(doc.get(field)) != value:
                continue
            results.append(doc)
        return results

    if base_path == "nosql/drop" and method == "POST":
        col = query.get("collection", "default")
        s["nosql"].pop(col, None)
        return None

    # Cache
    if base_path == "cache/set" and method == "POST":
        s["cache"][(body or {}).get("key", "")] = (body or {}).get("value")
        return None

    if base_path == "cache/get" and method == "GET":
        return s["cache"].get(query.get("key", ""))

    if base_path == "cache/del":
        s["cache"].pop(query.get("key", ""), None)
        return None

    # Queue
    if base_path == "queue/push" and method == "POST":
        name = (body or {}).get("queue", "")
        msg = (body or {}).get("body")
        s["queues"].setdefault(name, []).append(msg)
        return None

    if base_path == "queue/pop" and method == "POST":
        name = (body or {}).get("queue", "")
        q = s["queues"].get(name, [])
        if not q:
            return None
        return q.pop(0)

    # Blob
    if base_path == "blob/put" and method == "POST":
        s["blobs"][(body or {}).get("name", "")] = (body or {}).get("data")
        return None

    if base_path == "blob/get" and method == "GET":
        return s["blobs"].get(query.get("name", ""))

    # Secret — in local dev, read from environment variables (loaded from .env by the CLI)
    if base_path == "secret/get" and method == "GET":
        name = query.get("name", "")
        return os.environ.get(name)

    # Lock
    if base_path == "lock/acquire" and method == "POST":
        name = (body or {}).get("name", "")
        if name in s["locks"]:
            return None
        s["next_id"] += 1
        token = f"local-lock-{s['next_id']}"
        s["locks"][name] = token
        return {"token": token}

    if base_path == "lock/release" and method == "POST":
        s["locks"].pop((body or {}).get("name", ""), None)
        return None

    return None


# ---------------------------------------------------------------------------
# Secret
# ---------------------------------------------------------------------------

class _SecretNS:
    def get(self, name):
        # The runner injects declared secrets as DRIFT_SECRET_<NAME> env vars
        # at subprocess start. Read from env first; the HTTP fallback exists
        # only for local dev — backbone /secret/get is SAT-guarded in
        # production, so undeclared HTTP calls fail.
        env_val = os.environ.get("DRIFT_SECRET_" + name.upper())
        if env_val is not None:
            return env_val
        resp = _call("GET", f"secret/get?name={urllib.parse.quote(name)}")
        return resp if isinstance(resp, str) else (json.dumps(resp) if resp else "")

    def set(self, name, value):
        _call("POST", "secret/set", {"name": name, "value": value})

    def delete(self, name):
        _call("DELETE", f"secret/delete?name={urllib.parse.quote(name)}")

secret = _SecretNS()


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class _CacheNS:
    def get(self, key):
        return _call("GET", f"cache/get?key={urllib.parse.quote(key)}")

    def set(self, key, value, ttl):
        payload = {"key": key, "value": value}
        if ttl > 0:
            payload["ttl"] = ttl
        _call("POST", "cache/set", payload)

    def delete(self, key):
        _call("DELETE", f"cache/del?key={urllib.parse.quote(key)}")

cache = _CacheNS()


# ---------------------------------------------------------------------------
# NoSQL
# ---------------------------------------------------------------------------

class _NoSQLNS:
    def collection(self, name):
        return _CollectionHandle(name)

class _CollectionHandle:
    def __init__(self, name):
        self.name = name

    def insert(self, doc):
        payload = {"collection": self.name}
        if isinstance(doc, dict):
            payload.update(doc)
        else:
            payload["data"] = doc
        resp = _call("POST", "write", payload)
        if isinstance(resp, dict):
            return resp.get("key", "")
        return ""

    def read(self, key):
        return _call("GET", f"read?collection={urllib.parse.quote(self.name)}&key={urllib.parse.quote(key)}")

    def get(self, id):
        """Find the row whose user-facing `_id` matches via the platform's
        indexed lookup."""
        path = (f"nosql/list?collection={urllib.parse.quote(self.name)}"
                f"&field=_id&value={urllib.parse.quote(id)}")
        rows = _call("GET", path)
        rows = rows if isinstance(rows, list) else []
        return rows[0] if rows else None

    def delete(self, key):
        return _call("POST", f"nosql/delete?collection={urllib.parse.quote(self.name)}&key={urllib.parse.quote(key)}")

    def list(self, filter=None):
        path = f"nosql/list?collection={urllib.parse.quote(self.name)}"
        if filter:
            for k, v in filter.items():
                path += f"&field={urllib.parse.quote(k)}&value={urllib.parse.quote(v)}"
        resp = _call("GET", path)
        return resp if isinstance(resp, list) else []

    def drop(self):
        _call("POST", f"nosql/drop?collection={urllib.parse.quote(self.name)}")

nosql = _NoSQLNS()


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------

class _QueueHandle:
    def __init__(self, name):
        self.name = name

    def push(self, body):
        _call("POST", "queue/push", {"queue": self.name, "body": body})

    def pop(self):
        return _call("POST", "queue/pop", {"queue": self.name})

def queue(name):
    return _QueueHandle(name)


# ---------------------------------------------------------------------------
# Blob
# ---------------------------------------------------------------------------

class _BlobNS:
    def put(self, name, data, content_type=None):
        if "/" in name:
            bucket, key = name.split("/", 1)
        else:
            bucket, key = "default", name
        path = f"blob/put?bucket={urllib.parse.quote(bucket)}&key={urllib.parse.quote(key)}"
        _call_raw("POST", path, data if isinstance(data, (bytes, bytearray)) else str(data).encode("utf-8"),
                  content_type=content_type or "application/octet-stream")

    def get(self, name):
        if "/" in name:
            bucket, key = name.split("/", 1)
        else:
            bucket, key = "default", name
        return _call("GET", f"blob/get?bucket={urllib.parse.quote(bucket)}&key={urllib.parse.quote(key)}")


def _call_raw(method, path, data_bytes, content_type="application/octet-stream"):
    """Backbone call with raw byte body / response — used by Blob.put / Blob.get.

    Same connection-reuse + one-shot retry shape as _call. The body is
    bytes (no JSON envelope), the response is bytes (no JSON parse).
    """
    global _conn
    base = _get_backbone_url()
    if not base:
        _local_store["blobs"][path] = data_bytes
        return None

    headers = {"Content-Type": content_type}
    with _conn_lock:
        for attempt in range(2):
            conn, prefix = _ensure_conn(base)
            try:
                conn.request(method, f"{prefix}/{path}", body=data_bytes, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                if 200 <= resp.status < 300:
                    return raw if raw else None
                raise urllib.error.HTTPError(
                    f"{base}/{path}", resp.status, resp.reason or "", resp.getheaders(), None
                )
            except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
                try:
                    conn.close()
                except Exception:
                    pass
                _conn = None
                if attempt == 0:
                    continue
                raise

blob = _BlobNS()


# ---------------------------------------------------------------------------
# Lock
# ---------------------------------------------------------------------------

class _LockNS:
    def acquire(self, name, ttl):
        resp = _call("POST", "lock/acquire", {"name": name, "ttl": ttl})
        return (resp or {}).get("token", "")

    def release(self, name, token):
        _call("POST", "lock/release", {"name": name, "token": token})

lock = _LockNS()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(msg):
    """Write a log message to stderr (captured by the runner)."""
    sys.stderr.write(str(msg) + "\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

def http_request(method, url, headers=None, body=None, timeout=30):
    """Make an outbound HTTP request. Returns (status, body_bytes).

    Default timeout is 30 seconds. A function calling a hung remote
    shouldn't hold an Atomic invocation open longer than this; the
    runner's per-invocation deadline is the absolute ceiling.
    """
    data = body if isinstance(body, bytes) else (body.encode("utf-8") if body else None)
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()

# ─── SSE (Server-Sent Events) ────────────────────────────────────────────────

def run_sse(handler):
    """Entry point for SSE streaming functions.

    Usage:
        # @atomic http=get:events auth=none stream=sse
        import drift

        def get_events(req, emit):
            for i in range(10):
                emit("counter", {"value": i})
                time.sleep(1)

        drift.run_sse(get_events)
    """
    if os.environ.get("DRIFT_RUNTIME"):
        req = json.loads(sys.stdin.read())
        def emit(event, data):
            if event:
                sys.stdout.write(f"event: {event}\n")
            sys.stdout.write(f"data: {json.dumps(data)}\n\n")
            sys.stdout.flush()
        handler(req, emit)
        return

    # Local dev: serve SSE over HTTP for `drift atomic run`.
    port = int(os.environ.get("PORT", "8080"))

    class _SSEHandler(BaseHTTPRequestHandler):
        def do_GET(self):  self._handle()
        def do_POST(self): self._handle()

        def _handle(self):
            content_length = int(self.headers.get("Content-Length", 0))
            body = None
            if content_length > 0:
                raw = self.rfile.read(content_length)
                try:
                    body = json.loads(raw)
                except json.JSONDecodeError:
                    body = raw.decode("utf-8", errors="replace")
            parsed = urllib.parse.urlparse(self.path)
            req = {
                "path":parsed.path,
                "headers": {k: self.headers[k] for k in self.headers},
                "query": parsed.query,
                "body": body,
            }

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            def emit(event, data):
                if event:
                    self.wfile.write(f"event: {event}\n".encode("utf-8"))
                self.wfile.write(f"data: {json.dumps(data)}\n\n".encode("utf-8"))
                self.wfile.flush()

            handler(req, emit)

        def log_message(self, fmt, *args):
            sys.stderr.write(f"drift-sdk: {fmt % args}\n")

    server = HTTPServer(("", port), _SSEHandler)
    sys.stderr.write(f"drift-sdk: local SSE server starting on :{port}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


# ─── WebSocket ───────────────────────────────────────────────────────────────

class WsConn:
    """WebSocket connection bridged through stdin/stdout."""

    def read(self):
        """Read the next message from the client. Returns None on disconnect."""
        line = sys.stdin.readline()
        if not line:
            return None
        line = line.strip()
        if not line:
            return None
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return line

    def write(self, data):
        """Send a message to the client (JSON-encoded)."""
        sys.stdout.write(json.dumps(data) + "\n")
        sys.stdout.flush()

    def write_raw(self, msg):
        """Send a raw string message to the client."""
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()


def run_ws(handler):
    """Entry point for WebSocket functions.

    Usage:
        # @atomic http=get:chat auth=none stream=ws
        import drift

        def get_chat(req, conn):
            while True:
                msg = conn.read()
                if msg is None:
                    break
                conn.write({"echo": msg})

        drift.run_ws(get_chat)
    """
    if os.environ.get("DRIFT_RUNTIME"):
        # First stdin line is the initial request.
        first_line = sys.stdin.readline()
        req = json.loads(first_line) if first_line else {}
        conn = WsConn()
        handler(req, conn)
    else:
        _run_local(lambda req: handler(req, WsConn()))


# ─── SQL ──────────────────────────────────────────────────────────────────────
#
# Per-slice SQLite databases addressed by name. Wire shape: one JSON envelope
# per call ({db, sql, args, tx?}). See docs/memos/backbone-sql.md.
#
#   db = drift.sql("clinic")
#   rows = db.query("SELECT * FROM appointments WHERE slot >= ?", ["2026-05-01"])
#   res = db.execute("INSERT INTO appointments(...) VALUES(?, ?)", ["alice", "10:00"])
#   with db.transaction() as tx:
#       tx.execute("UPDATE appointments SET status=? WHERE id=?", ["confirmed", 7])
#

class _SQLDB:
    def __init__(self, name):
        self._name = name

    def query(self, sql, args=None):
        body = {"db": self._name, "sql": sql, "args": list(args or [])}
        resp = _call("POST", "sql/query", body) or {}
        cols = resp.get("columns") or []
        rows = resp.get("rows") or []
        return [dict(zip(cols, r)) for r in rows]

    def execute(self, sql, args=None):
        body = {"db": self._name, "sql": sql, "args": list(args or [])}
        return _call("POST", "sql/execute", body) or {}

    def begin(self):
        resp = _call("POST", "sql/begin", {"db": self._name}) or {}
        return _SQLTx(self._name, resp.get("tx", ""))

    def transaction(self):
        return _SQLTxCtx(self)


class _SQLTx:
    def __init__(self, db, token):
        self._db = db
        self._token = token

    def query(self, sql, args=None):
        body = {"db": self._db, "sql": sql, "args": list(args or []), "tx": self._token}
        resp = _call("POST", "sql/query", body) or {}
        cols = resp.get("columns") or []
        rows = resp.get("rows") or []
        return [dict(zip(cols, r)) for r in rows]

    def execute(self, sql, args=None):
        body = {"db": self._db, "sql": sql, "args": list(args or []), "tx": self._token}
        return _call("POST", "sql/execute", body) or {}

    def commit(self):
        _call("POST", "sql/commit", {"tx": self._token})

    def rollback(self):
        _call("POST", "sql/rollback", {"tx": self._token})


class _SQLTxCtx:
    """Context manager: begins a tx, commits on success, rolls back on
    exception. Use with `with db.transaction() as tx:`."""

    def __init__(self, db):
        self._db = db
        self._tx = None

    def __enter__(self):
        self._tx = self._db.begin()
        return self._tx

    def __exit__(self, exc_type, exc, tb):
        if self._tx is None:
            return False
        if exc_type is None:
            self._tx.commit()
        else:
            try:
                self._tx.rollback()
            except Exception:
                pass
        return False


def sql(name):
    """Return a handle to the named SQLite database. The name must
    already be declared in the Driftfile under `slice.backbone.sql[]`."""
    return _SQLDB(name)


# Capitalised alias for parity with other Drift SDKs that expose
# `drift.SQL("name")` as the canonical entry point.
SQL = sql


# ---------------------------------------------------------------------------
# Realtime — backbone primitive (parity with the Go SDK)
# ---------------------------------------------------------------------------

class _RealtimeChannel:
    """A realtime channel handle. Subscribers connect over WebSocket at the
    Canvas route /realtime/<name>; publish() fans a message out to all of them."""

    def __init__(self, name):
        self.name = name

    def publish(self, msg):
        resp = _call("POST", "realtime/publish", {"channel": self.name, "message": msg})
        return resp.get("recipients", 0) if isinstance(resp, dict) else 0

    def presence(self):
        resp = _call("GET", f"realtime/presence?channel={urllib.parse.quote(self.name)}")
        return resp.get("present", 0) if isinstance(resp, dict) else 0


class _RealtimeNS:
    def channel(self, name):
        return _RealtimeChannel(name)


# ---------------------------------------------------------------------------
# Backbone — the B of the sacred A·B·C triad; sole entrypoint for state.
#   drift.backbone.{secret,cache,nosql,queue,blob,lock,sql,realtime}
# ---------------------------------------------------------------------------

class _Backbone:
    def __init__(self):
        self.secret = _SecretNS()
        self.cache = _CacheNS()
        self.nosql = _NoSQLNS()
        self.blob = _BlobNS()
        self.lock = _LockNS()
        self.realtime = _RealtimeNS()

    def queue(self, name):
        return _QueueHandle(name)

    def sql(self, name):
        return _SQLDB(name)


backbone = _Backbone()


# ---------------------------------------------------------------------------
# Deed — identity, verified. The fourth pillar alongside Atomic / Backbone /
# Canvas — a peer subsystem, not a Backbone primitive, with its own loopback
# listener (DEED_URL) separate from Backbone's (BACKBONE_URL). See
# docs/memos/cyberpunk-shit/deed-the-fourth-pillar-drift-identity.md.
#
#   drift.deed.{keyauth,jwt,vault,link,pocket}
#
# keyauth: passwordless Ed25519 device-key auth. jwt: general-purpose HS256
# sign/verify (keyauth mints its own tokens through it). vault: an
# account-key-wrapped keyring. link: multi-device attestation / enrollment /
# revocation. pocket: E2EE per-identity app data, JWT-gated.
#
# Not to be confused with cross-slice calling (drift.slice(name) /
# caller_slice, further below) — that's inter-slice networking, a different,
# still-hypothetical future pillar. deed.link enrolls another DEVICE for the
# same identity; it has nothing to do with calling another SLICE.
# ---------------------------------------------------------------------------


class _KeyAuthNS:
    """Passwordless Ed25519 device-key auth (Deed.KeyAuth). uid = the public
    key — no accounts, no passwords, no email. challenge mints a one-time
    nonce; the client signs the canonical {domain,nonce,pubkey}; verify
    checks the signature and issues this slice's session JWT (sub = pubkey).
    The client half (keygen + signing + recovery-phrase derivation) is a
    small browser library; see the @ondrift/keyauth memo."""

    def challenge(self, pubkey):
        """Return a one-time login nonce for the given Ed25519 public key
        (32-byte hex). Single-use, short-TTL, cache-backed in the slice."""
        resp = _call_deed("POST", "keyauth/challenge", {"pubkey": pubkey})
        return resp.get("nonce", "") if isinstance(resp, dict) else ""

    def verify(self, pubkey, sig, domain):
        """Check the client's signature over the canonical
        {domain,nonce,pubkey} and, on success, return this slice's session
        JWT (sub = the pubkey). ``domain`` namespaces the signature to your
        app (e.g. "myapp-auth-v1") so a signature for one app/slice can't be
        replayed at another — the client must sign the same domain."""
        resp = _call_deed("POST", "keyauth/verify", {"pubkey": pubkey, "sig": sig, "domain": domain})
        return resp.get("token", "") if isinstance(resp, dict) else ""


class JWTError(Exception):
    """Raised by deed.jwt.verify on validation failure. ``reason`` is one of
    the stable wire strings: ``malformed``, ``bad_signature``, ``expired``,
    ``not_yet_valid``, ``wrong_algorithm``, ``wrong_issuer``,
    ``wrong_audience``, ``invalid_claims``, ``missing_exp``,
    ``internal_error``.
    """

    def __init__(self, reason):
        super().__init__(f"jwt verify: {reason}")
        self.reason = reason


class _JWTNS:
    """HS256 JWT minting + verification (Deed.JWT), signed with the slice's
    per-slice JKey. The signing key never leaves the slice's backbone
    process; all operations flow through loopback HTTP to
    backbone /jwt/{sign,verify,slice-id}. General-purpose on its own, but
    squarely part of Deed — keyauth mints its tokens through it, and pocket
    verifies them.

    Design: internal/todo/slice-jwt-primitive.md.
    """

    def issue(
        self,
        sub=None,
        exp=None,
        iat=None,
        nbf=None,
        iss=None,
        aud=None,
        jti=None,
        custom=None,
    ):
        """Sign a JWT with the slice's HS256 JKey.

        ``exp`` is required. ``iat``, ``iss``, and ``jti`` are auto-set when
        unset. ``custom`` is a dict of app-specific claims that the platform
        never inspects.
        """
        body = {}
        if sub is not None:
            body["sub"] = sub
        if exp is not None:
            body["exp"] = exp
        if iat is not None:
            body["iat"] = iat
        if nbf is not None:
            body["nbf"] = nbf
        if iss is not None:
            body["iss"] = iss
        if aud is not None:
            body["aud"] = aud
        if jti is not None:
            body["jti"] = jti
        if custom is not None:
            body["custom"] = custom
        resp = _call_deed("POST", "jwt/sign", body)
        return resp.get("token") if isinstance(resp, dict) else None

    def verify(self, token, audience=None, allowed_issuer=None):
        """Validate ``token``. Returns the parsed claims dict on success;
        raises ``JWTError`` on any validation failure.
        """
        body = {"token": token}
        if audience:
            body["audience"] = audience
        if allowed_issuer:
            body["allowed_issuer"] = allowed_issuer
        resp = _call_deed("POST", "jwt/verify", body)
        if not isinstance(resp, dict):
            raise JWTError("internal_error")
        if not resp.get("valid"):
            raise JWTError(resp.get("reason") or "internal_error")
        return resp.get("claims") or {}

    def slice_id(self):
        """Return the slice's auto-set issuer string."""
        resp = _call_deed("GET", "jwt/slice-id")
        if isinstance(resp, dict):
            return resp.get("slice_id", "")
        return ""


class _VaultNS:
    """Zero-knowledge recovery store (Deed.Vault): opaque, user-scoped,
    append-only. The client encrypts the blob under a key derived from its
    recovery phrase (which the slice NEVER sees), so Drift stores the
    backup but cannot read it. Scoped to a uid the caller supplies —
    typically the authenticated keyauth pubkey. Backed by Deed's own
    dedicated routes: AES-256-GCM at rest (defense in depth only — the blob
    must already be ciphertext before it arrives, since that's the actual
    source of Vault's confidentiality guarantee) and a per-item size quota.
    No Driftfile declaration needed."""

    def put(self, uid, blob):
        """Append an opaque encrypted backup blob for uid. Append-only (a
        new version each call); get returns the newest."""
        _call_deed("POST", "deed/vault/put", {"uid": uid, "blob": blob})

    def get(self, uid):
        """Return the most recent backup blob for uid. Propagates as an
        error if uid has never written one (same "not found is an error"
        convention as secret.get/blob.get)."""
        resp = _call_deed("GET", f"deed/vault/get?uid={urllib.parse.quote(uid)}")
        return resp.get("blob") if isinstance(resp, dict) else None


class _LinkNS:
    """Multi-device continuity (Deed.Link): generalizes the
    enroll/attest/revoke pattern so an identity's KeyAuth session can move
    to a second, third, ... device. The signature parameters below (sig,
    attesting_pubkey, etc.) are produced entirely client-side — this SDK
    only forwards them, the same way keyauth.verify forwards a signature it
    never computes itself. The one rule the whole design rests on: Deed
    verifies, it never decides — a device is only ever added on the
    strength of a signature from a device already active in the identity's
    registry.

    Not to be confused with cross-slice calling (drift.slice(name) /
    caller_slice, further below) — this Link enrolls a DEVICE for one
    identity, it does not call another slice."""

    def begin(self, pubkey, metadata=None):
        """Start a device-linking session for a not-yet-enrolled device's
        pubkey (usually carried in a QR code alongside the pubkey). Returns
        a session id for an already-active device to present to attest.

        ``metadata`` is an optional opaque string an attesting device can
        read back via ``session_info`` — e.g. an ephemeral key it should
        seal a payload for. Deed never interprets it."""
        body = {"pubkey": pubkey}
        if metadata is not None:
            body["metadata"] = metadata
        resp = _call_deed("POST", "deed/link/begin", body)
        return resp.get("session_id", "") if isinstance(resp, dict) else ""

    def session_info(self, session_id):
        """A read-only, repeatable peek at a pending session — what an
        attesting device (which only ever learns ``session_id``, from a
        scanned/typed code) uses to learn ``new_pubkey`` (``attest``'s
        message is verified server-side against the session's own stored
        value, never the request body, so the attester has to reconstruct
        it exactly) and whatever opaque metadata the joining device passed
        to ``begin``. Returns {"new_pubkey": ..., "metadata": ...}."""
        resp = _call_deed("POST", "deed/link/session", {"session_id": session_id})
        return resp if isinstance(resp, dict) else {}

    def qr(self, text):
        """Render ``text`` (in practice, a Link session id) as a scannable
        QR code, returning inline SVG markup. Pure rendering — no session
        or identity involvement, so it works for any short string."""
        resp = _call_deed("POST", "deed/link/qr", {"text": text})
        return resp.get("svg", "") if isinstance(resp, dict) else ""

    def attest(self, identity, session_id, attesting_pubkey, sig, sealed=None):
        """Have an already-active device vouch for the session's pending
        device. ``sig`` is the client's signature over the canonical
        {domain,identity,new_pubkey} message — computed client-side, never
        by this SDK.

        ``sealed`` is an optional opaque string relayed back once
        ``complete`` reports "attested" — e.g. a payload end-to-end-
        encrypted for whatever key the joiner published as ``begin``'s
        metadata. Deed only relays it, never opens it."""
        body = {
            "identity": identity,
            "session_id": session_id,
            "attesting_pubkey": attesting_pubkey,
            "sig": sig,
        }
        if sealed is not None:
            body["sealed"] = sealed
        _call_deed("POST", "deed/link/attest", body)

    def complete(self, session_id):
        """Poll a session the new device started with begin, returning
        whether an active device has attested it yet: {"status": ...,
        "identity": ..., "sealed": ...}. ``identity``/``sealed`` are only
        present once ``status`` == "attested" (``sealed`` only if
        ``attest`` supplied one)."""
        resp = _call_deed("POST", "deed/link/complete", {"session_id": session_id})
        return resp if isinstance(resp, dict) else {}

    def revoke(self, identity, target_pubkey, revoking_pubkey, sig):
        """Deactivate ``target_pubkey`` in ``identity``'s device registry.
        Any currently-active device may revoke another (or itself);
        ``revoking_pubkey`` is the device doing the revoking, ``sig`` its
        signature over the canonical {domain,identity,target_pubkey}
        message."""
        _call_deed("POST", "deed/link/revoke", {
            "identity": identity,
            "target_pubkey": target_pubkey,
            "revoking_pubkey": revoking_pubkey,
            "sig": sig,
        })


def _call_deed(method, path, body=None):
    """Like _call but hits Deed's own listener (DEED_URL) and has no
    local-dev fallback. deed.vault and deed.link have no in-memory
    local-dev implementation the way every Backbone primitive does
    (_call_local has no "deed/..." case), so routing them through plain
    _call would silently no-op a void-returning call like vault.put or
    link.attest in local dev — it would report success without ever
    storing anything, since _call_local returns None for an unmatched
    path. Requiring DEED_URL here makes that failure honest and
    immediate instead, matching keyauth/jwt's existing "not available
    without a running slice" precedent.

    Same connection-reuse + one-shot retry shape as _call, but against
    Deed's own persistent connection (_ensure_deed_conn) — a separate
    port from Backbone's.
    """
    global _deed_conn
    base = _get_deed_url()
    if not base:
        raise RuntimeError(
            "drift: deed requires a running slice (DEED_URL) — not available in local dev"
        )

    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    with _deed_conn_lock:
        for attempt in range(2):
            conn, prefix = _ensure_deed_conn(base)
            try:
                conn.request(method, f"{prefix}/{path}", body=data, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                if 200 <= resp.status < 300:
                    if not raw:
                        return None
                    try:
                        return json.loads(raw)
                    except (json.JSONDecodeError, ValueError):
                        return raw.decode("utf-8", errors="replace")
                raise urllib.error.HTTPError(
                    f"{base}/{path}", resp.status, resp.reason or "", resp.getheaders(), None
                )
            except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
                try:
                    conn.close()
                except Exception:
                    pass
                _deed_conn = None
                if attempt == 0:
                    continue
                raise


def _call_auth(method, path, token, body=None):
    """Like _call_deed but attaches an Authorization: Bearer header — used
    by deed.pocket, whose routes are JWT-gated (unlike every other
    loopback-open Backbone/Deed primitive). Local dev (no DEED_URL) has
    no JWT verification to check a token against, so — same as
    keyauth/jwt already — this call isn't available without a running
    slice.

    Same connection-reuse + one-shot retry shape as _call, against
    Deed's own persistent connection.
    """
    global _deed_conn
    base = _get_deed_url()
    if not base:
        raise RuntimeError(
            "drift: deed pocket requires a running slice (DEED_URL) — not available in local dev"
        )

    data = None
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    with _deed_conn_lock:
        for attempt in range(2):
            conn, prefix = _ensure_deed_conn(base)
            try:
                conn.request(method, f"{prefix}/{path}", body=data, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                if 200 <= resp.status < 300:
                    if not raw:
                        return None
                    try:
                        return json.loads(raw)
                    except (json.JSONDecodeError, ValueError):
                        return raw.decode("utf-8", errors="replace")
                raise urllib.error.HTTPError(
                    f"{base}/{path}", resp.status, resp.reason or "", resp.getheaders(), None
                )
            except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
                try:
                    conn.close()
                except Exception:
                    pass
                _deed_conn = None
                if attempt == 0:
                    continue
                raise


class _PocketNS:
    """An app's actual data (Deed.Pocket) — E2EE, content-keyed, following
    an identity across every device link has enrolled. The crypto work
    happens entirely client-side before anything reaches this primitive;
    Pocket never encrypts or decrypts the payload itself. Every call takes
    ``token`` explicitly (the JWT keyauth.verify returned) rather than
    holding hidden session state — matching the rest of this SDK's
    stateless posture inside an Atomic function invocation. The token's
    ``sub`` is the only identity a call can read or write under; there is
    no way to name a different one."""

    def set(self, token, key, blob):
        """Store blob under key for whichever identity token resolves to."""
        _call_auth("POST", "deed/pocket/set", token, {"key": key, "blob": blob})

    def get(self, token, key):
        """Return the blob stored under key for token's identity.
        Propagates as an error if no such key exists."""
        resp = _call_auth("GET", f"deed/pocket/get?key={urllib.parse.quote(key)}", token)
        return resp.get("blob") if isinstance(resp, dict) else None

    def delete(self, token, key):
        """Remove key for token's identity. Propagates as an error if no
        such key exists."""
        _call_auth("POST", "deed/pocket/delete", token, {"key": key})

    def list(self, token):
        """Return every key stored under token's identity — never another
        identity's, even by guessing."""
        resp = _call_auth("GET", "deed/pocket/list", token)
        return resp if isinstance(resp, list) else []


class _Deed:
    """Deed is the entrypoint for every identity primitive: keyauth, jwt,
    vault, link, pocket."""

    def __init__(self):
        self.keyauth = _KeyAuthNS()
        self.jwt = _JWTNS()
        self.vault = _VaultNS()
        self.link = _LinkNS()
        self.pocket = _PocketNS()


deed = _Deed()


# ---------------------------------------------------------------------------
# Slice-to-slice linking (top-level; inter-slice networking — unrelated to
# deed.link, which enrolls DEVICES for one identity rather than calling
# another slice)
# ---------------------------------------------------------------------------

def _link_env_name(name):
    return "DRIFT_LINK_" + re.sub(r"[^A-Z0-9]", "_", name.upper()) + "_URL"


class _SliceClient:
    def __init__(self, name):
        self.name = name

    def _url(self, path):
        base = os.environ.get(_link_env_name(self.name))
        if not base:
            raise RuntimeError(
                f'drift: not linked to slice "{self.name}" — run `drift slice link add {self.name}`'
            )
        return base.rstrip("/") + "/" + str(path).lstrip("/")

    def request(self, method, path, headers=None, body=None):
        h = {"X-Drift-Slice": os.environ.get("DRIFT_SLICE", "")}
        if headers:
            h.update(headers)
        return http_request(method, self._url(path), headers=h, body=body)

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, body=None):
        return self.request(
            "POST", path,
            headers={"Content-Type": "application/json"},
            body=json.dumps(body) if body is not None else None,
        )


def slice(name):
    """A client for another slice you've LINKED to (`drift slice link`). The
    call travels in-cluster and carries this slice's identity (X-Drift-Slice)."""
    return _SliceClient(name)


def caller_slice(req):
    """The linked slice that called this request, or "" if not via a link."""
    headers = (req or {}).get("headers") or {}
    for k, v in headers.items():
        if k.lower() == "x-drift-slice":
            return v
    return ""


def env(key):
    """An environment variable value ("" if unset)."""
    return os.environ.get(key, "")


# The sacred triad is the SOLE entrypoint for state primitives — remove the
# top-level names so everything stateful goes through drift.backbone.
del secret, cache, nosql, blob, lock, queue, sql, SQL
