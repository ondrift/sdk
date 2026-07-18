//! Drift SDK for Rust Atomic functions.
//!
//! Provides:
//!   - run(handler): Entry point (deployed or local mode).
//!   - `backbone` — the B of the sacred A·B·C triad; the SOLE entrypoint for
//!     every STATE primitive: `backbone::secret`, `backbone::cache`,
//!     `backbone::nosql`, `backbone::queue`, `backbone::blob`, `backbone::lock`,
//!     `backbone::sql`, `backbone::realtime`. (There is no top-level
//!     `drift_sdk::secret` etc. — go through `drift_sdk::backbone`.)
//!   - `deed` — Drift's 4th architectural pillar (identity, verified), a peer
//!     of Backbone/Atomic/Canvas: `deed::keyauth` (passwordless Ed25519
//!     device-key auth), `deed::jwt` (general-purpose HS256 sign/verify —
//!     KeyAuth mints its own tokens through it), `deed::vault` (an
//!     account-key-wrapped keyring), `deed::link` (multi-device attestation /
//!     enrollment / revocation), `deed::pocket` (E2EE per-identity app data,
//!     JWT-gated). Not to be confused with `slice`/`caller_slice` below —
//!     that's inter-slice networking; `deed::link` enrolls another DEVICE for
//!     the same identity, it has nothing to do with calling another SLICE.
//!   - log(msg): Writes to stderr (captured by runner).
//!   - http_request(): Outbound HTTP from within a function.
//!   - slice(name) / caller_slice(req): slice-to-slice linking.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpListener;

pub use serde_json::Value;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn run<F>(handler: F)
where
    F: Fn(Value) -> Value + 'static,
{
    if std::env::var("DRIFT_RUNTIME").is_ok() {
        run_deployed(&handler);
    } else {
        run_local(handler);
    }
}

fn run_deployed<F: Fn(Value) -> Value>(handler: &F) {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).unwrap();
    let mut req: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
    if let Some(q) = req.get("query").and_then(|v| v.as_str()).map(str::to_string) {
        let mut parsed = serde_json::Map::new();
        for pair in q.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = match pair.split_once('=') {
                Some((k, v)) => (k, v),
                None => (pair, ""),
            };
            parsed.insert(percent_decode(k), Value::String(percent_decode(v)));
        }
        if let Some(obj) = req.as_object_mut() {
            obj.insert("query".to_string(), Value::Object(parsed));
        }
    }
    let resp = handler(req);
    let out = serde_json::to_string(&resp).unwrap();
    io::stdout().write_all(out.as_bytes()).unwrap();
    io::stdout().flush().unwrap();
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("00"), 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' { out.push(b' '); } else { out.push(bytes[i]); }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn run_local<F: Fn(Value) -> Value + 'static>(handler: F) {
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .unwrap_or(8080);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).unwrap();
    eprintln!("drift-sdk: local server starting on :{}", port);

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        let mut reader = BufReader::new(match stream.try_clone() {
            Ok(s) => s,
            Err(_) => continue,
        });

        // Read request line.
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).is_err() {
            continue;
        }
        let parts: Vec<&str> = request_line.trim().splitn(3, ' ').collect();
        if parts.len() < 2 {
            continue;
        }
        let _ = parts[0]; // method is no longer surfaced to handlers
        let path_str = parts[1];

        // Read headers.
        let mut headers = serde_json::Map::new();
        let mut content_length: usize = 0;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                break;
            }
            let trimmed = line.trim().to_string();
            if trimmed.is_empty() {
                break;
            }
            if let Some((k, v)) = trimmed.split_once(": ") {
                if k.eq_ignore_ascii_case("content-length") {
                    content_length = v.parse().unwrap_or(0);
                }
                headers.insert(k.to_lowercase(), Value::String(v.to_string()));
            }
        }

        // Read body.
        let body = if content_length > 0 {
            let mut buf = vec![0u8; content_length];
            let _ = reader.read_exact(&mut buf);
            let raw = String::from_utf8_lossy(&buf).to_string();
            match serde_json::from_str::<Value>(&raw) {
                Ok(v) => v,
                Err(_) => Value::String(raw),
            }
        } else {
            Value::Null
        };

        // Parse path and query.
        let (path, query) = match path_str.split_once('?') {
            Some((p, q)) => (p, q),
            None => (path_str, ""),
        };

        let req = serde_json::json!({
            "path": path,
            "headers": Value::Object(headers),
            "query": query,
            "body": body,
        });

        let resp = handler(req);
        let status = resp.get("status").and_then(|s| s.as_u64()).unwrap_or(200);
        let out = serde_json::to_string(&resp).unwrap();

        let response = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            status,
            out.len(),
            out
        );
        let _ = stream.write_all(response.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Backbone transport
// ---------------------------------------------------------------------------

fn get_backbone_url() -> String {
    std::env::var("BACKBONE_URL").unwrap_or_default()
}

// Deed lives on its own listener/port now — a separate env var, separate
// base URL from Backbone's. See the `deed` module below.
fn get_deed_url() -> String {
    std::env::var("DEED_URL").unwrap_or_default()
}

fn percent_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

fn call(method: &str, path: &str, body: Option<Value>) -> Option<Value> {
    let base = get_backbone_url();
    if base.is_empty() {
        return call_local(method, path, body);
    }
    call_at(&base, method, path, body)
}

// Same wire logic as `call`, against an explicit base URL rather than
// Backbone's. Used by `deed_call` (Deed's own listener/port) so `jwt` — the
// one Deed primitive that keeps this softer, non-`Result` calling
// convention — can move off Backbone's base URL without duplicating the
// whole HTTP dance.
fn call_at(base: &str, method: &str, path: &str, body: Option<Value>) -> Option<Value> {
    let url = format!("{}/{}", base, path);

    let result = if let Some(b) = body {
        ureq::request(method, &url)
            .set("Content-Type", "application/json")
            .send_string(&serde_json::to_string(&b).unwrap_or_default())
    } else {
        ureq::request(method, &url).call()
    };

    match result {
        Ok(resp) => {
            if resp.status() == 204 {
                return None;
            }
            let text = resp.into_string().unwrap_or_default();
            if text.is_empty() {
                return None;
            }
            serde_json::from_str(&text)
                .ok()
                .or_else(|| Some(Value::String(text)))
        }
        Err(ureq::Error::Status(204, _)) => None,
        Err(ureq::Error::Status(_, resp)) => {
            let text = resp.into_string().unwrap_or_default();
            if text.is_empty() {
                return None;
            }
            serde_json::from_str(&text)
                .ok()
                .or_else(|| Some(Value::String(text)))
        }
        Err(_) => None,
    }
}

// `jwt`'s transport: same forgiving None-on-miss convention as `call`, but
// against Deed's own base URL (DEED_URL) since /jwt/* moved off Backbone's
// listener along with the rest of Deed. Falls back to `call_local` in local
// dev, same as `call` — `call_local` has no jwt/* arm, so this preserves
// today's local-dev behavior (None) unchanged.
fn deed_call(method: &str, path: &str, body: Option<Value>) -> Option<Value> {
    let base = get_deed_url();
    if base.is_empty() {
        return call_local(method, path, body);
    }
    call_at(&base, method, path, body)
}

// call_raw posts raw bytes (used by blob.put). The platform's /blob/put
// expects ?bucket=&key= query params and a binary body, not JSON.
fn call_raw(method: &str, path: &str, data_bytes: &[u8], content_type: &str) -> Option<Vec<u8>> {
    let base = get_backbone_url();
    if base.is_empty() {
        return None;
    }
    let url = format!("{}/{}", base, path);
    let resp = ureq::request(method, &url)
        .set("Content-Type", content_type)
        .send_bytes(data_bytes);
    match resp {
        Ok(r) => {
            let mut buf = Vec::new();
            let _ = r.into_reader().read_to_end(&mut buf);
            Some(buf)
        }
        Err(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Local in-memory backbone (used when BACKBONE_URL is unset, i.e. local dev)
// ---------------------------------------------------------------------------

use std::sync::{Mutex, OnceLock};

struct LocalStore {
    secrets: HashMap<String, String>,
    cache: HashMap<String, Value>,
    nosql: HashMap<String, Vec<Value>>,    // collection -> docs
    queues: HashMap<String, Vec<Value>>,
    blobs: HashMap<String, Value>,
    locks: HashMap<String, String>,        // name -> token
}

fn local_store() -> &'static Mutex<LocalStore> {
    static STORE: OnceLock<Mutex<LocalStore>> = OnceLock::new();
    STORE.get_or_init(|| {
        Mutex::new(LocalStore {
            secrets: HashMap::new(),
            cache: HashMap::new(),
            nosql: HashMap::new(),
            queues: HashMap::new(),
            blobs: HashMap::new(),
            locks: HashMap::new(),
        })
    })
}

fn parse_query(path: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(q) = path.split_once('?').map(|(_, q)| q) {
        for pair in q.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(k.to_string(), v.to_string());
            }
        }
    }
    params
}

fn call_local(_method: &str, path: &str, body: Option<Value>) -> Option<Value> {
    let q = parse_query(path);
    let base_path = path.split('?').next().unwrap_or(path);
    let mut store = local_store().lock().ok()?;

    match base_path {
        // Secrets — read from env vars in local dev (matching other SDKs)
        "secret/get" => {
            let name = q.get("name")?;
            store.secrets.get(name)
                .map(|v| Value::String(v.clone()))
                .or_else(|| std::env::var(name).ok().map(Value::String))
        }
        "secret/set" => {
            let b = body?;
            let name = b.get("name")?.as_str()?.to_string();
            let value = b.get("value")?.as_str()?.to_string();
            store.secrets.insert(name, value);
            None
        }
        "secret/delete" => {
            let name = q.get("name")?;
            store.secrets.remove(name);
            None
        }
        "secret/list" => {
            let names: Vec<Value> = store.secrets.keys().map(|k| Value::String(k.clone())).collect();
            Some(Value::Array(names))
        }
        // Cache
        "cache/get" => {
            let key = q.get("key")?;
            store.cache.get(key).cloned()
        }
        "cache/set" => {
            let b = body?;
            let key = b.get("key")?.as_str()?.to_string();
            let value = b.get("value")?.clone();
            store.cache.insert(key, value);
            None
        }
        "cache/del" => {
            let key = q.get("key")?;
            store.cache.remove(key);
            None
        }
        // NoSQL — use 1-indexed string keys matching other SDKs
        "write" => {
            let b = body?;
            let coll = b.get("collection")?.as_str()?.to_string();
            let docs = store.nosql.entry(coll).or_default();
            let next_id = docs.len() + 1;
            let key = format!("{}", next_id);
            let mut doc = b.clone();
            doc["_key"] = Value::String(key.clone());
            docs.push(doc);
            Some(serde_json::json!({"key": key}))
        }
        "read" => {
            let coll = q.get("collection")?;
            let key = q.get("key")?;
            let docs = store.nosql.get(coll)?;
            docs.iter().find(|d| d.get("_key").and_then(|k| k.as_str()) == Some(key)).cloned()
        }
        "nosql/list" => {
            let coll = q.get("collection")?;
            let docs = store.nosql.get(coll).cloned().unwrap_or_default();
            Some(Value::Array(docs))
        }
        "nosql/drop" => {
            let coll = q.get("collection")?;
            store.nosql.remove(coll);
            None
        }
        // Queues
        "queue/push" => {
            let b = body?;
            let name = b.get("queue")?.as_str()?.to_string();
            let msg = b.get("body")?.clone();
            store.queues.entry(name).or_default().push(msg);
            None
        }
        "queue/pop" => {
            let b = body?;
            let name = b.get("queue")?.as_str()?.to_string();
            let q = store.queues.get_mut(&name)?;
            if q.is_empty() { return None; }
            Some(q.remove(0))
        }
        // Blobs
        "blob/put" => {
            let b = body?;
            let name = b.get("name")?.as_str()?.to_string();
            let data = b.get("data")?.clone();
            store.blobs.insert(name, data);
            None
        }
        "blob/get" => {
            let name = q.get("name")?;
            store.blobs.get(name).cloned()
        }
        // Locks — check for existing owner before acquiring
        "lock/acquire" => {
            let b = body?;
            let name = b.get("name")?.as_str()?.to_string();
            if store.locks.contains_key(&name) {
                return None; // lock already held
            }
            let token = format!("local-{}", store.locks.len());
            store.locks.insert(name, token.clone());
            Some(serde_json::json!({"token": token}))
        }
        "lock/release" => {
            let b = body?;
            let name = b.get("name")?.as_str()?.to_string();
            store.locks.remove(&name);
            None
        }
        _ => None,
    }
}

// ===========================================================================
// Backbone — the B of the sacred A·B·C triad. The SOLE entrypoint for every
// state primitive. Reach these as `backbone::secret::get(...)`,
// `backbone::queue(...)`, `backbone::realtime::channel(...)`, etc. Nothing
// stateful lives at the crate root.
//
// Sub-modules glob-import the crate root (`use crate::*`) for the private
// transport helpers (`call`, `percent_encode`, …) and `Value` — exactly the
// target the modules used before they were nested under `backbone`.
// ===========================================================================

pub mod backbone {
    use crate::*;

    // -----------------------------------------------------------------------
    // Secret
    // -----------------------------------------------------------------------

    pub mod secret {
        use crate::*;

        /// Read order:
        ///   1. `DRIFT_SECRET_<NAME>` env var — set by the runner from the
        ///      function's `@atomic-secrets` allowlist. Only path that works
        ///      in production: backbone `/secret/get` is SAT-guarded and the
        ///      subprocess does not hold the SAT.
        ///   2. HTTP fallback — local-dev only. In production, returns 401.
        pub fn get(name: &str) -> String {
            let env_key = format!("DRIFT_SECRET_{}", name.to_uppercase());
            if let Ok(v) = std::env::var(&env_key) {
                return v;
            }
            match call("GET", &format!("secret/get?name={}", percent_encode(name)), None) {
                Some(Value::String(s)) => s,
                Some(v) => serde_json::to_string(&v).unwrap_or_default(),
                None => String::new(),
            }
        }

        pub fn set(name: &str, value: &str) {
            call("POST", "secret/set", Some(serde_json::json!({"name": name, "value": value})));
        }

        pub fn delete(name: &str) {
            call("DELETE", &format!("secret/delete?name={}", percent_encode(name)), None);
        }
    }

    // -----------------------------------------------------------------------
    // Cache
    // -----------------------------------------------------------------------

    pub mod cache {
        use crate::*;

        pub fn get(key: &str) -> Option<Value> {
            call("GET", &format!("cache/get?key={}", percent_encode(key)), None)
        }

        pub fn set(key: &str, value: Value, ttl: u64) {
            let mut payload = serde_json::json!({"key": key, "value": value});
            if ttl > 0 {
                payload["ttl"] = Value::from(ttl);
            }
            call("POST", "cache/set", Some(payload));
        }

        pub fn delete(key: &str) {
            call("DELETE", &format!("cache/del?key={}", percent_encode(key)), None);
        }
    }

    // -----------------------------------------------------------------------
    // NoSQL
    // -----------------------------------------------------------------------

    pub mod nosql {
        use crate::*;

        pub struct Collection {
            name: String,
        }

        pub fn collection(name: &str) -> Collection {
            Collection { name: name.to_string() }
        }

        impl Collection {
            pub fn insert(&self, doc: Value) -> String {
                let mut payload = serde_json::json!({"collection": self.name});
                if let Value::Object(map) = doc {
                    for (k, v) in map {
                        payload[&k] = v;
                    }
                } else {
                    payload["data"] = doc;
                }
                match call("POST", "write", Some(payload)) {
                    Some(Value::Object(m)) => {
                        m.get("key").and_then(|v| v.as_str()).unwrap_or("").to_string()
                    }
                    _ => String::new(),
                }
            }

            pub fn read(&self, key: &str) -> Option<Value> {
                call("GET", &format!("read?collection={}&key={}", percent_encode(&self.name), percent_encode(key)), None)
            }

            pub fn get(&self, id: &str) -> Option<Value> {
                let path = format!(
                    "nosql/list?collection={}&field=_id&value={}",
                    percent_encode(&self.name),
                    percent_encode(id),
                );
                match call("GET", &path, None) {
                    Some(Value::Array(arr)) if !arr.is_empty() => Some(arr[0].clone()),
                    _ => None,
                }
            }

            pub fn delete(&self, key: &str) {
                call("POST", &format!(
                    "nosql/delete?collection={}&key={}",
                    percent_encode(&self.name),
                    percent_encode(key),
                ), None);
            }

            pub fn list(&self, filter: Option<HashMap<String, String>>) -> Vec<Value> {
                let mut path = format!("nosql/list?collection={}", percent_encode(&self.name));
                if let Some(f) = filter {
                    for (k, v) in f {
                        path.push_str(&format!("&field={}&value={}", percent_encode(&k), percent_encode(&v)));
                    }
                }
                match call("GET", &path, None) {
                    Some(Value::Array(arr)) => arr,
                    _ => vec![],
                }
            }

            pub fn drop(&self) {
                call("POST", &format!("nosql/drop?collection={}", percent_encode(&self.name)), None);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Queue
    // -----------------------------------------------------------------------

    pub struct QueueHandle {
        name: String,
    }

    pub fn queue(name: &str) -> QueueHandle {
        QueueHandle { name: name.to_string() }
    }

    impl QueueHandle {
        pub fn push(&self, body: Value) {
            call("POST", "queue/push", Some(serde_json::json!({"queue": self.name, "body": body})));
        }

        pub fn pop(&self) -> Option<Value> {
            call("POST", "queue/pop", Some(serde_json::json!({"queue": self.name})))
        }
    }

    // -----------------------------------------------------------------------
    // Blob
    // -----------------------------------------------------------------------

    pub mod blob {
        use crate::*;

        fn split_bucket_key(name: &str) -> (&str, &str) {
            match name.split_once('/') {
                Some((b, k)) => (b, k),
                None => ("default", name),
            }
        }

        pub fn put(name: &str, data: &[u8], content_type: Option<&str>) {
            let (bucket, key) = split_bucket_key(name);
            let path = format!("blob/put?bucket={}&key={}", percent_encode(bucket), percent_encode(key));
            call_raw("POST", &path, data, content_type.unwrap_or("application/octet-stream"));
        }

        pub fn get(name: &str) -> Option<Vec<u8>> {
            let (bucket, key) = split_bucket_key(name);
            let path = format!("blob/get?bucket={}&key={}", percent_encode(bucket), percent_encode(key));
            let base = get_backbone_url();
            if base.is_empty() { return None; }
            let url = format!("{}/{}", base, path);
            match ureq::get(&url).call() {
                Ok(r) => {
                    let mut buf = Vec::new();
                    let _ = r.into_reader().read_to_end(&mut buf);
                    Some(buf)
                }
                Err(_) => None,
            }
        }
    }

    // -----------------------------------------------------------------------
    // Lock
    // -----------------------------------------------------------------------

    pub mod lock {
        use crate::*;

        pub fn acquire(name: &str, ttl: u64) -> String {
            match call("POST", "lock/acquire", Some(serde_json::json!({"name": name, "ttl": ttl}))) {
                Some(Value::Object(m)) => {
                    m.get("token").and_then(|v| v.as_str()).unwrap_or("").to_string()
                }
                _ => String::new(),
            }
        }

        pub fn release(name: &str, token: &str) {
            call("POST", "lock/release", Some(serde_json::json!({"name": name, "token": token})));
        }
    }

    // -----------------------------------------------------------------------
    // Realtime — pub/sub fan-out over the slice's Canvas WebSocket hub.
    // -----------------------------------------------------------------------
    //
    // Subscribers connect over WebSocket at the Canvas route /realtime/<name>;
    // publish fans a message out to every connected subscriber.

    pub mod realtime {
        use crate::*;

        pub struct Channel {
            name: String,
        }

        pub fn channel(name: &str) -> Channel {
            Channel { name: name.to_string() }
        }

        impl Channel {
            /// Publish a message to every subscriber. Returns the recipient count.
            pub fn publish(&self, message: Value) -> u64 {
                match call("POST", "realtime/publish", Some(serde_json::json!({"channel": self.name, "message": message}))) {
                    Some(Value::Object(m)) => m.get("recipients").and_then(|v| v.as_u64()).unwrap_or(0),
                    _ => 0,
                }
            }

            /// The number of subscribers currently connected to this channel.
            pub fn presence(&self) -> u64 {
                match call("GET", &format!("realtime/presence?channel={}", percent_encode(&self.name)), None) {
                    Some(Value::Object(m)) => m.get("present").and_then(|v| v.as_u64()).unwrap_or(0),
                    _ => 0,
                }
            }
        }
    }

    // ─── SQL ──────────────────────────────────────────────────────────────
    //
    // Per-slice SQLite databases addressed by name. Wire shape: one JSON
    // envelope per call ({db, sql, args, tx?}). See docs/memos/backbone-sql.md.
    //
    //   let db = drift_sdk::backbone::sql("clinic");
    //   let rows = db.query("SELECT * FROM appointments WHERE slot >= ?", &[json!("2026-05-01")]);
    //   let res = db.execute("INSERT INTO appointments(...) VALUES(?, ?)", &[json!("alice"), json!("10:00")]);

    pub struct SqlDb {
        name: String,
    }

    pub struct SqlTx {
        db: String,
        token: String,
    }

    #[derive(Default, Debug, Clone)]
    pub struct SqlExecResult {
        pub rows_affected: i64,
        pub last_insert_id: i64,
    }

    fn sql_exec_from_value(v: Option<Value>) -> SqlExecResult {
        let v = match v {
            Some(v) => v,
            None => return SqlExecResult::default(),
        };
        SqlExecResult {
            rows_affected: v.get("rows_affected").and_then(|x| x.as_i64()).unwrap_or(0),
            last_insert_id: v.get("last_insert_id").and_then(|x| x.as_i64()).unwrap_or(0),
        }
    }

    pub fn sql<S: Into<String>>(name: S) -> SqlDb {
        SqlDb { name: name.into() }
    }

    fn sql_rows(resp: Option<Value>) -> Vec<serde_json::Map<String, Value>> {
        let v = match resp {
            Some(v) => v,
            None => return vec![],
        };
        let cols: Vec<String> = v
            .get("columns")
            .and_then(|c| c.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|e| e.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let rows = v.get("rows").and_then(|r| r.as_array()).cloned().unwrap_or_default();
        rows.into_iter()
            .map(|row| {
                let arr = row.as_array().cloned().unwrap_or_default();
                let mut map = serde_json::Map::new();
                for (i, col) in cols.iter().enumerate() {
                    map.insert(col.clone(), arr.get(i).cloned().unwrap_or(Value::Null));
                }
                map
            })
            .collect()
    }

    impl SqlDb {
        pub fn query(&self, sql: &str, args: &[Value]) -> Vec<serde_json::Map<String, Value>> {
            let body = serde_json::json!({"db": self.name, "sql": sql, "args": args});
            sql_rows(call("POST", "sql/query", Some(body)))
        }

        pub fn execute(&self, sql: &str, args: &[Value]) -> SqlExecResult {
            let body = serde_json::json!({"db": self.name, "sql": sql, "args": args});
            sql_exec_from_value(call("POST", "sql/execute", Some(body)))
        }

        pub fn begin(&self) -> Option<SqlTx> {
            let body = serde_json::json!({"db": self.name});
            let raw = call("POST", "sql/begin", Some(body))?;
            let token = raw.get("tx")?.as_str()?.to_string();
            Some(SqlTx { db: self.name.clone(), token })
        }
    }

    impl SqlTx {
        pub fn query(&self, sql: &str, args: &[Value]) -> Vec<serde_json::Map<String, Value>> {
            let body = serde_json::json!({"db": self.db, "sql": sql, "args": args, "tx": self.token});
            sql_rows(call("POST", "sql/query", Some(body)))
        }

        pub fn execute(&self, sql: &str, args: &[Value]) -> SqlExecResult {
            let body = serde_json::json!({"db": self.db, "sql": sql, "args": args, "tx": self.token});
            sql_exec_from_value(call("POST", "sql/execute", Some(body)))
        }

        pub fn commit(&self) {
            let _ = call("POST", "sql/commit", Some(serde_json::json!({"tx": self.token})));
        }

        pub fn rollback(&self) {
            let _ = call("POST", "sql/rollback", Some(serde_json::json!({"tx": self.token})));
        }
    }
}

// ===========================================================================
// Deed — Drift's 4th architectural pillar (identity, verified), a peer of
// Backbone/Atomic/Canvas. Five primitives: `deed::keyauth`, `deed::jwt`,
// `deed::vault`, `deed::link`, `deed::pocket`.
//
// KeyAuth: passwordless Ed25519 device-key auth. JWT: general-purpose HS256
// sign/verify (KeyAuth mints its own tokens through it). Vault: an
// account-key-wrapped keyring. Link: multi-device attestation / enrollment /
// revocation. Pocket: E2EE per-identity app data, JWT-gated.
//
// Not to be confused with slice-to-slice linking (`slice(name)` /
// `caller_slice`, further below) — that's inter-slice networking, a
// different, still-hypothetical future pillar. `deed::link` enrolls another
// DEVICE for the same identity; it has nothing to do with calling another
// SLICE.
// ===========================================================================

pub mod deed {
    use crate::*;

    /// Error returned by any Deed call: a transport failure, a non-2xx HTTP
    /// status, or a malformed response body. Deed calls always fail loudly —
    /// in particular, a "not found" Get (e.g. `vault::get` on a uid that
    /// never wrote a backup, `pocket::get` on an unknown key) comes back as
    /// `Err`, never a silent `Ok(None)`/default.
    #[derive(Debug, Clone)]
    pub struct DeedError {
        pub message: String,
    }

    impl std::fmt::Display for DeedError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }
    impl std::error::Error for DeedError {}

    fn unavailable() -> DeedError {
        DeedError {
            message: "drift deed: requires a running slice (DEED_URL) — not available in local dev".to_string(),
        }
    }

    fn malformed(path: &str) -> DeedError {
        DeedError { message: format!("drift deed: malformed {} response", path) }
    }

    // Shared low-level transport for every Deed primitive except `jwt` (which
    // keeps using the crate-root `deed_call()`, a softer None-on-miss
    // convention — see its doc comment — since it's a straight relocation,
    // not a rewrite). Unlike `call()`/`deed_call()`, this propagates transport
    // failures and HTTP >= 400 as `Err` rather than collapsing them into
    // `None` — the stricter convention Deed's wire contract calls for. `token`
    // is `Some(...)` only for `pocket`, whose routes are JWT-gated; every
    // other Deed primitive passes `None`. Deed has its own listener/port
    // (DEED_URL), separate from Backbone's.
    fn request(method: &str, path: &str, token: Option<&str>, body: Option<&Value>) -> Result<Value, DeedError> {
        let base = get_deed_url();
        if base.is_empty() {
            return Err(unavailable());
        }
        let url = format!("{}/{}", base, path);

        let mut req = ureq::request(method, &url);
        if let Some(t) = token {
            req = req.set("Authorization", &format!("Bearer {}", t));
        }

        let result = if let Some(b) = body {
            req.set("Content-Type", "application/json")
                .send_string(&serde_json::to_string(b).unwrap_or_default())
        } else {
            req.call()
        };

        match result {
            Ok(resp) => {
                if resp.status() == 204 {
                    return Ok(Value::Null);
                }
                let text = resp.into_string().unwrap_or_default();
                if text.is_empty() {
                    return Ok(Value::Null);
                }
                serde_json::from_str(&text)
                    .map_err(|e| DeedError { message: format!("drift deed: parse {} response: {}", path, e) })
            }
            Err(ureq::Error::Status(code, resp)) => {
                let text = resp.into_string().unwrap_or_default();
                let text = text.trim();
                let msg = if text.is_empty() {
                    format!("drift deed: {} HTTP {}", path, code)
                } else {
                    format!("drift deed: {} HTTP {}: {}", path, code, text)
                };
                Err(DeedError { message: msg })
            }
            Err(ureq::Error::Transport(t)) => {
                Err(DeedError { message: format!("drift deed: {} request failed: {}", path, t) })
            }
        }
    }

    // -----------------------------------------------------------------------
    // KeyAuth — passwordless Ed25519 device-key auth.
    // -----------------------------------------------------------------------
    //
    // The client holds a keypair; the pubkey IS its identity — no accounts,
    // no passwords, no email. `challenge` mints a one-time nonce; the client
    // signs the canonical {domain,nonce,pubkey}; `verify` checks the
    // signature and issues this slice's session JWT (sub = pubkey). The
    // slice stores nothing about the user — it just verifies a signature.
    // The client half (keygen + signing + recovery-phrase derivation) is a
    // small browser library; see the @ondrift/keyauth memo.

    pub mod keyauth {
        use super::{request, malformed, DeedError};

        /// Returns a one-time login nonce for the given Ed25519 public key
        /// (32-byte hex). Single-use, short-TTL, cache-backed in the slice.
        pub fn challenge(pubkey: &str) -> Result<String, DeedError> {
            let resp = request("POST", "keyauth/challenge", None, Some(&serde_json::json!({"pubkey": pubkey})))?;
            resp.get("nonce")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| malformed("keyauth/challenge"))
        }

        /// Checks the client's signature over the canonical
        /// {domain,nonce,pubkey} and, on success, returns this slice's
        /// session JWT (sub = the pubkey). `domain` namespaces the signature
        /// to your app (e.g. "myapp-auth-v1") so a signature for one
        /// app/slice can't be replayed at another — the client must sign the
        /// same domain. A bad/expired/absent challenge or a bad signature
        /// returns `Err`.
        pub fn verify(pubkey: &str, sig: &str, domain: &str) -> Result<String, DeedError> {
            let resp = request(
                "POST",
                "keyauth/verify",
                None,
                Some(&serde_json::json!({"pubkey": pubkey, "sig": sig, "domain": domain})),
            )?;
            resp.get("token")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| malformed("keyauth/verify"))
        }
    }

    // -----------------------------------------------------------------------
    // JWT primitive
    // -----------------------------------------------------------------------
    //
    // HS256 minting + verification, signed with the slice's per-slice JKey. The
    // signing key never leaves the slice's backbone process; all operations flow
    // through loopback HTTP to backbone /jwt/{sign,verify,slice-id}.
    //
    // Design: internal/todo/slice-jwt-primitive.md.

    pub mod jwt {
        use crate::*;

        /// Returned by [`verify`] when the token fails validation. `reason`
        /// is one of the stable wire strings: `malformed`, `bad_signature`,
        /// `expired`, `not_yet_valid`, `wrong_algorithm`, `wrong_issuer`,
        /// `wrong_audience`, `invalid_claims`, `missing_exp`, `internal_error`.
        #[derive(Debug, Clone)]
        pub struct JWTError {
            pub reason: String,
        }

        impl std::fmt::Display for JWTError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "jwt verify: {}", self.reason)
            }
        }
        impl std::error::Error for JWTError {}

        /// Claims payload. Matches the wire shape of the platform JWT
        /// primitive — standard fields plus an open `custom` map.
        #[derive(Debug, Clone, Default)]
        pub struct Claims {
            pub sub: Option<String>,
            pub iat: Option<i64>,
            pub exp: Option<i64>,
            pub nbf: Option<i64>,
            pub iss: Option<String>,
            pub aud: Option<Vec<String>>,
            pub jti: Option<String>,
            pub custom: Option<Value>,
        }

        /// Sign a JWT with the slice's HS256 JKey. `exp` is required;
        /// `iat`, `iss`, and `jti` are auto-set when `None`.
        pub fn issue(claims: Claims) -> String {
            let mut body = serde_json::Map::new();
            if let Some(v) = claims.sub    { body.insert("sub".to_string(),    Value::String(v)); }
            if let Some(v) = claims.iat    { body.insert("iat".to_string(),    Value::from(v)); }
            if let Some(v) = claims.exp    { body.insert("exp".to_string(),    Value::from(v)); }
            if let Some(v) = claims.nbf    { body.insert("nbf".to_string(),    Value::from(v)); }
            if let Some(v) = claims.iss    { body.insert("iss".to_string(),    Value::String(v)); }
            if let Some(v) = claims.aud    { body.insert("aud".to_string(),    serde_json::to_value(v).unwrap_or(Value::Null)); }
            if let Some(v) = claims.jti    { body.insert("jti".to_string(),    Value::String(v)); }
            if let Some(v) = claims.custom { body.insert("custom".to_string(), v); }

            match deed_call("POST", "jwt/sign", Some(Value::Object(body))) {
                Some(Value::Object(m)) => {
                    m.get("token").and_then(|v| v.as_str()).unwrap_or_default().to_string()
                }
                _ => String::new(),
            }
        }

        /// Validate a token. Returns parsed claims on success; `JWTError`
        /// on validation failure.
        pub fn verify(token: &str, audience: Option<&str>, allowed_issuer: Option<&str>) -> Result<Value, JWTError> {
            let mut body = serde_json::Map::new();
            body.insert("token".to_string(), Value::String(token.to_string()));
            if let Some(a) = audience { body.insert("audience".to_string(), Value::String(a.to_string())); }
            if let Some(i) = allowed_issuer { body.insert("allowed_issuer".to_string(), Value::String(i.to_string())); }

            let resp = match deed_call("POST", "jwt/verify", Some(Value::Object(body))) {
                Some(Value::Object(m)) => m,
                _ => return Err(JWTError { reason: "internal_error".to_string() }),
            };
            let valid = resp.get("valid").and_then(|v| v.as_bool()).unwrap_or(false);
            if !valid {
                let reason = resp.get("reason").and_then(|v| v.as_str()).unwrap_or("internal_error").to_string();
                return Err(JWTError { reason });
            }
            Ok(resp.get("claims").cloned().unwrap_or(Value::Null))
        }

        /// The slice's auto-set issuer string ("drift-slice-<user>-<slice>").
        pub fn slice_id() -> String {
            match deed_call("GET", "jwt/slice-id", None) {
                Some(Value::Object(m)) => {
                    m.get("slice_id").and_then(|v| v.as_str()).unwrap_or_default().to_string()
                }
                _ => String::new(),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Vault — zero-knowledge recovery store.
    // -----------------------------------------------------------------------
    //
    // Opaque, user-scoped, append-only. The client encrypts the blob under a
    // key derived from its recovery phrase (which the slice NEVER sees), so
    // Drift stores the backup but cannot read it. Scoped to a uid the caller
    // supplies — typically the authenticated KeyAuth pubkey. Backed by
    // Deed's own dedicated routes: AES-256-GCM at rest (defense in depth
    // only — the blob must already be ciphertext before it arrives, since
    // that's the actual source of Vault's confidentiality guarantee) and a
    // per-item size quota. No Driftfile declaration needed.

    pub mod vault {
        use crate::*;
        use super::{request, malformed, DeedError};

        /// Appends an opaque encrypted backup blob for uid. Append-only (a
        /// new version each call); `get` returns the newest.
        pub fn put(uid: &str, blob: Value) -> Result<(), DeedError> {
            request("POST", "deed/vault/put", None, Some(&serde_json::json!({"uid": uid, "blob": blob})))?;
            Ok(())
        }

        /// Returns the most recent backup blob for uid. Returns `Err` if uid
        /// has never written one (same "not found is an error" convention as
        /// `backbone::secret::get`/`backbone::blob::get`).
        pub fn get(uid: &str) -> Result<Value, DeedError> {
            let resp = request("GET", &format!("deed/vault/get?uid={}", percent_encode(uid)), None, None)?;
            resp.get("blob").cloned().ok_or_else(|| malformed("deed/vault/get"))
        }
    }

    // -----------------------------------------------------------------------
    // Link — multi-device continuity.
    // -----------------------------------------------------------------------
    //
    // Generalizes the enroll/attest/revoke pattern so an identity's KeyAuth
    // session can move to a second, third, ... device. The signature
    // parameters below (sig, attesting_pubkey, etc.) are produced entirely
    // client-side — this SDK only forwards them, the same way
    // `keyauth::verify` forwards a signature it never computes itself. The
    // one rule the whole design rests on: Deed verifies, it never decides —
    // a device is only ever added on the strength of a signature from a
    // device already active in the identity's registry.
    //
    // Not to be confused with slice-to-slice linking (`slice(name)` /
    // `caller_slice`, at the crate root) — this Link enrolls a DEVICE for
    // one identity, it does not call another slice.

    pub mod link {
        use super::{request, malformed, DeedError};

        /// Returned by [`complete`]. `identity`/`sealed` are set only once
        /// `status == "attested"` (`sealed` only if `attest_with_seal`
        /// supplied one).
        #[derive(Debug, Clone, Default)]
        pub struct LinkStatus {
            pub status: String,
            pub identity: Option<String>,
            pub sealed: Option<String>,
        }

        /// Returned by [`session_info`].
        #[derive(Debug, Clone, Default)]
        pub struct LinkSessionInfo {
            pub new_pubkey: String,
            pub metadata: Option<String>,
        }

        /// Starts a device-linking session for a not-yet-enrolled device's
        /// pubkey (usually carried in a QR code alongside the pubkey).
        /// Returns a session ID for an already-active device to present to
        /// `attest`.
        pub fn begin(pubkey: &str) -> Result<String, DeedError> {
            let resp = request("POST", "deed/link/begin", None, Some(&serde_json::json!({"pubkey": pubkey})))?;
            resp.get("session_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| malformed("deed/link/begin"))
        }

        /// Same as [`begin`], but also registers an opaque `metadata`
        /// string an attesting device can read back via `session_info` —
        /// e.g. an ephemeral key it should seal a payload for. Deed never
        /// interprets it. A separate function rather than an added
        /// parameter to `begin`, since Rust has no default arguments and
        /// changing `begin`'s arity would break every existing caller.
        pub fn begin_with_metadata(pubkey: &str, metadata: &str) -> Result<String, DeedError> {
            let resp = request(
                "POST",
                "deed/link/begin",
                None,
                Some(&serde_json::json!({"pubkey": pubkey, "metadata": metadata})),
            )?;
            resp.get("session_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| malformed("deed/link/begin"))
        }

        /// A read-only, repeatable peek at a pending session — what an
        /// attesting device (which only ever learns `session_id`, from a
        /// scanned/typed code) uses to learn `new_pubkey` (`attest`'s
        /// message is verified server-side against the session's own
        /// stored value, never the request body, so the attester has to
        /// reconstruct it exactly) and whatever opaque metadata the
        /// joining device passed to `begin`.
        pub fn session_info(session_id: &str) -> Result<LinkSessionInfo, DeedError> {
            let resp = request(
                "POST",
                "deed/link/session",
                None,
                Some(&serde_json::json!({"session_id": session_id})),
            )?;
            Ok(LinkSessionInfo {
                new_pubkey: resp.get("new_pubkey").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                metadata: resp.get("metadata").and_then(|v| v.as_str()).map(|s| s.to_string()),
            })
        }

        /// Renders `text` (in practice, a Link session ID) as a scannable
        /// QR code, returning inline SVG markup. Pure rendering — no
        /// session or identity involvement, so it works for any short
        /// string.
        pub fn qr(text: &str) -> Result<String, DeedError> {
            let resp = request("POST", "deed/link/qr", None, Some(&serde_json::json!({"text": text})))?;
            resp.get("svg")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| malformed("deed/link/qr"))
        }

        /// Has an already-active device vouch for the session's pending
        /// device. `sig` is the client's signature over the canonical
        /// {domain,identity,new_pubkey} message — computed client-side,
        /// never by this SDK.
        pub fn attest(identity: &str, session_id: &str, attesting_pubkey: &str, sig: &str) -> Result<(), DeedError> {
            request(
                "POST",
                "deed/link/attest",
                None,
                Some(&serde_json::json!({
                    "identity": identity,
                    "session_id": session_id,
                    "attesting_pubkey": attesting_pubkey,
                    "sig": sig,
                })),
            )?;
            Ok(())
        }

        /// Same as [`attest`], but also carries an opaque `sealed` string
        /// relayed back once [`complete`] reports "attested" — e.g. a
        /// payload end-to-end-encrypted for whatever key the joiner
        /// published as `begin_with_metadata`'s metadata. Deed only relays
        /// it, never opens it. A separate function for the same arity
        /// reason as `begin_with_metadata`.
        pub fn attest_with_seal(identity: &str, session_id: &str, attesting_pubkey: &str, sig: &str, sealed: &str) -> Result<(), DeedError> {
            request(
                "POST",
                "deed/link/attest",
                None,
                Some(&serde_json::json!({
                    "identity": identity,
                    "session_id": session_id,
                    "attesting_pubkey": attesting_pubkey,
                    "sig": sig,
                    "sealed": sealed,
                })),
            )?;
            Ok(())
        }

        /// Polls a session the new device started with `begin`, returning
        /// whether an active device has attested it yet.
        pub fn complete(session_id: &str) -> Result<LinkStatus, DeedError> {
            let resp = request("POST", "deed/link/complete", None, Some(&serde_json::json!({"session_id": session_id})))?;
            Ok(LinkStatus {
                status: resp.get("status").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                identity: resp.get("identity").and_then(|v| v.as_str()).map(|s| s.to_string()),
                sealed: resp.get("sealed").and_then(|v| v.as_str()).map(|s| s.to_string()),
            })
        }

        /// Deactivates target_pubkey in identity's device registry. Any
        /// currently-active device may revoke another (or itself);
        /// revoking_pubkey is the device doing the revoking, sig its
        /// signature over the canonical {domain,identity,target_pubkey}
        /// message.
        pub fn revoke(identity: &str, target_pubkey: &str, revoking_pubkey: &str, sig: &str) -> Result<(), DeedError> {
            request(
                "POST",
                "deed/link/revoke",
                None,
                Some(&serde_json::json!({
                    "identity": identity,
                    "target_pubkey": target_pubkey,
                    "revoking_pubkey": revoking_pubkey,
                    "sig": sig,
                })),
            )?;
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // Pocket — E2EE per-identity app data.
    // -----------------------------------------------------------------------
    //
    // An app's actual data — E2EE, content-keyed, following an identity
    // across every device Link has enrolled. The crypto work happens
    // entirely client-side before anything reaches this primitive; Pocket
    // never encrypts or decrypts the payload itself. Every call takes
    // `token` explicitly (the JWT `keyauth::verify` returned) rather than
    // holding hidden session state — matching the rest of this SDK's
    // stateless posture inside an Atomic function invocation. The token's
    // sub is the only identity a call can read or write under; there is no
    // way to name a different one. Every call sends it as an
    // `Authorization: Bearer <token>` header.

    pub mod pocket {
        use crate::*;
        use super::{request, malformed, DeedError};

        /// Stores blob under key for whichever identity token resolves to.
        pub fn set(token: &str, key: &str, blob: Value) -> Result<(), DeedError> {
            request(
                "POST",
                "deed/pocket/set",
                Some(token),
                Some(&serde_json::json!({"key": key, "blob": blob})),
            )?;
            Ok(())
        }

        /// Returns the blob stored under key for token's identity. Returns
        /// `Err` if no such key exists.
        pub fn get(token: &str, key: &str) -> Result<Value, DeedError> {
            let resp = request(
                "GET",
                &format!("deed/pocket/get?key={}", percent_encode(key)),
                Some(token),
                None,
            )?;
            resp.get("blob").cloned().ok_or_else(|| malformed("deed/pocket/get"))
        }

        /// Removes key for token's identity. Returns `Err` if no such key
        /// exists.
        pub fn delete(token: &str, key: &str) -> Result<(), DeedError> {
            request("POST", "deed/pocket/delete", Some(token), Some(&serde_json::json!({"key": key})))?;
            Ok(())
        }

        /// Returns every key stored under token's identity — never another
        /// identity's, even by guessing.
        pub fn list(token: &str) -> Result<Vec<String>, DeedError> {
            let resp = request("GET", "deed/pocket/list", Some(token), None)?;
            match resp {
                Value::Array(arr) => Ok(arr
                    .into_iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()),
                _ => Err(malformed("deed/pocket/list")),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

pub fn log(msg: &str) {
    eprintln!("{}", msg);
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

/// Default 30-second timeout. A function calling a hung remote
/// shouldn't hold an Atomic invocation open longer than this; the
/// runner's per-invocation deadline is the absolute ceiling. Use
/// `http_request_with_timeout` to override.
pub fn http_request(
    method: &str,
    url: &str,
    headers: Option<HashMap<String, String>>,
    body: Option<&str>,
) -> (u16, String) {
    http_request_with_timeout(method, url, headers, body, std::time::Duration::from_secs(30))
}

/// `http_request` with a caller-supplied timeout.
pub fn http_request_with_timeout(
    method: &str,
    url: &str,
    headers: Option<HashMap<String, String>>,
    body: Option<&str>,
    timeout: std::time::Duration,
) -> (u16, String) {
    // TLS is an opt-in feature so the default build stays pure Rust (no C
    // cross-compiler needed to deploy). Fail loudly rather than silently
    // when a function tries HTTPS without it.
    #[cfg(not(feature = "tls"))]
    if url.starts_with("https://") {
        return (
            0,
            "drift-sdk: outbound HTTPS needs the \"tls\" feature — set \
             drift-sdk = { ..., features = [\"tls\"] } in Cargo.toml (deploys then \
             require a C cross-toolchain such as zig)."
                .to_string(),
        );
    }

    let agent = ureq::AgentBuilder::new()
        .timeout(timeout)
        .build();
    let mut req = agent.request(method, url);

    if let Some(hdrs) = headers {
        for (k, v) in hdrs {
            req = req.set(&k, &v);
        }
    }

    let result = if let Some(b) = body {
        req.send_string(b)
    } else {
        req.call()
    };

    match result {
        Ok(r) => {
            let status = r.status();
            let text = r.into_string().unwrap_or_default();
            (status, text)
        }
        Err(ureq::Error::Status(code, resp)) => {
            let text = resp.into_string().unwrap_or_default();
            (code, text)
        }
        Err(_) => (0, String::new()),
    }
}

// ─── SSE (Server-Sent Events) ────────────────────────────────────────────────

/// Entry point for SSE streaming functions.
///
/// ```rust,no_run
/// // @atomic http=get:events auth=none stream=sse
/// drift_sdk::run_sse(|req, emit| {
///     for i in 0..10 {
///         emit("counter", &serde_json::json!({"value": i}));
///         std::thread::sleep(std::time::Duration::from_secs(1));
///     }
/// });
/// ```
pub fn run_sse<F>(handler: F)
where
    F: Fn(serde_json::Value, &dyn Fn(&str, &serde_json::Value)) + Send + Sync + 'static,
{
    if std::env::var("DRIFT_RUNTIME").is_ok() {
        run_sse_deployed(&handler);
    } else {
        run_sse_local(handler);
    }
}

fn run_sse_deployed<F>(handler: &F)
where
    F: Fn(serde_json::Value, &dyn Fn(&str, &serde_json::Value)),
{
    let input = {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf).ok();
        buf
    };
    let req: serde_json::Value = serde_json::from_str(&input).unwrap_or_default();

    let emit = |event: &str, data: &serde_json::Value| {
        let mut stdout = std::io::stdout().lock();
        if !event.is_empty() {
            let _ = write!(stdout, "event: {}\n", event);
        }
        let _ = write!(stdout, "data: {}\n\n", data);
        let _ = stdout.flush();
    };

    handler(req, &emit);
}

// Local-dev SSE server. Mirrors run_local's request-parsing plumbing but
// writes the response chunks directly to the TCP stream and flushes after
// every emit, so SSE events reach the client immediately rather than being
// buffered until process exit.
fn run_sse_local<F>(handler: F)
where
    F: Fn(serde_json::Value, &dyn Fn(&str, &serde_json::Value)) + Send + Sync + 'static,
{
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .unwrap_or(8080);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).unwrap();
    eprintln!("drift-sdk: local SSE server starting on :{}", port);

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        let mut reader = BufReader::new(match stream.try_clone() {
            Ok(s) => s,
            Err(_) => continue,
        });

        let mut request_line = String::new();
        if reader.read_line(&mut request_line).is_err() {
            continue;
        }
        let parts: Vec<&str> = request_line.trim().splitn(3, ' ').collect();
        if parts.len() < 2 {
            continue;
        }
        let _ = &parts[0]; // method is no longer surfaced to handlers
        let path_str = parts[1].to_string();

        let mut headers = serde_json::Map::new();
        let mut content_length: usize = 0;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                break;
            }
            let trimmed = line.trim().to_string();
            if trimmed.is_empty() {
                break;
            }
            if let Some((k, v)) = trimmed.split_once(": ") {
                if k.eq_ignore_ascii_case("content-length") {
                    content_length = v.parse().unwrap_or(0);
                }
                headers.insert(k.to_lowercase(), Value::String(v.to_string()));
            }
        }

        let body = if content_length > 0 {
            let mut buf = vec![0u8; content_length];
            let _ = reader.read_exact(&mut buf);
            let raw = String::from_utf8_lossy(&buf).to_string();
            match serde_json::from_str::<Value>(&raw) {
                Ok(v) => v,
                Err(_) => Value::String(raw),
            }
        } else {
            Value::Null
        };

        let (path, query) = match path_str.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path_str, String::new()),
        };

        let req = serde_json::json!({
            "path": path,
            "headers": Value::Object(headers),
            "query": query,
            "body": body,
        });

        // Write SSE response headers.
        let head = "HTTP/1.1 200 OK\r\n\
                    Content-Type: text/event-stream\r\n\
                    Cache-Control: no-cache, no-transform\r\n\
                    Connection: keep-alive\r\n\
                    X-Accel-Buffering: no\r\n\
                    \r\n";
        if stream.write_all(head.as_bytes()).is_err() {
            continue;
        }
        let _ = stream.flush();

        // Hand the handler an emit closure that writes + flushes per event.
        // Wrap the stream in a Mutex so emit can borrow it mutably across
        // calls without violating Rust's aliasing rules.
        let stream_mu = std::sync::Mutex::new(stream);
        let emit = |event: &str, data: &Value| {
            if let Ok(mut s) = stream_mu.lock() {
                if !event.is_empty() {
                    let _ = write!(s, "event: {}\n", event);
                }
                let _ = write!(s, "data: {}\n\n", data);
                let _ = s.flush();
            }
        };

        handler(req, &emit);
    }
}

// ─── WebSocket ───────────────────────────────────────────────────────────────

/// Bidirectional WebSocket connection bridged through stdin/stdout.
pub struct WsConn {
    lines: std::io::Lines<std::io::BufReader<std::io::Stdin>>,
}

impl WsConn {
    fn new() -> Self {
        use std::io::BufRead;
        WsConn {
            lines: std::io::BufReader::new(std::io::stdin()).lines(),
        }
    }

    /// Read the next message from the client. Returns None on disconnect.
    pub fn read(&mut self) -> Option<serde_json::Value> {
        match self.lines.next() {
            Some(Ok(line)) if !line.trim().is_empty() => {
                serde_json::from_str(&line).ok().or(Some(serde_json::Value::String(line)))
            }
            _ => None,
        }
    }

    /// Send a JSON message to the client.
    pub fn write(&self, data: &serde_json::Value) {
        println!("{}", data);
    }

    /// Send a raw string message to the client.
    pub fn write_raw(&self, msg: &str) {
        println!("{}", msg);
    }
}

/// Entry point for WebSocket functions.
///
/// ```rust,no_run
/// // @atomic http=get:chat auth=none stream=ws
/// drift_sdk::run_ws(|req, conn| {
///     loop {
///         match conn.read() {
///             Some(msg) => conn.write(&serde_json::json!({"echo": msg})),
///             None => break,
///         }
///     }
/// });
/// ```
pub fn run_ws<F>(handler: F)
where
    F: FnOnce(serde_json::Value, &mut WsConn),
{
    let mut conn = WsConn::new();

    // First line is the initial request.
    let req = conn.read().unwrap_or_default();
    handler(req, &mut conn);
}

// ---------------------------------------------------------------------------
// Slice-to-slice linking (top-level; the seed of a future "D" pillar)
// ---------------------------------------------------------------------------
//
// Not Backbone — this is inter-slice networking, parked at the crate root until
// the fourth pillar lands.

fn link_env_name(name: &str) -> String {
    let mangled: String = name
        .to_uppercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("DRIFT_LINK_{}_URL", mangled)
}

/// A client for another slice you've LINKED to (`drift slice link`). The call
/// travels in-cluster and carries this slice's identity (X-Drift-Slice).
pub struct SliceClient {
    name: String,
}

pub fn slice(name: &str) -> SliceClient {
    SliceClient { name: name.to_string() }
}

impl SliceClient {
    fn url(&self, path: &str) -> Result<String, String> {
        let base = std::env::var(link_env_name(&self.name)).unwrap_or_default();
        if base.is_empty() {
            return Err(format!(
                "drift: not linked to slice \"{}\" — run `drift slice link add {}`",
                self.name, self.name
            ));
        }
        Ok(format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/')))
    }

    /// Issue a request to the linked slice. On (0, msg) the link is missing.
    pub fn request(
        &self,
        method: &str,
        path: &str,
        headers: Option<HashMap<String, String>>,
        body: Option<&str>,
    ) -> (u16, String) {
        let url = match self.url(path) {
            Ok(u) => u,
            Err(e) => return (0, e),
        };
        let mut h = headers.unwrap_or_default();
        h.insert("X-Drift-Slice".to_string(), std::env::var("DRIFT_SLICE").unwrap_or_default());
        http_request(method, &url, Some(h), body)
    }

    pub fn get(&self, path: &str) -> (u16, String) {
        self.request("GET", path, None, None)
    }

    pub fn post(&self, path: &str, body: Option<&str>) -> (u16, String) {
        let mut h = HashMap::new();
        h.insert("Content-Type".to_string(), "application/json".to_string());
        self.request("POST", path, Some(h), body)
    }
}

/// The linked slice that called this request, or "" if not via a link.
pub fn caller_slice(req: &Value) -> String {
    if let Some(headers) = req.get("headers").and_then(|h| h.as_object()) {
        for (k, v) in headers {
            if k.eq_ignore_ascii_case("x-drift-slice") {
                return v.as_str().unwrap_or_default().to_string();
            }
        }
    }
    String::new()
}

/// An environment variable value ("" if unset).
pub fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}
