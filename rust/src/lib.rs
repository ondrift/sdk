//! Drift SDK for Rust Atomic functions.
//!
//! Provides:
//!   - run(handler): Entry point (deployed or local mode).
//!   - Backbone helpers: secret, cache, nosql, queue, blob, lock.
//!   - log(msg): Writes to stderr (captured by runner).
//!   - http_request(): Outbound HTTP from within a function.

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
        let method = parts[0];
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
            "method": method,
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

// ---------------------------------------------------------------------------
// Secret
// ---------------------------------------------------------------------------

pub mod secret {
    use super::*;

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

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

pub mod cache {
    use super::*;

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

// ---------------------------------------------------------------------------
// NoSQL
// ---------------------------------------------------------------------------

pub mod nosql {
    use super::*;

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

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Blob
// ---------------------------------------------------------------------------

pub mod blob {
    use super::*;

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

// ---------------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------------

pub mod lock {
    use super::*;

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

// ---------------------------------------------------------------------------
// JWT primitive
// ---------------------------------------------------------------------------
//
// HS256 minting + verification, signed with the slice's per-slice JKey. The
// signing key never leaves the slice's backbone process; all operations flow
// through loopback HTTP to backbone /jwt/{sign,verify,slice-id}.
//
// Design: internal/todo/slice-jwt-primitive.md.

pub mod jwt {
    use super::*;

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

        match call("POST", "jwt/sign", Some(Value::Object(body))) {
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

        let resp = match call("POST", "jwt/verify", Some(Value::Object(body))) {
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
        match call("GET", "jwt/slice-id", None) {
            Some(Value::Object(m)) => {
                m.get("slice_id").and_then(|v| v.as_str()).unwrap_or_default().to_string()
            }
            _ => String::new(),
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
/// ```rust
/// // @atomic route=get:events auth=none stream=sse
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
        let method = parts[0].to_string();
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
            "method": method,
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
/// ```rust
/// // @atomic route=get:chat auth=none stream=ws
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

// ─── SQL ────────────────────────────────────────────────────────────────────
//
// Per-slice SQLite databases addressed by name. Wire shape: one JSON
// envelope per call ({db, sql, args, tx?}). See docs/memos/backbone-sql.md.
//
//   let db = drift_sdk::sql("clinic");
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
