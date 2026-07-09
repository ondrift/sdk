# The Drift SDK

The SDK is what user-written code imports to talk to [Drift](https://ondrift.eu). It exists in six languages — **Go, Python, Node.js, Ruby, PHP, and Rust** — and every one exposes the same API surface, follows the same wire protocol, and provides the same local-development experience. This repository (`github.com/ondrift/sdk`) is the single authoritative source for all six; one tag per release versions them together.

## Contents

- [Install](#install)
- [Writing a function](#writing-a-function)
- [API Reference](#api-reference)
  - [Go](#go)
  - [Python](#python)
  - [Node.js](#nodejs)
  - [Ruby](#ruby)
  - [PHP](#php)
  - [Rust](#rust)
- [License](#license)

---

## Install

No version is pinned anywhere — reference the SDK unversioned and every build tracks the latest tag.

```bash
# Go — name the ROOT module (NOT …/sdk/v2/go: the repo's early history had a
# nested …/sdk/go module whose stale pseudo-versions still resolve first).
# The /v2 segment is Go's own semantic-import-versioning rule for a v2+
# module — a plain `github.com/ondrift/sdk@latest` (no /v2) stays on the v1
# line forever, by design: existing v1 consumers never see a breaking change
# land under their feet.
go get github.com/ondrift/sdk/v2@latest      # then: import drift "github.com/ondrift/sdk/v2/go"
```
```text
# Python (requirements.txt)
drift-sdk @ git+https://github.com/ondrift/sdk.git#subdirectory=python
```
```bash
# Node.js (package.json dependency) — #semver:* = latest tag
npm i "github:ondrift/sdk#semver:*"
```
```ruby
# Ruby (Gemfile) — branch:master is the repo default (and bundler's default
# for git gems); glob locates the gemspec in ruby/
gem "drift-sdk", git: "https://github.com/ondrift/sdk", branch: "master", glob: "ruby/*.gemspec"
```
```bash
# PHP — VCS repo + the "*" constraint tracks the latest tag
composer config repositories.drift vcs https://github.com/ondrift/sdk
composer require "ondrift/sdk:*"
```
```toml
# Rust (Cargo.toml)
drift-sdk = { git = "https://github.com/ondrift/sdk" }
```

---

## Writing a function

A Drift Atomic function is a **named handler with an `@atomic` annotation**. The CLI reads the annotation and generates the program's entry point — you don't write `main()` or call `run()` yourself (Ruby is the one exception; see below).

- **Annotation** — `@atomic http=<method>:<route> auth=<none|jwt|…>` for an HTTP route, or `@atomic queue=<name>` for a queue consumer. Path params use `:name` (e.g. `reviewer/decision/:id`).
- **Arguments** — `(req)` for a request with no body (GET); `(body, req)` when there's a request body or queue payload.
- **Return** — `(status, message, payload)`, with an optional 4th `headers` map. It's a tuple in Go/Python/Rust, an array in Node/PHP, a hash in Ruby.
- **Request** — read path params from `req.params`, query from `req.query`, headers from `req.headers`, raw body from `req.body`.
- **Streaming** — annotate `stream=sse` or `stream=ws`; the handler receives an extra `emit` (SSE) or `conn` (WebSocket) argument.

Everything stateful lives under the **Backbone** namespace — the *B* of the sacred A·B·C triad and the **sole entrypoint** for state. Nothing stateful sits at the top level. Backbone groups secrets, key/value cache, NoSQL documents, queues, blobs, locks, JWT, relational `sql(name)`, realtime channels, passwordless **KeyAuth** (Ed25519 device-key auth), and the zero-knowledge **Vault** (recovery store) — all shown per-language below. Cross-slice calling (`slice(name)` / `callerSlice(req)`) stays at the top level on purpose: it's inter-slice networking, the seed of a future *D* pillar — not Backbone.

---

## API Reference

The Backbone primitives are identical across languages; each section shows that language's idiomatic form. Reach state through the triad namespace — `drift.Backbone` (Go), `drift.backbone` (Python/Node), `Drift::Backbone` (Ruby), `\Drift\Backbone\…` (PHP), `drift_sdk::backbone` (Rust) — never the top level.

### Go

```go
import drift "github.com/ondrift/sdk/v2/go"
```

```go
// @atomic http=post:reviewer/decision/:id auth=none
func PostReviewerDecisionId(body RequestBody, req drift.Request) (int, string, any, map[string]string) {
    id := strings.TrimSpace(req.Params["id"])
    if id == "" {
        return 400, "Bad Request", map[string]string{"error": "id required"}, nil
    }
    return 200, "OK", map[string]any{"id": id, "action": body.Action}, nil
}
// GET handlers take just (req drift.Request).
```

**Backbone** — the sole entrypoint for state (`drift.Backbone.*`)
```go
drift.Backbone.Secret.Get("API_KEY")                           // .Set(k, v), .Delete(k)
c := drift.Backbone.NoSQL.Collection("orders")
c.Insert(doc); c.Get(id); c.Read(key); c.List(filter); c.Delete(key); c.Drop()
drift.Backbone.Cache.Set("k", v, 60); drift.Backbone.Cache.Get("k")        // ttl seconds
drift.Backbone.Queue("q").Push(m); drift.Backbone.Queue("q").Pop()
drift.Backbone.Blob.Put("k", data, "image/png"); drift.Backbone.Blob.Get("k")
drift.Backbone.Lock.Acquire("r", 30); drift.Backbone.Lock.Release("r", token)
drift.Backbone.SQL("clinic").Query("SELECT …", args)           // .Execute(…), .Begin() (transactions)
drift.Backbone.JWT.Issue(drift.JWTClaims{Sub: "alice"}); drift.Backbone.JWT.Verify(tok, drift.JWTVerifyOptions{})
drift.Backbone.Realtime.Channel("events").Publish(msg)         // fan-out → WS subscribers at /realtime/events; .Presence()
drift.Backbone.KeyAuth.Challenge(pubkey); drift.Backbone.KeyAuth.Verify(pubkey, sig, "my-app")  // passwordless Ed25519 → this slice's JWT
drift.Backbone.Vault.Put(uid, blob); drift.Backbone.Vault.Get(uid)         // zero-knowledge recovery store (keyvault collection)
// Top-level (NOT Backbone) — cross-slice networking + utilities:
drift.Slice("c12").Post("/api/events", body); drift.Slice("c12").Get("/x")  // call a LINKED slice (`drift slice link add c12`); carries X-Drift-Slice
drift.CallerSlice(req)                                          // name of the linked slice that called you ("" if not via a link)
drift.Log("msg"); drift.HTTPRequest("GET", url, nil, nil)      // HTTPRequestWithTimeout to override 30s
```

### Python

```python
import drift   # requires Python 3.9+
```

```python
# @atomic http=post:submit auth=none
def post_submit(body, req):
    if not body.get("permit_type"):
        return 400, "Bad Request", {"error": "permit_type is required"}
    return 200, "OK", {"ok": True}
# GET handlers take just (req).
```

**Backbone** — the sole entrypoint for state (`drift.backbone.*`)
```python
drift.backbone.secret.get("API_KEY")                          # .set(k, v), .delete(k)
c = drift.backbone.nosql.collection("orders")
c.insert(doc); c.get(id); c.read(key); c.list(filter); c.delete(key); c.drop()
drift.backbone.cache.set("k", v, 60); drift.backbone.cache.get("k"); drift.backbone.cache.delete("k")
drift.backbone.queue("q").push(m); drift.backbone.queue("q").pop()
drift.backbone.blob.put("k", data); drift.backbone.blob.get("k")
drift.backbone.lock.acquire("r", ttl=30); drift.backbone.lock.release("r", token)
db = drift.backbone.sql("clinic"); db.query("SELECT …", args)  # .execute(…), .begin()
drift.backbone.jwt.issue({"sub": "alice"}); drift.backbone.jwt.verify(tok)      # raises drift.JWTError
drift.backbone.realtime.channel("events").publish(msg)        # fan-out → WS subscribers at /realtime/events; .presence()
drift.backbone.keyauth.challenge(pubkey); drift.backbone.keyauth.verify(pubkey, sig, "my-app")
drift.backbone.vault.put(uid, blob); drift.backbone.vault.get(uid)
# Top-level (NOT Backbone) — cross-slice networking + utilities:
drift.slice("c12").post("/api/events", body); drift.slice("c12").get("/x")
drift.caller_slice(req)
drift.log("msg"); drift.http_request("GET", url, timeout=30)
```

### Node.js

```js
const drift = require("@ondrift/sdk");   // requires Node.js 18+
```

```js
// @atomic http=get:status/:token auth=none
async function getStatusToken(req) {
  const token = (req.params && req.params.token) || "";
  if (!token) return [400, "Bad Request", { error: "token required" }];
  return [200, "OK", { token }];
}
module.exports = { getStatusToken };   // export the handler for the CLI wrapper
```

**Backbone** — the sole entrypoint for state (`drift.backbone.*`; calls are `async` — `await` them)
```js
await drift.backbone.secret.get("API_KEY");                   // .set(k, v), .delete(k)
const c = drift.backbone.nosql.collection("orders");
await c.insert(doc); await c.get(id); await c.read(key); await c.list(filter); await c.delete(key); await c.drop();
await drift.backbone.cache.set("k", v, 60); await drift.backbone.cache.get("k"); await drift.backbone.cache.delete("k");
await drift.backbone.queue("q").push(m); await drift.backbone.queue("q").pop();
await drift.backbone.blob.put("k", data); await drift.backbone.blob.get("k");
await drift.backbone.lock.acquire("r", 30); await drift.backbone.lock.release("r", token);
const db = drift.backbone.sql("clinic"); await db.query("SELECT …", args);   // .execute(…), .begin()
await drift.backbone.jwt.issue({ sub: "alice" }); await drift.backbone.jwt.verify(tok);   // throws drift.JWTError
await drift.backbone.realtime.channel("events").publish(msg); // fan-out → WS subscribers at /realtime/events; .presence()
await drift.backbone.keyauth.challenge(pubkey); await drift.backbone.keyauth.verify(pubkey, sig, "my-app");
await drift.backbone.vault.put(uid, blob); await drift.backbone.vault.get(uid);
// Top-level (NOT Backbone) — cross-slice networking + utilities:
await drift.slice("c12").post("/api/events", body); await drift.slice("c12").get("/x");
drift.callerSlice(req);
drift.log("msg"); await drift.httpRequest("GET", url);                  // { timeoutMs } 5th arg
```

### Ruby

```ruby
require "drift"   # requires Ruby 3.0+
```

```ruby
# @atomic http=get:reviewer/queue auth=none
def get_reviewer_queue(req)
  rows = Drift::Backbone::Nosql.collection("submissions").list
  { "status" => 200, "message" => "OK", "payload" => { "count" => rows.length } }
end

Drift.run(method(:get_reviewer_queue))   # Ruby: end the file with this
```

**Backbone** — the sole entrypoint for state (`Drift::Backbone::*`)
```ruby
Drift::Backbone::Secret.get("API_KEY")                        # .set(k, v), .delete(k)
c = Drift::Backbone::Nosql.collection("orders")
c.insert(doc); c.get(id); c.list(filter)
Drift::Backbone::Cache.get("k"); Drift::Backbone::Cache.set("k", v, ttl: 60)
Drift::Backbone.queue("q").push(m); Drift::Backbone.queue("q").pop
Drift::Backbone::Blob.put("k", data, content_type: "image/png"); Drift::Backbone::Blob.get("k")
Drift::Backbone::Lock.acquire("r", ttl: 30); Drift::Backbone::Lock.release("r", token)
db = Drift::Backbone.sql("clinic"); db.query("SELECT …", args)   # .execute(…), .transaction { |tx| … }
Drift::Backbone::JWT.issue(sub: "alice", exp: Time.now.to_i + 3600); Drift::Backbone::JWT.verify(tok)   # raises Drift::JWTError
Drift::Backbone::Realtime.channel("events").publish(msg)      # fan-out → WS subscribers at /realtime/events; .presence
Drift::Backbone::KeyAuth.challenge(pubkey); Drift::Backbone::KeyAuth.verify(pubkey, sig, "my-app")
Drift::Backbone::Vault.put(uid, blob); Drift::Backbone::Vault.get(uid)
# Top-level (NOT Backbone) — cross-slice networking + utilities:
Drift.slice("c12").post("/api/events", body); Drift.slice("c12").get("/x")
Drift.caller_slice(req)
Drift.log("msg"); Drift.http_request("GET", url, timeout: 30)
```

> The local-dev server uses `webrick` (lazy-required; Ruby 3.0+ dropped it from stdlib). Add `gem "webrick"` to your Gemfile if needed.

### PHP

```php
// installed via Composer → \Drift\… is autoloaded (requires PHP 8.1+)
```

```php
<?php
// @atomic queue=notify auth=none
function queue_notifier($body, $req = null) {
    $id = is_array($body) ? ($body['submission_id'] ?? '') : '';
    if ($id === '') return [200, 'OK', ['ok' => false]];
    return [200, 'OK', ['ok' => true]];
}
// HTTP handlers take ($req) for GET, ($body, $req) when there's a body.
```

**Backbone** — the sole entrypoint for state (`\Drift\Backbone\…`)
```php
\Drift\Backbone\Secret::get('API_KEY');                       // ::set($k, $v), ::delete($k)
$c = \Drift\Backbone\Nosql::collection('orders');
$c->insert($doc); $c->get($id); $c->read($key); $c->list($filter); $c->delete($key); $c->drop();
\Drift\Backbone\Cache::set('k', $v, 60); \Drift\Backbone\Cache::get('k'); \Drift\Backbone\Cache::delete('k');
\Drift\Backbone\queue('q')->push($m); \Drift\Backbone\queue('q')->pop();
\Drift\Backbone\Blob::put('k', $data); \Drift\Backbone\Blob::get('k');
\Drift\Backbone\Lock::acquire('r', 30); \Drift\Backbone\Lock::release('r', $token);
$db = \Drift\Backbone\sql('clinic'); $db->query('SELECT …', $args);   // ->execute(…), ->transaction(fn($tx)=>…)
\Drift\Backbone\JWT::issue(['sub' => 'alice']); \Drift\Backbone\JWT::verify($tok);   // throws \Drift\JWTError
\Drift\Backbone\Realtime::channel('events')->publish($msg);   // fan-out → WS subscribers at /realtime/events; ->presence()
\Drift\Backbone\KeyAuth::challenge($pubkey); \Drift\Backbone\KeyAuth::verify($pubkey, $sig, 'my-app');
\Drift\Backbone\Vault::put($uid, $blob); \Drift\Backbone\Vault::get($uid);
// Top-level (NOT Backbone) — cross-slice networking + utilities:
\Drift\slice('c12')->post('/api/events', $body); \Drift\slice('c12')->get('/x');
\Drift\caller_slice($req);
\Drift\log('msg'); \Drift\http_request('GET', $url);
```

### Rust

```toml
[dependencies]
drift-sdk  = { git = "https://github.com/ondrift/sdk" }
serde_json = "1"
```

```rust
use drift_sdk::{self as drift, Value};
use serde_json::json;

// @atomic queue=validate auth=none
pub fn queue_validator(body: Value, _req: Value) -> (i64, &'static str, Value) {
    let id = body.get("submission_id").and_then(|v| v.as_str()).unwrap_or("");
    if id.is_empty() {
        return (200, "OK", json!({ "ok": false }));
    }
    (200, "OK", json!({ "ok": true }))
}
```

**Backbone** — the sole entrypoint for state (`drift_sdk::backbone::*`)
```rust
use drift_sdk::backbone::{secret, cache, nosql, queue, blob, lock, jwt, sql, realtime, keyauth, vault};
secret::get("API_KEY");                                       // set(k, v), delete(k)
let c = nosql::collection("orders");
c.insert(doc); c.get(id); c.read(key); c.list(Some(filter)); c.delete(key); c.drop();
cache::set("k", v, 60); cache::get("k"); cache::delete("k");
queue("q").push(m); queue("q").pop();
blob::put("k", &data, Some("image/png")); blob::get("k");
lock::acquire("r", 30); lock::release("r", &token);
let db = sql("clinic"); db.query("SELECT …", &args);         // .execute(…), .begin()
jwt::issue(claims); jwt::verify(&tok, None, None);           // -> Result<Value, jwt::JWTError>
realtime::channel("events").publish(msg);                    // fan-out → WS subscribers at /realtime/events; .presence()
keyauth::challenge(&pubkey); keyauth::verify(&pubkey, &sig, "my-app");
vault::put(uid, blob); vault::get(uid);
// Top-level (NOT Backbone) — cross-slice networking + utilities:
drift_sdk::slice("c12").post("/api/events", Some(body)); drift_sdk::slice("c12").get("/x");
drift_sdk::caller_slice(&req);
drift_sdk::log("msg"); drift_sdk::http_request("GET", url, None, None);
```

> `ureq v2` is the maintained line we use today; `ureq v3` (2025) isn't a drop-in upgrade — re-evaluate when its API stabilises.

> **Outbound HTTPS is opt-in.** By default the SDK is pure Rust (`http_request` does plain HTTP), so a Rust function cross-compiles to the runner with **just rustup — no C toolchain**. To call `https://` URLs, enable the `tls` feature:
> ```toml
> drift-sdk = { git = "https://github.com/ondrift/sdk", features = ["tls"] }
> ```
> That pulls `ring` (C/assembly), so deploying then needs a C cross-toolchain — install [`zig`](https://ziglang.org) and `cargo install cargo-zigbuild`, or a musl cross-gcc. Calling `https://` without the feature returns a clear error rather than failing silently.

---

## License

MIT — see [`LICENSE`](LICENSE).

---

*This is the single source of truth for all six SDKs. Any change to the public API surface of any language MUST update the matching section above in the same change. Last verified 2026-06-27 — all six languages namespaced under `Backbone` (sole entrypoint for state) with Realtime + KeyAuth + Vault, plus top-level slice-link.*
