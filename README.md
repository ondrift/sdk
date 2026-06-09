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
# Go — name the ROOT module (NOT …/sdk/go: the repo's early history had a
# nested …/sdk/go module whose stale pseudo-versions still resolve first).
go get github.com/ondrift/sdk@latest      # then: import drift "github.com/ondrift/sdk/go"
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

Backbone also exposes a relational `sql(name)` primitive (`query` / `execute` / `begin`) alongside the key/value, document, queue, blob, lock, and JWT primitives shown below.

---

## API Reference

The seven Backbone primitives are identical across languages; each section shows that language's idiomatic form.

### Go

```go
import drift "github.com/ondrift/sdk/go"
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

**Backbone**
```go
drift.Secret.Get("API_KEY")                                    // .Set(k, v), .Delete(k)
c := drift.NoSQL.Collection("orders")
c.Insert(doc); c.Get(id); c.Read(key); c.List(filter); c.Delete(key); c.Drop()
drift.Cache.Set("k", v, 60); drift.Cache.Get("k")             // ttl seconds
drift.Queue("q").Push(m); drift.Queue("q").Pop()
drift.Blob.Put("k", data, "image/png"); drift.Blob.Get("k")
drift.Lock.Acquire("r", 30); drift.Lock.Release("r", token)
drift.JWT.Issue(drift.JWTClaims{Sub: "alice"}); drift.JWT.Verify(tok, drift.JWTVerifyOptions{})
drift.Log("msg"); drift.HTTPRequest("GET", url, nil, nil)     // HTTPRequestWithTimeout to override 30s
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

**Backbone**
```python
drift.secret.get("API_KEY")                                   # .set(k, v), .delete(k)
c = drift.nosql.collection("orders")
c.insert(doc); c.get(id); c.read(key); c.list(filter); c.delete(key); c.drop()
drift.cache.set("k", v, 60); drift.cache.get("k"); drift.cache.delete("k")
drift.queue("q").push(m); drift.queue("q").pop()
drift.blob.put("k", data); drift.blob.get("k")
drift.lock.acquire("r", ttl=30); drift.lock.release("r", token)
drift.jwt.issue({"sub": "alice"}); drift.jwt.verify(tok)      # raises drift.JWTError
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

**Backbone** (Backbone calls are `async` — `await` them)
```js
await drift.secret.get("API_KEY");                            // .set(k, v), .delete(k)
const c = drift.nosql.collection("orders");
await c.insert(doc); await c.get(id); await c.read(key); await c.list(filter); await c.delete(key); await c.drop();
await drift.cache.set("k", v, 60); await drift.cache.get("k"); await drift.cache.delete("k");
await drift.queue("q").push(m); await drift.queue("q").pop();
await drift.blob.put("k", data); await drift.blob.get("k");
await drift.lock.acquire("r", 30); await drift.lock.release("r", token);
await drift.jwt.issue({ sub: "alice" }); await drift.jwt.verify(tok);   // throws drift.JWTError
drift.log("msg"); await drift.httpRequest("GET", url);                  // { timeoutMs } 5th arg
```

### Ruby

```ruby
require "drift"   # requires Ruby 3.0+
```

```ruby
# @atomic http=get:reviewer/queue auth=none
def get_reviewer_queue(req)
  rows = Drift::Nosql.collection("submissions").list
  { "status" => 200, "message" => "OK", "payload" => { "count" => rows.length } }
end

Drift.run(method(:get_reviewer_queue))   # Ruby: end the file with this
```

**Backbone**
```ruby
Drift::Secret.get("API_KEY")                                  # .set(k, v)
c = Drift::Nosql.collection("orders")
c.insert(doc); c.get(id); c.list(filter)
Drift::Cache.get("k"); Drift::Cache.set("k", v, ttl: 60)
Drift.queue("q").push(m); Drift.queue("q").pop
Drift::Blob.put("k", data, content_type: "image/png"); Drift::Blob.get("k")
Drift::Lock.acquire("r", ttl: 30); Drift::Lock.release("r", token)
Drift::JWT.issue(sub: "alice", exp: Time.now.to_i + 3600); Drift::JWT.verify(tok)   # raises Drift::JWTError
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

**Backbone**
```php
\Drift\Secret::get('API_KEY');                                // ::set($k, $v), ::delete($k)
$c = \Drift\Nosql::collection('orders');
$c->insert($doc); $c->get($id); $c->read($key); $c->list($filter); $c->delete($key); $c->drop();
\Drift\Cache::set('k', $v, 60); \Drift\Cache::get('k'); \Drift\Cache::delete('k');
\Drift\queue('q')->push($m); \Drift\queue('q')->pop();
\Drift\Blob::put('k', $data); \Drift\Blob::get('k');
\Drift\Lock::acquire('r', 30); \Drift\Lock::release('r', $token);
\Drift\JWT::issue(['sub' => 'alice']); \Drift\JWT::verify($tok);   // throws \Drift\JWTError
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

**Backbone**
```rust
use drift_sdk::{secret, cache, nosql, queue, blob, lock, jwt};
secret::get("API_KEY");                                       // set(k, v), delete(k)
let c = nosql::collection("orders");
c.insert(doc); c.get(id); c.read(key); c.list(Some(filter)); c.delete(key); c.drop();
cache::set("k", v, 60); cache::get("k"); cache::delete("k");
queue("q").push(m); queue("q").pop();
blob::put("k", &data, Some("image/png")); blob::get("k");
lock::acquire("r", 30); lock::release("r", &token);
jwt::issue(claims); jwt::verify(&tok, None, None);           // -> Result<Value, JWTError>
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

*This is the single source of truth for all six SDKs. Any change to the public API surface of any language MUST update the matching section above in the same change. Last verified 2026-06-08.*
