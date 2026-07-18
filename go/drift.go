package drift

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Request is the incoming HTTP request passed to the function handler.
// The runner serializes the original HTTP request into this struct
// and writes it to the subprocess's stdin as JSON.
type Request struct {
	// The HTTP method is intentionally absent: a function is addressed by
	// method+path and was routed here AS the (e.g.) POST handler, so there's
	// nothing to branch on. An off-method request 404s; it never arrives here.
	Path         string            `json:"path"`
	Headers      map[string]string `json:"headers"`
	Query        string            `json:"query"`
	Body         json.RawMessage   `json:"body"`
	Params       map[string]string `json:"params,omitempty"`        // path parameters (e.g., ":id" → "123")
	RoutePattern string            `json:"route_pattern,omitempty"` // the registered pattern that matched (e.g. "reviewer/submission/:id"). Used by multi-handler wrappers to dispatch.
}

// Response is what the function handler returns. The runner reads
// this from the subprocess's stdout and converts it back into an HTTP response.
type Response struct {
	Status  int               `json:"status"`
	Message string            `json:"message"`
	Payload json.RawMessage   `json:"payload"`
	Headers map[string]string `json:"headers,omitempty"` // custom response headers
}

// Run is the entry point for Drift Atomic functions. The handler receives
// the incoming HTTP request and must return a response.
//
// You normally do NOT call Run yourself. The Drift CLI reads the `@atomic`
// annotation above your exported handler (e.g. `func PostItems(body, req)
// (int, string, any, map[string]string)`), generates the program's `main()`,
// and that generated main is what calls Run. Write the annotated handler and
// let the CLI wire the entry point. (Ruby is the one exception: its file ends
// with `Drift.run(method(:handler))`.) Call Run directly only if you are
// hand-building a main without the CLI.
//
// In deployed mode (DRIFT_RUNTIME is set): reads a JSON request from stdin,
// calls the handler, and writes the JSON response to stdout. The runner
// manages the HTTP routing, log capture, and metrics.
//
// In local dev mode (no DRIFT_RUNTIME): starts a local HTTP server on
// the port specified by the PORT env var (default 8080) so developers can
// test their functions with `drift atomic run`.
func Run(handler func(Request) Response) {
	if os.Getenv("DRIFT_RUNTIME") != "" {
		runDeployed(handler)
	} else {
		runLocal(handler)
	}
}

// runDeployed implements the deployed-mode protocol: read request from stdin,
// call handler, write response to stdout.
func runDeployed(handler func(Request) Response) {
	var req Request
	if err := json.NewDecoder(os.Stdin).Decode(&req); err != nil {
		resp := Response{
			Status:  http.StatusBadRequest,
			Message: "failed to decode request",
		}
		json.NewEncoder(os.Stdout).Encode(resp) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		return
	}

	resp := handler(req)
	json.NewEncoder(os.Stdout).Encode(resp) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
}

// runLocal starts a local HTTP server for development and testing.
func runLocal(handler func(Request) Response) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var body json.RawMessage
		if r.Body != nil {
			json.NewDecoder(r.Body).Decode(&body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		}

		headers := make(map[string]string)
		for k := range r.Header {
			headers[k] = r.Header.Get(k)
		}

		req := Request{
			Path:    r.URL.Path,
			Headers: headers,
			Query:   r.URL.RawQuery,
			Body:    body,
		}

		resp := handler(req)

		w.Header().Set("Content-Type", "application/json")
		for k, v := range resp.Headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(resp.Status)
		json.NewEncoder(w).Encode(map[string]any{ // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
			"status":  resp.Status,
			"message": resp.Message,
			"payload": resp.Payload,
		})
	})

	// #nosec G114 -- false-positive: see audit baseline.
	fmt.Fprintf(os.Stderr, "drift-sdk: local server starting on :%s\n", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil { // #nosec G114 -- false-positive: see the cross-repo audit baseline; this site has been reviewed.
		fmt.Fprintf(os.Stderr, "drift-sdk: server error: %v\n", err)
		os.Exit(1)
	}
}

//       Local backbone section          //

type memBackbone struct {
	mu     sync.Mutex
	nosql  map[string]map[string]json.RawMessage
	cache  map[string]json.RawMessage
	queues map[string][]json.RawMessage
	blobs  map[string][]byte
	locks  map[string]string
	nextID int
}

// localBackbone is an in-memory implementation of backbone services for local
// development with `drift atomic run`. All state lives in memory and is lost
// when the process exits.
var localBackbone = &memBackbone{
	nosql:  make(map[string]map[string]json.RawMessage),
	cache:  make(map[string]json.RawMessage),
	queues: make(map[string][]json.RawMessage),
	blobs:  make(map[string][]byte),
	locks:  make(map[string]string),
}

// backboneRequest is the internal envelope for backbone calls (local dev only).
type BackboneRequest struct {
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Body   json.RawMessage `json:"body,omitempty"`
}

func (m *memBackbone) handle(req BackboneRequest) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := req.Path
	method := strings.ToUpper(req.Method)

	var query url.Values
	if i := strings.IndexByte(path, '?'); i >= 0 {
		query, _ = url.ParseQuery(path[i+1:])
		path = path[:i]
	}

	switch {
	// --- NoSQL ---
	case path == "write" && method == "POST":
		var body map[string]any
		json.Unmarshal(req.Body, &body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		col, _ := body["collection"].(string)
		if col == "" {
			col = "default"
		}
		if m.nosql[col] == nil {
			m.nosql[col] = make(map[string]json.RawMessage)
		}
		m.nextID++
		key := fmt.Sprintf("%d", m.nextID)
		doc, _ := json.Marshal(body)
		m.nosql[col][key] = doc
		resp, _ := json.Marshal(map[string]string{"key": key})
		return resp

	case path == "read" && method == "GET":
		col := query.Get("collection")
		key := query.Get("key")
		if col == "" {
			col = "default"
		}
		if docs, ok := m.nosql[col]; ok {
			if doc, ok := docs[key]; ok {
				return doc
			}
		}
		return nil

	case path == "nosql/list" && method == "GET":
		col := query.Get("collection")
		if col == "" {
			col = "default"
		}
		docs, ok := m.nosql[col]
		if !ok {
			return marshalJSON([]any{})
		}
		filterField := query.Get("field")
		filterValue := query.Get("value")
		var results []json.RawMessage
		for _, doc := range docs {
			if filterField != "" {
				var obj map[string]any
				json.Unmarshal(doc, &obj) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
				v := fmt.Sprintf("%v", obj[filterField])
				if v != filterValue {
					continue
				}
			}
			results = append(results, doc)
		}
		if results == nil {
			results = []json.RawMessage{}
		}
		return marshalJSON(results)

	case strings.HasPrefix(path, "nosql/drop") && method == "POST":
		col := query.Get("collection")
		delete(m.nosql, col)
		return nil

	// --- Cache ---
	case path == "cache/set" && method == "POST":
		var body map[string]any
		json.Unmarshal(req.Body, &body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		key, _ := body["key"].(string)
		val, _ := json.Marshal(body["value"])
		m.cache[key] = val
		return nil

	case path == "cache/get" && method == "GET":
		key := query.Get("key")
		if val, ok := m.cache[key]; ok {
			return val
		}
		return nil

	case strings.HasPrefix(path, "cache/del"):
		key := query.Get("key")
		delete(m.cache, key)
		return nil

	// --- Queue ---
	case path == "queue/push" && method == "POST":
		var body map[string]any
		json.Unmarshal(req.Body, &body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		name, _ := body["queue"].(string)
		msg, _ := json.Marshal(body["body"])
		m.queues[name] = append(m.queues[name], msg)
		return nil

	case path == "queue/pop" && method == "POST":
		var body map[string]any
		json.Unmarshal(req.Body, &body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		name, _ := body["queue"].(string)
		q := m.queues[name]
		if len(q) == 0 {
			return nil
		}
		msg := q[0]
		m.queues[name] = q[1:]
		return msg

	// --- Blob ---
	case path == "blob/put" && method == "POST":
		var body map[string]any
		json.Unmarshal(req.Body, &body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		name, _ := body["name"].(string)
		data, _ := json.Marshal(body["data"])
		m.blobs[name] = data
		return nil

	case path == "blob/get" && method == "GET":
		name := query.Get("name")
		if data, ok := m.blobs[name]; ok {
			return data
		}
		return nil

	// --- Secret --- in local dev, read from environment variables (loaded from .env by the CLI)
	case path == "secret/get" && method == "GET":
		if v := os.Getenv(query.Get("name")); v != "" {
			b, _ := json.Marshal(v)
			return b
		}
		return nil

	// --- Lock ---
	case path == "lock/acquire" && method == "POST":
		var body map[string]any
		json.Unmarshal(req.Body, &body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		name, _ := body["name"].(string)
		if _, held := m.locks[name]; held {
			return nil
		}
		m.nextID++
		token := fmt.Sprintf("local-lock-%d", m.nextID)
		m.locks[name] = token
		return marshalJSON(map[string]string{"token": token})

	case path == "lock/release" && method == "POST":
		var body map[string]any
		json.Unmarshal(req.Body, &body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		name, _ := body["name"].(string)
		delete(m.locks, name)
		return nil

	// --- Realtime --- no WS hub in local dev (no slice), so publish/presence
	// are no-ops: zero subscribers. Keeps Channel().Publish() callable under
	// `drift atomic run` without a running slice.
	case path == "realtime/publish" && method == "POST":
		return marshalJSON(map[string]int{"recipients": 0})

	case path == "realtime/presence" && method == "GET":
		return marshalJSON(map[string]int{"present": 0})

	}

	return nil
}

func marshalJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// callBackbone routes backbone requests to the real service (when deployed)
// or the in-memory store (local dev).
func callBackbone(method, path string, body any) ([]byte, error) {
	if url := os.Getenv("BACKBONE_URL"); url != "" {
		return callBackboneHTTP(url, method, path, body)
	}
	return callBackboneLocal(method, path, body)
}

// callBackboneHTTP calls the real backbone service via HTTP over TCP
// localhost. BACKBONE_URL is the full base URL as-is (e.g.
// http://127.0.0.1:8000).
func callBackboneHTTP(baseURL, method, path string, body any) ([]byte, error) {
	url := baseURL + "/" + path

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("drift: marshal backbone body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("drift: backbone request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("drift: backbone call: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("drift: read backbone response: %w", err)
	}
	// A non-2xx response body is an ERROR, not data. Without this check a
	// 401/404/500 body (e.g. an unauthorized secret read) is returned to the
	// caller as if it were the value — so a guard like `if v != ""` passes
	// with the error text. Return an error so callers fail loudly instead.
	if resp.StatusCode >= 400 {
		msg := strings.TrimSpace(string(data))
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return nil, fmt.Errorf("drift: backbone %s: HTTP %d: %s", path, resp.StatusCode, msg)
	}
	if resp.StatusCode == http.StatusNoContent || len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

// callBackboneLocal uses the in-memory store for local dev.
func callBackboneLocal(method, path string, body any) ([]byte, error) {
	var bodyJSON json.RawMessage
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("drift: marshal backbone request body: %w", err)
		}
		bodyJSON = b
	}

	resp := localBackbone.handle(BackboneRequest{
		Method: method,
		Path:   path,
		Body:   bodyJSON,
	})
	if len(resp) == 0 {
		return nil, nil
	}
	return resp, nil
}

// ================================================================
// Namespace API — dot-separated access (drift.Secret.Get, etc.)
// ================================================================

// ================================================================
// Backbone — the B of the sacred A·B·C triad and the single
// entrypoint for every stateful primitive:
//
//	drift.Backbone.Secret / NoSQL / Cache / Queue / Blob / Lock /
//	              Realtime / SQL
//
// Nothing stateful lives at the top level — the triad is the namespace
// for everything under it. (Atomic-side entrypoints like Run live with
// the runtime; cross-slice calling is the seed of a future "D"; identity
// — KeyAuth/JWT/Vault/Link/Pocket — lives under Deed, below, a peer of
// Backbone rather than one of its primitives.)
// ================================================================

type backboneNS struct {
	Secret   secretNS
	NoSQL    nosqlNS
	Cache    cacheNS
	Blob     blobNS
	Lock     lockNS
	Realtime realtimeNS
}

// Backbone is the entrypoint for all Backbone (state) primitives.
var Backbone backboneNS

// Queue returns a handle to the named Backbone message queue.
func (backboneNS) Queue(name string) queueHandle { return queueHandle{name: name} }

// SQL returns a handle to the named Backbone SQLite database (declared in the
// Driftfile under backbone.sql[]).
func (backboneNS) SQL(name string) SQLDB { return SQLDB{name: name} }

// realtimeNS groups the realtime hub. Subscribers connect over WebSocket at the
// Canvas route /realtime/<name>; a function fans messages to them with
// drift.Backbone.Realtime.Channel(name).Publish(msg).
type realtimeNS struct{}

// Channel returns a handle to a realtime channel for server-side publishing.
func (realtimeNS) Channel(name string) RealtimeChannel { return RealtimeChannel{name: name} }

// ================================================================
// Deed — identity, verified. The fourth pillar alongside Atomic /
// Backbone / Canvas — a peer subsystem, not a Backbone primitive, with
// its own loopback listener (DEED_URL) separate from Backbone's
// (BACKBONE_URL). See docs/memos/cyberpunk-shit/
// deed-the-fourth-pillar-drift-identity.md.
//
//	drift.Deed.KeyAuth / JWT / Vault / Link / Pocket
//
// KeyAuth: passwordless Ed25519 device-key auth. JWT: general-purpose
// HS256 sign/verify (KeyAuth mints its own tokens through it). Vault: an
// account-key-wrapped keyring. Link: multi-device attestation /
// enrollment / revocation. Pocket: E2EE per-identity app data, JWT-gated.
//
// Not to be confused with cross-slice calling (drift.Slice(name) /
// CallerSlice, further below) — that's inter-slice networking, a
// different, still-hypothetical future pillar. Deed.Link enrolls another
// DEVICE for the same identity; it has nothing to do with calling
// another SLICE.
// ================================================================

type deedNS struct {
	KeyAuth keyAuthNS
	JWT     jwtNS
	Vault   vaultNS
	Link    linkNS
	Pocket  pocketNS
}

// Deed is the entrypoint for every identity primitive: KeyAuth, JWT,
// Vault, Link, Pocket.
var Deed deedNS

// keyAuthNS / vaultNS / linkNS / pocketNS are defined here so deedNS
// compiles; their methods live in the sections below.
type keyAuthNS struct{}
type vaultNS struct{}
type linkNS struct{}
type pocketNS struct{}

// --- Secret ---

type secretNS struct{}

// Get returns the value of the named secret.
//
// In deployed mode, the runner injects declared secrets as DRIFT_SECRET_<NAME>
// env vars at subprocess start. Get reads from the env first; if the secret
// wasn't declared in the function's `// @atomic-secrets` annotation, the env
// var won't exist. The HTTP fallback to backbone exists only for local-dev
// mode (no DRIFT_RUNTIME) and for back-compat — backbone /secret/get is
// SAT-guarded in production, so an undeclared HTTP call will be rejected.
func (secretNS) Get(name string) (string, error) {
	if v, ok := os.LookupEnv("DRIFT_SECRET_" + strings.ToUpper(name)); ok {
		return v, nil
	}
	resp, err := callBackbone("GET", "secret/get?name="+url.QueryEscape(name), nil)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func (secretNS) Set(name, value string) error {
	_, err := callBackbone("POST", "secret/set", map[string]any{
		"name":  name,
		"value": value,
	})
	return err
}

func (secretNS) Delete(name string) error {
	_, err := callBackbone("DELETE", "secret/delete?name="+url.QueryEscape(name), nil)
	return err
}

// --- Cache ---

type cacheNS struct{}

func (cacheNS) Get(key string) ([]byte, error) {
	return callBackbone("GET", "cache/get?key="+url.QueryEscape(key), nil)
}

func (cacheNS) Set(key string, value any, ttlSeconds int) error {
	payload := map[string]any{
		"key":   key,
		"value": value,
	}
	if ttlSeconds > 0 {
		payload["ttl"] = ttlSeconds
	}
	_, err := callBackbone("POST", "cache/set", payload)
	return err
}

func (cacheNS) Del(key string) error {
	_, err := callBackbone("DELETE", "cache/del?key="+url.QueryEscape(key), nil)
	return err
}

// --- NoSQL ---

type nosqlNS struct{}

type collectionHandle struct{ name string }

func (nosqlNS) Collection(name string) collectionHandle {
	return collectionHandle{name: name}
}

func (c collectionHandle) Insert(doc any) (string, error) {
	payload := map[string]any{
		"collection": c.name,
	}
	if m, ok := doc.(map[string]any); ok {
		maps.Copy(payload, m)
	} else {
		payload["data"] = doc
	}
	resp, err := callBackbone("POST", "write", payload)
	if err != nil {
		return "", err
	}
	var result struct {
		Key string `json:"key"`
	}
	_ = json.Unmarshal(resp, &result)
	return result.Key, nil
}

func (c collectionHandle) Read(key string) (json.RawMessage, error) {
	return callBackbone("GET", "read?collection="+url.QueryEscape(c.name)+"&key="+url.QueryEscape(key), nil)
}

// Get finds the row whose user-facing `_id` equals id via the
// platform's `_id` index. Returns nil if no match.
func (c collectionHandle) Get(id string) (json.RawMessage, error) {
	rows, err := c.List(map[string]string{"_id": id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// Delete removes a single document by storage key.
func (c collectionHandle) Delete(key string) error {
	_, err := callBackbone("POST",
		"nosql/delete?collection="+url.QueryEscape(c.name)+"&key="+url.QueryEscape(key), nil)
	return err
}

func (c collectionHandle) List(filter map[string]string) ([]json.RawMessage, error) {
	path := "nosql/list?collection=" + url.QueryEscape(c.name)
	for k, v := range filter {
		path += "&field=" + url.QueryEscape(k) + "&value=" + url.QueryEscape(v)
	}
	resp, err := callBackbone("GET", path, nil)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return []json.RawMessage{}, nil
	}
	var results []json.RawMessage
	if err := json.Unmarshal(resp, &results); err != nil {
		return nil, fmt.Errorf("drift: parse list response: %w", err)
	}
	return results, nil
}

func (c collectionHandle) Drop() error {
	_, err := callBackbone("POST", "nosql/drop?collection="+url.QueryEscape(c.name), nil)
	return err
}

// --- Queue ---

type queueHandle struct{ name string }

func (q queueHandle) Push(body any) error {
	_, err := callBackbone("POST", "queue/push", map[string]any{
		"queue": q.name,
		"body":  body,
	})
	return err
}

func (q queueHandle) Pop() (json.RawMessage, error) {
	return callBackbone("POST", "queue/pop", map[string]any{
		"queue": q.name,
	})
}

// --- Blob ---

type blobNS struct{}

// splitBucketKey turns a path-shaped name ("submissions/sub-X/file.pdf")
// into (bucket, key) so the platform's bucket+key blob protocol works
// uniformly across SDKs. A bare name with no slash maps to bucket "default".
func splitBucketKey(name string) (string, string) {
	i := strings.IndexByte(name, '/')
	if i < 0 {
		return "default", name
	}
	return name[:i], name[i+1:]
}

// Put stores raw bytes at the given path-shaped name. content_type is
// honoured at upload time (the platform doesn't yet persist it for
// download — see docs/memos/blob-protocol.md). Pass an empty string
// to default to application/octet-stream.
func (blobNS) Put(name string, data []byte, contentType string) error {
	bucket, key := splitBucketKey(name)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	path := "blob/put?bucket=" + url.QueryEscape(bucket) + "&key=" + url.QueryEscape(key)
	return callBackboneRaw("POST", path, data, contentType)
}

func (blobNS) Get(name string) ([]byte, error) {
	bucket, key := splitBucketKey(name)
	return callBackbone("GET", "blob/get?bucket="+url.QueryEscape(bucket)+"&key="+url.QueryEscape(key), nil)
}

// callBackboneRaw posts raw bytes (used by blob.Put). The platform's
// /blob/put expects ?bucket=&key= query params and a binary body, not
// JSON.
func callBackboneRaw(method, path string, data []byte, contentType string) error {
	baseURL := os.Getenv("BACKBONE_URL")
	if baseURL == "" {
		return nil // local dev — silently no-op
	}
	req, err := http.NewRequest(method, baseURL+"/"+path, bytes.NewReader(data)) // #nosec G704 -- BACKBONE_URL is the slice's own backbone over UDS or loopback TCP; never a user-supplied URL.
	if err != nil {
		return fmt.Errorf("drift: backbone raw request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req) // #nosec G704 -- BACKBONE_URL is the slice's own backbone over UDS or loopback TCP; never a user-supplied URL.
	if err != nil {
		return fmt.Errorf("drift: backbone raw call: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drift: backbone raw HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// --- Lock ---

type lockNS struct{}

func (lockNS) Acquire(name string, ttlSeconds int) (string, error) {
	resp, err := callBackbone("POST", "lock/acquire", map[string]any{
		"name": name,
		"ttl":  ttlSeconds,
	})
	if err != nil {
		return "", err
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", fmt.Errorf("drift: parse lock response: %w", err)
	}
	return result.Token, nil
}

func (lockNS) Release(name, token string) error {
	_, err := callBackbone("POST", "lock/release", map[string]any{
		"name":  name,
		"token": token,
	})
	return err
}

// --- Realtime ---

// Realtime has two halves. The CLIENT half is a WebSocket: a browser (or any
// WS client) connects to the Canvas route /realtime/<name> and receives every
// message published to that channel. The SERVER half is this: a function calls
// drift.Backbone.Realtime.Channel(name).Publish(msg) to fan a message out to all
// of those subscribers — the primitive for live, server-pushed UIs (telemetry
// streams, job progress, notifications).

// RealtimeChannel is a server-side realtime channel handle. Construct it with
// drift.Backbone.Realtime.Channel(name).
type RealtimeChannel struct{ name string }

// Publish delivers msg — any JSON-serializable value — to every subscriber
// currently connected to the channel, and returns how many received it. A
// publish to a channel with no subscribers is a no-op that returns 0; callers
// that don't care about delivery can ignore both returns.
func (c RealtimeChannel) Publish(msg any) (int, error) {
	resp, err := callBackbone("POST", "realtime/publish", map[string]any{
		"channel": c.name,
		"message": msg,
	})
	if err != nil {
		return 0, err
	}
	var out struct {
		Recipients int `json:"recipients"`
	}
	if len(resp) > 0 {
		_ = json.Unmarshal(resp, &out)
	}
	return out.Recipients, nil
}

// Presence reports how many subscribers are currently connected to the channel
// (e.g. "N browsers watching this dashboard").
func (c RealtimeChannel) Presence() (int, error) {
	resp, err := callBackbone("GET", "realtime/presence?channel="+url.QueryEscape(c.name), nil)
	if err != nil {
		return 0, err
	}
	var out struct {
		Present int `json:"present"`
	}
	if len(resp) > 0 {
		_ = json.Unmarshal(resp, &out)
	}
	return out.Present, nil
}

// --- KeyAuth (Deed.KeyAuth) ---
//
// Passwordless device-key auth (Ed25519). The client holds a keypair; the uid
// IS its public key — no accounts, no passwords, no email. Challenge mints a
// one-time nonce; the client signs the canonical {domain,nonce,pubkey}; Verify
// checks the signature and issues THIS slice's session JWT (sub = pubkey). The
// slice stores nothing about the user — it just verifies a signature. The
// client half (keygen + signing + recovery-phrase derivation) is a small
// browser library; see the @ondrift/keyauth memo.

// Challenge returns a one-time login nonce for the given Ed25519 public key
// (32-byte hex). Single-use, short-TTL, cache-backed in the slice.
func (keyAuthNS) Challenge(pubkey string) (string, error) {
	resp, err := callDeed("POST", "keyauth/challenge", map[string]any{"pubkey": pubkey})
	if err != nil {
		return "", err
	}
	var out struct {
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", err
	}
	return out.Nonce, nil
}

// Verify checks the client's signature over the canonical {domain,nonce,pubkey}
// and, on success, returns this slice's session JWT (sub = the pubkey). `domain`
// namespaces the signature to your app (e.g. "myapp-auth-v1") so a signature for
// one app/slice can't be replayed at another — the client must sign the same
// domain. A bad/expired/absent challenge or a bad signature returns an error.
func (keyAuthNS) Verify(pubkey, sig, domain string) (string, error) {
	resp, err := callDeed("POST", "keyauth/verify", map[string]any{
		"pubkey": pubkey, "sig": sig, "domain": domain,
	})
	if err != nil {
		return "", err
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", err
	}
	return out.Token, nil
}

// --- Vault (Deed.Vault) ---
//
// Zero-knowledge recovery store: opaque, user-scoped, append-only. The client
// encrypts the blob under a key derived from its recovery phrase (which the
// slice NEVER sees), so Drift stores the backup but cannot read it. Scoped to
// a uid the caller supplies — typically the authenticated KeyAuth pubkey.
// Backed by Deed's own dedicated routes: AES-256-GCM at rest (defense in
// depth only — the blob must already be ciphertext before it arrives, since
// that's the actual source of Vault's confidentiality guarantee) and a
// per-item size quota. No Driftfile declaration needed — replaces the old
// generic-NoSQL-collection implementation entirely.

// Put appends an opaque encrypted backup blob for uid. Append-only (a new
// version each call); Get returns the newest.
func (vaultNS) Put(uid string, blob any) error {
	_, err := callDeed("POST", "deed/vault/put", map[string]any{"uid": uid, "blob": blob})
	return err
}

// Get returns the most recent backup blob for uid. Returns an error if uid
// has never written one (same "not found is an error" convention as
// Secret.Get/Blob.Get).
func (vaultNS) Get(uid string) (json.RawMessage, error) {
	resp, err := callDeed("GET", "deed/vault/get?uid="+url.QueryEscape(uid), nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Blob json.RawMessage `json:"blob"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return nil, err
	}
	return out.Blob, nil
}

// --- Link (Deed.Link) ---
//
// Multi-device continuity: generalizes the enroll/attest/revoke pattern so
// an identity's KeyAuth session can move to a second, third, ... device.
// The signature parameters below (sig, attestingPubkey, etc.) are produced
// entirely client-side — this SDK only forwards them, the same way
// KeyAuth.Verify forwards a signature it never computes itself. The one
// rule the whole design rests on: Deed verifies, it never decides — a
// device is only ever added on the strength of a signature from a device
// already active in the identity's registry.
//
// Not to be confused with cross-slice calling (Slice(name)/CallerSlice,
// further below) — this Link enrolls a DEVICE for one identity, it does
// not call another slice.

// Begin starts a device-linking session for a not-yet-enrolled device's
// pubkey (usually carried in a QR code alongside the pubkey). Returns a
// session ID for an already-active device to present to Attest.
//
// metadata is an optional opaque string (Go has no default arguments, so
// this is variadic — Begin(pubkey) still works unchanged; pass one extra
// string to set it) an attesting device can retrieve via SessionInfo —
// e.g. an ephemeral key it should seal a payload for. Deed never
// interprets it.
func (linkNS) Begin(pubkey string, metadata ...string) (string, error) {
	body := map[string]any{"pubkey": pubkey}
	if len(metadata) > 0 {
		body["metadata"] = metadata[0]
	}
	resp, err := callDeed("POST", "deed/link/begin", body)
	if err != nil {
		return "", err
	}
	var out struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", err
	}
	return out.SessionID, nil
}

// LinkSessionInfo is returned by Link.SessionInfo.
type LinkSessionInfo struct {
	NewPubkey string `json:"new_pubkey"`
	Metadata  string `json:"metadata,omitempty"`
}

// SessionInfo is a read-only, repeatable peek at a pending session an
// attesting device uses after scanning/typing a session ID: NewPubkey is
// required to reconstruct the message Attest signs (the server verifies
// against its own stored value, never the request body), Metadata is
// whatever the joining device passed to Begin.
func (linkNS) SessionInfo(sessionID string) (LinkSessionInfo, error) {
	resp, err := callDeed("POST", "deed/link/session", map[string]any{"session_id": sessionID})
	if err != nil {
		return LinkSessionInfo{}, err
	}
	var out LinkSessionInfo
	if err := json.Unmarshal(resp, &out); err != nil {
		return LinkSessionInfo{}, err
	}
	return out, nil
}

// QR renders text (in practice, a Link session ID) as a scannable QR code,
// returning inline SVG markup. Pure rendering — no session or identity
// involvement, so it works for any short string, not just Link sessions.
func (linkNS) QR(text string) (string, error) {
	resp, err := callDeed("POST", "deed/link/qr", map[string]any{"text": text})
	if err != nil {
		return "", err
	}
	var out struct {
		SVG string `json:"svg"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", err
	}
	return out.SVG, nil
}

// Attest has an already-active device vouch for the session's pending
// device. sig is the client's signature over the canonical
// {domain,identity,new_pubkey} message — computed client-side, never by
// this SDK.
//
// sealed is an optional opaque string (variadic for the same reason as
// Begin's metadata) relayed back once Complete reports "attested" — e.g.
// a payload end-to-end-encrypted for whatever key the joiner published as
// Begin's metadata. Deed only relays it, never opens it.
func (linkNS) Attest(identity, sessionID, attestingPubkey, sig string, sealed ...string) error {
	body := map[string]any{
		"identity": identity, "session_id": sessionID,
		"attesting_pubkey": attestingPubkey, "sig": sig,
	}
	if len(sealed) > 0 {
		body["sealed"] = sealed[0]
	}
	_, err := callDeed("POST", "deed/link/attest", body)
	return err
}

// LinkStatus is returned by Link.Complete. Identity and Sealed are set
// only once Status == "attested" (Sealed only if Attest supplied one).
type LinkStatus struct {
	Status   string `json:"status"`
	Identity string `json:"identity,omitempty"`
	Sealed   string `json:"sealed,omitempty"`
}

// Complete polls a session the new device started with Begin, returning
// whether an active device has attested it yet.
func (linkNS) Complete(sessionID string) (LinkStatus, error) {
	resp, err := callDeed("POST", "deed/link/complete", map[string]any{"session_id": sessionID})
	if err != nil {
		return LinkStatus{}, err
	}
	var out LinkStatus
	if err := json.Unmarshal(resp, &out); err != nil {
		return LinkStatus{}, err
	}
	return out, nil
}

// Revoke deactivates target_pubkey in identity's device registry. Any
// currently-active device may revoke another (or itself); revokingPubkey
// is the device doing the revoking, sig its signature over the canonical
// {domain,identity,target_pubkey} message.
func (linkNS) Revoke(identity, targetPubkey, revokingPubkey, sig string) error {
	_, err := callDeed("POST", "deed/link/revoke", map[string]any{
		"identity": identity, "target_pubkey": targetPubkey,
		"revoking_pubkey": revokingPubkey, "sig": sig,
	})
	return err
}

// --- Pocket (Deed.Pocket) ---
//
// An app's actual data — E2EE, content-keyed, following an identity across
// every device Link has enrolled. The crypto work happens entirely
// client-side before anything reaches this primitive; Pocket never
// encrypts or decrypts the payload itself. Every call takes token
// explicitly (the JWT KeyAuth.Verify returned) rather than holding hidden
// session state — matching the rest of this SDK's stateless posture inside
// an Atomic function invocation. The token's sub is the only identity a
// call can read or write under; there is no way to name a different one.

// Set stores blob under key for whichever identity token resolves to.
func (pocketNS) Set(token, key string, blob any) error {
	_, err := callBackboneAuth("POST", "deed/pocket/set", token, map[string]any{"key": key, "blob": blob})
	return err
}

// Get returns the blob stored under key for token's identity. Returns an
// error if no such key exists.
func (pocketNS) Get(token, key string) (json.RawMessage, error) {
	resp, err := callBackboneAuth("GET", "deed/pocket/get?key="+url.QueryEscape(key), token, nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Blob json.RawMessage `json:"blob"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return nil, err
	}
	return out.Blob, nil
}

// Delete removes key for token's identity. Returns an error if no such key
// exists.
func (pocketNS) Delete(token, key string) error {
	_, err := callBackboneAuth("POST", "deed/pocket/delete", token, map[string]any{"key": key})
	return err
}

// List returns every key stored under token's identity — never another
// identity's, even by guessing.
func (pocketNS) List(token string) ([]string, error) {
	resp, err := callBackboneAuth("GET", "deed/pocket/list", token, nil)
	if err != nil {
		return nil, err
	}
	var out []string
	if err := json.Unmarshal(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// callDeed calls Deed's own listener (DEED_URL) — a separate port from
// Backbone's (BACKBONE_URL) since Deed got its own runtime isolation. No
// local-dev fallback: Deed.Vault and Deed.Link have no in-memory local-dev
// implementation the way every Backbone primitive does, so silently
// no-op'ing here (the way callBackboneLocal would for an unmatched path)
// would report success without ever storing anything. Requiring DEED_URL
// makes that failure honest and immediate instead, matching KeyAuth/JWT's
// existing "not available without a running slice" precedent.
func callDeed(method, path string, body any) ([]byte, error) {
	baseURL := os.Getenv("DEED_URL")
	if baseURL == "" {
		return nil, fmt.Errorf("drift: deed requires a running slice (DEED_URL) — not available in local dev")
	}
	return callBackboneHTTP(baseURL, method, path, body)
}

// callBackboneAuth is callDeed with a bearer token attached — used by
// Deed.Pocket, whose routes are JWT-gated (unlike every other loopback-open
// Backbone/Deed primitive).
func callBackboneAuth(method, path, token string, body any) ([]byte, error) {
	baseURL := os.Getenv("DEED_URL")
	if baseURL == "" {
		return nil, fmt.Errorf("drift: deed pocket requires a running slice (DEED_URL) — not available in local dev")
	}
	return callBackboneHTTPAuth(baseURL, method, path, token, body)
}

// callBackboneHTTPAuth mirrors callBackboneHTTP but attaches an
// Authorization: Bearer header — the one Deed/Backbone call shape that
// needs it.
func callBackboneHTTPAuth(baseURL, method, path, token string, body any) ([]byte, error) {
	url := baseURL + "/" + path

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("drift: marshal backbone body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("drift: backbone request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("drift: backbone call: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("drift: read backbone response: %w", err)
	}
	if resp.StatusCode >= 400 {
		msg := strings.TrimSpace(string(data))
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return nil, fmt.Errorf("drift: backbone %s: HTTP %d: %s", path, resp.StatusCode, msg)
	}
	if resp.StatusCode == http.StatusNoContent || len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

// --- Slice-to-slice linking ---

// Slice returns a client for calling another slice you're LINKED to (e.g. an
// app slice → your own observability slice). Establish the link first with
// `drift slice link add <name>` (same-user). The call travels in-cluster — no
// public hop — and carries this slice's identity so the callee can authorize it
// (see CallerSlice). Returns an error at call time if no link exists.
//
//	resp, err := drift.Slice("c12").Post("/api/events", batch)
func Slice(name string) SliceClient { return SliceClient{name: name} }

// SliceClient calls a linked slice. Construct it with Slice(name).
type SliceClient struct{ name string }

// resolveURL builds the absolute in-cluster URL for path. The platform injects
// the linked slice's internal base URL as DRIFT_LINK_<NAME>_URL when the link
// is created; its absence means "not linked".
func (s SliceClient) resolveURL(path string) (string, error) {
	base := os.Getenv("DRIFT_LINK_" + linkEnvName(s.name) + "_URL")
	if base == "" {
		return "", fmt.Errorf("drift: not linked to slice %q — run `drift slice link add %s`", s.name, s.name)
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimPrefix(path, "/"), nil
}

// Request calls the linked slice with a raw body. The X-Drift-Slice identity
// header is injected automatically; caller headers override it only if they set
// the same key. A non-existent link surfaces as an error before any network I/O.
func (s SliceClient) Request(method, path string, headers map[string]string, body []byte) (*HTTPResponse, error) {
	url, err := s.resolveURL(path)
	if err != nil {
		return nil, err
	}
	h := map[string]string{"X-Drift-Slice": os.Getenv("DRIFT_SLICE")}
	for k, v := range headers {
		h[k] = v
	}
	return HTTPRequest(method, url, h, body)
}

// Get issues a GET to the linked slice.
func (s SliceClient) Get(path string) (*HTTPResponse, error) {
	return s.Request("GET", path, nil, nil)
}

// Post JSON-encodes body and POSTs it to the linked slice.
func (s SliceClient) Post(path string, body any) (*HTTPResponse, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("drift: marshal slice request body: %w", err)
	}
	return s.Request("POST", path, map[string]string{"Content-Type": "application/json"}, b)
}

// CallerSlice returns the name of the linked slice that made this request, or ""
// if the request did not arrive over a slice-to-slice link. Trustworthy within
// the same owner (and, later, the same Team): the NetworkPolicy guarantees the
// only slices that can reach this one are the ones you linked, so the asserted
// identity can only be one of your own slices.
func CallerSlice(req Request) string {
	for k, v := range req.Headers {
		if strings.EqualFold(k, "X-Drift-Slice") {
			return v
		}
	}
	return ""
}

// linkEnvName upper-cases a slice name and replaces every non-alphanumeric byte
// with "_" — mirrors the operator's envSafe so DRIFT_LINK_<NAME>_URL matches.
func linkEnvName(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte(c - 32)
		case (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'):
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// ─── Environment ────────────────────────────────────────────────────────────

// Env returns the value of the environment variable named by the key.
func Env(key string) string {
	return os.Getenv(key)
}

// ─── Logging ─────────────────────────────────────────────────────────────────

// Log writes a message to stderr, which the runner captures as function logs.
func Log(msg string) {
	fmt.Fprintln(os.Stderr, msg)
}

// ─── HTTP client ─────────────────────────────────────────────────────────────

// HTTPResponse holds the status code and body returned by HTTPRequest.
type HTTPResponse struct {
	Status int
	Body   []byte
}

// httpRequestClient is the HTTP client every HTTPRequest call uses.
// 30s default timeout: a function calling a hung remote shouldn't be
// able to hold an Atomic invocation open longer than this. The runner's
// per-invocation deadline is the absolute upper bound, but having a
// client-level timeout means a hung tail dependency fails fast and
// surfaces as a normal error rather than a runner-killed-me 504.
var httpRequestClient = &http.Client{Timeout: 30 * time.Second}

// HTTPRequest makes an outbound HTTP request from within an Atomic function.
// Headers are optional (pass nil to skip). Body is optional (pass nil for
// bodyless methods like GET).
//
// Default timeout is 30 seconds. Use HTTPRequestWithTimeout to override.
func HTTPRequest(method, rawURL string, headers map[string]string, body []byte) (*HTTPResponse, error) {
	return HTTPRequestWithTimeout(method, rawURL, headers, body, 30*time.Second)
}

// HTTPRequestWithTimeout is HTTPRequest with a caller-supplied timeout.
func HTTPRequestWithTimeout(method, rawURL string, headers map[string]string, body []byte, timeout time.Duration) (*HTTPResponse, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, rawURL, reader)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := httpRequestClient
	if timeout != 30*time.Second {
		client = &http.Client{Timeout: timeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		// If the slice is in `egress.mode: allowlist` and the host
		// isn't on the list, the kernel-level NetworkPolicy refuses
		// the connection. The user sees a `dial tcp: connect:
		// connection refused` from the http stack — which is also
		// what they'd see for a host that's allowlisted but
		// genuinely down. Map the policy-refusal case to a
		// structured `EgressDeniedError` so the user can branch on
		// "this is my allowlist's fault" vs "this remote is just
		// broken." Best-effort: the underlying err is preserved
		// via Unwrap().
		if eerr := wrapEgressDenied(rawURL, err); eerr != nil {
			return nil, eerr
		}
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &HTTPResponse{Status: resp.StatusCode, Body: respBody}, nil
}

// EgressDeniedError is returned when the slice's egress NetworkPolicy
// refuses an outbound connection. The Host field is the destination
// the SDK extracted from the URL; the wrapped Err is the original
// transport error so callers can still log or branch on it.
//
// User code can detect the case with `errors.As`:
//
//	var ee *drift.EgressDeniedError
//	if errors.As(err, &ee) {
//	    return 502, "external API not allowlisted",
//	        map[string]string{"host": ee.Host}
//	}
//
// Mapping is best-effort: a host that's *not* private-CIDR but
// happens to be down (genuine DNS or transport failure) will also
// map to `EgressDeniedError{}`. The structured-error path is meant
// for the common "spell-check your allowlist" debugging story, not
// a precise blame attribution.
type EgressDeniedError struct {
	Host string
	Err  error
}

func (e *EgressDeniedError) Error() string {
	return "drift: egress denied for host " + e.Host + " — check your slice.atomic.egress.hosts allowlist (underlying: " + e.Err.Error() + ")"
}

func (e *EgressDeniedError) Unwrap() error { return e.Err }

// wrapEgressDenied returns a non-nil *EgressDeniedError when the
// underlying transport error matches the shape the kernel produces
// for a NetworkPolicy refusal: "connection refused" on a TCP dial.
// Returns nil for everything else (DNS failures, TLS errors,
// timeouts) so the original error continues unchanged.
func wrapEgressDenied(rawURL string, err error) *EgressDeniedError {
	if err == nil {
		return nil
	}
	msg := err.Error()
	// Linux kernel ipset deny manifests as "connection refused"
	// on the dial. We deliberately don't try to be clever about
	// the IP family / cause-chain — a substring match is the
	// honest minimum that will keep working as Go's net error
	// shapes evolve.
	if !strings.Contains(msg, "connection refused") {
		return nil
	}
	host := rawURL
	if u, perr := url.Parse(rawURL); perr == nil && u.Host != "" {
		host = u.Hostname()
	}
	return &EgressDeniedError{Host: host, Err: err}
}

// ─── SSE (Server-Sent Events) ───────────────────────────────────────────────

// RunSSE is the entry point for SSE streaming functions. The handler receives
// the initial request and an Emitter for sending events to the client.
//
// Usage:
//
//	// @atomic http=get:events auth=none stream=sse
//	func main() {
//	    drift.RunSSE(func(req drift.Request, emit drift.Emitter) {
//	        for i := 0; i < 10; i++ {
//	            emit.Send("counter", map[string]int{"value": i})
//	            time.Sleep(time.Second)
//	        }
//	    })
//	}
func RunSSE(handler func(Request, Emitter)) {
	if os.Getenv("DRIFT_RUNTIME") != "" {
		var req Request
		json.NewDecoder(os.Stdin).Decode(&req) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		handler(req, Emitter{w: os.Stdout})
		return
	}
	runLocalSSE(handler)
}

// runLocalSSE serves SSE over HTTP for `drift atomic run` development.
func runLocalSSE(handler func(Request, Emitter)) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		var body json.RawMessage
		if r.Body != nil {
			json.NewDecoder(r.Body).Decode(&body) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.
		}
		headers := make(map[string]string)
		for k := range r.Header {
			headers[k] = r.Header.Get(k)
		}
		req := Request{
			Path:    r.URL.Path,
			Headers: headers,
			Query:   r.URL.RawQuery,
			Body:    body,
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		handler(req, Emitter{w: &flushingWriter{w: w, f: flusher}})
	})

	// #nosec G114 -- false-positive: see audit baseline.
	fmt.Fprintf(os.Stderr, "drift-sdk: local SSE server starting on :%s\n", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil { // #nosec G114 -- false-positive: see the cross-repo audit baseline; this site has been reviewed.
		fmt.Fprintf(os.Stderr, "drift-sdk: server error: %v\n", err)
		os.Exit(1)
	}
}

type flushingWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw *flushingWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	fw.f.Flush()
	return n, err
}

// Emitter writes Server-Sent Events to the client.
type Emitter struct {
	w io.Writer
}

// Send emits an SSE event with the given event name and JSON data.
func (e Emitter) Send(event string, data any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	if event != "" {
		fmt.Fprintf(e.w, "event: %s\n", event)
	}
	fmt.Fprintf(e.w, "data: %s\n\n", jsonData)
}

// SendRaw emits an SSE event with raw string data (not JSON-encoded).
func (e Emitter) SendRaw(event, data string) {
	if event != "" {
		fmt.Fprintf(e.w, "event: %s\n", event)
	}
	fmt.Fprintf(e.w, "data: %s\n\n", data)
}

// ─── WebSocket ──────────────────────────────────────────────────────────────

// RunWS is the entry point for WebSocket functions. The handler receives
// the initial connection request and a Conn for reading/writing messages.
//
// Usage:
//
//	// @atomic http=get:chat auth=none stream=ws
//	func main() {
//	    drift.RunWS(func(req drift.Request, conn drift.Conn) {
//	        for {
//	            msg, ok := conn.Read()
//	            if !ok { break }
//	            conn.Write(map[string]string{"echo": msg})
//	        }
//	    })
//	}
func RunWS(handler func(Request, Conn)) {
	// The runner writes the initial request as the first stdin line.
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}
	var req Request
	json.Unmarshal(scanner.Bytes(), &req) // #nosec G104 -- discarded return is intentional and audited; the call's failure does not affect downstream correctness in this context.

	conn := Conn{
		scanner: scanner,
		w:       os.Stdout,
	}
	handler(req, conn)
}

// Conn represents a WebSocket connection bridged through stdin/stdout.
// The runner handles the actual WebSocket protocol; the function sees
// JSON lines on stdin (incoming) and writes JSON lines to stdout (outgoing).
type Conn struct {
	scanner *bufio.Scanner
	w       io.Writer
}

// Read blocks until the next message arrives from the client.
// Returns the raw message bytes and true, or nil and false if the
// connection is closed.
func (c Conn) Read() (json.RawMessage, bool) {
	if !c.scanner.Scan() {
		return nil, false
	}
	return json.RawMessage(c.scanner.Bytes()), true
}

// ReadJSON reads the next message and decodes it into the target.
func (c Conn) ReadJSON(target any) bool {
	msg, ok := c.Read()
	if !ok {
		return false
	}
	return json.Unmarshal(msg, target) == nil
}

// Write sends a message to the client. The value is JSON-encoded.
func (c Conn) Write(data any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	fmt.Fprintf(c.w, "%s\n", jsonData)
}

// WriteRaw sends a raw string message to the client.
func (c Conn) WriteRaw(msg string) {
	fmt.Fprintf(c.w, "%s\n", msg)
}

// ─── JWT primitive (Deed.JWT) ─────────────────────────────────────────────────
//
// HS256 JWT minting + verification, signed with the slice's per-slice JKey.
// The signing key never leaves the slice's backbone process; all operations
// flow through loopback HTTP to /jwt/{sign,verify,slice-id}. General-purpose
// on its own, but squarely part of Deed — KeyAuth mints its tokens through
// it, and Pocket verifies them.
//
// Design: docs/memos/slice-jwt-primitive.md.

// JWTClaims is the claims payload accepted by JWT.Issue and returned by
// JWT.Verify. Standard fields (Sub/Iat/Exp/Nbf/Iss/Aud/Jti) follow RFC 7519;
// Custom carries app-defined claims that the platform never inspects.
//
// On Issue: Iat, Iss, and Jti are auto-set when zero. Exp is required.
// On Verify: every standard field round-trips; Custom contains whatever the
// signer put in it.
type JWTClaims struct {
	Sub    string         `json:"sub,omitempty"`
	Iat    int64          `json:"iat,omitempty"`
	Exp    int64          `json:"exp,omitempty"`
	Nbf    int64          `json:"nbf,omitempty"`
	Iss    string         `json:"iss,omitempty"`
	Aud    []string       `json:"aud,omitempty"`
	Jti    string         `json:"jti,omitempty"`
	Custom map[string]any `json:"custom,omitempty"`
}

// JWTVerifyOptions tunes JWT.Verify. Zero values use platform defaults:
//   - Audience: unchecked.
//   - AllowedIssuer: empty → backbone enforces "iss must equal this slice".
type JWTVerifyOptions struct {
	Audience      string
	AllowedIssuer string
}

// JWTError is returned by JWT.Verify when validation fails. Reason is one
// of the stable wire strings: "malformed", "bad_signature", "expired",
// "not_yet_valid", "wrong_algorithm", "wrong_issuer", "wrong_audience",
// "invalid_claims", "missing_exp", "internal_error".
type JWTError struct {
	Reason string
}

func (e *JWTError) Error() string {
	return "jwt verify: " + e.Reason
}

type jwtNS struct{}

// Issue signs a JWT with the slice's HS256 JKey and returns the encoded
// token. Iat/Iss/Jti are auto-populated when zero; Exp is required (Issue
// returns an error if it's missing or in the past).
func (jwtNS) Issue(claims JWTClaims) (string, error) {
	body := map[string]any{}
	if claims.Sub != "" {
		body["sub"] = claims.Sub
	}
	if claims.Iat != 0 {
		body["iat"] = claims.Iat
	}
	if claims.Exp != 0 {
		body["exp"] = claims.Exp
	}
	if claims.Nbf != 0 {
		body["nbf"] = claims.Nbf
	}
	if claims.Iss != "" {
		body["iss"] = claims.Iss
	}
	if len(claims.Aud) > 0 {
		body["aud"] = claims.Aud
	}
	if claims.Jti != "" {
		body["jti"] = claims.Jti
	}
	if len(claims.Custom) > 0 {
		body["custom"] = claims.Custom
	}
	resp, err := callDeed("POST", "jwt/sign", body)
	if err != nil {
		return "", err
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", err
	}
	return out.Token, nil
}

// Verify validates a token and returns the parsed claims. On any
// validation failure (bad signature, expired, wrong issuer, etc.) it
// returns a *JWTError whose Reason is a stable wire string.
func (jwtNS) Verify(token string, opts JWTVerifyOptions) (JWTClaims, error) {
	body := map[string]any{"token": token}
	if opts.Audience != "" {
		body["audience"] = opts.Audience
	}
	if opts.AllowedIssuer != "" {
		body["allowed_issuer"] = opts.AllowedIssuer
	}
	resp, err := callDeed("POST", "jwt/verify", body)
	if err != nil {
		return JWTClaims{}, err
	}
	var out struct {
		Valid  bool                   `json:"valid"`
		Reason string                 `json:"reason,omitempty"`
		Claims map[string]interface{} `json:"claims,omitempty"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return JWTClaims{}, err
	}
	if !out.Valid {
		return JWTClaims{}, &JWTError{Reason: out.Reason}
	}
	c := JWTClaims{}
	if v, ok := out.Claims["sub"].(string); ok {
		c.Sub = v
	}
	if v, ok := out.Claims["iss"].(string); ok {
		c.Iss = v
	}
	if v, ok := out.Claims["jti"].(string); ok {
		c.Jti = v
	}
	if v, ok := out.Claims["iat"].(float64); ok {
		c.Iat = int64(v)
	}
	if v, ok := out.Claims["exp"].(float64); ok {
		c.Exp = int64(v)
	}
	if v, ok := out.Claims["nbf"].(float64); ok {
		c.Nbf = int64(v)
	}
	if rawAud, ok := out.Claims["aud"]; ok {
		switch arr := rawAud.(type) {
		case []interface{}:
			for _, a := range arr {
				if s, ok := a.(string); ok {
					c.Aud = append(c.Aud, s)
				}
			}
		case string:
			c.Aud = []string{arr}
		}
	}
	if rawCustom, ok := out.Claims["custom"].(map[string]interface{}); ok {
		c.Custom = rawCustom
	}
	return c, nil
}

// SliceID returns the slice's auto-set issuer string
// ("drift-slice-<user>-<slice>"). Useful for logging and audit; not
// usually needed in app code because Verify defaults to checking that
// `iss` matches this value automatically.
func (jwtNS) SliceID() string {
	resp, err := callDeed("GET", "jwt/slice-id", nil)
	if err != nil {
		return ""
	}
	var out struct {
		SliceID string `json:"slice_id"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return ""
	}
	return out.SliceID
}

// ─── SQL ────────────────────────────────────────────────────────────────────
//
// Per-slice SQLite databases addressed by name. The wire shape is one
// JSON envelope per call (`{db, sql, args, tx?}`) — same posture as
// every other Backbone primitive. See docs/memos/done/backbone-sql.md.
//
//   db := drift.SQL("clinic")
//   rows, err := db.Query("SELECT * FROM appointments WHERE slot >= ?", "2026-05-01")
//   res, err := db.Execute("INSERT INTO appointments(...) VALUES(?, ?)", "alice", "10:00")
//   tx, err := db.Begin()
//   ...
//   tx.Commit()
//
// `Query` returns `[]map[string]any` for ergonomic JSON-shaped reads.
// `Execute` returns `(rowsAffected, lastInsertID)`. `Begin/Commit/Rollback`
// thread through an opaque transaction token issued by the slice.

// SQLDB is the per-database handle returned by SQL().
type SQLDB struct {
	name string
}

// SQLResult is the shape returned by Execute.
type SQLResult struct {
	RowsAffected int64 `json:"rows_affected"`
	LastInsertID int64 `json:"last_insert_id"`
}

type sqlQueryReply struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// Query runs a SELECT and returns rows shaped as `[]map[string]any`
// keyed by column name. SQLite's NULL is JSON null; other types pass
// through unchanged.
func (s SQLDB) Query(sqlText string, args ...any) ([]map[string]any, error) {
	body := map[string]any{"db": s.name, "sql": sqlText, "args": args}
	resp, err := callBackbone("POST", "sql/query", body)
	if err != nil {
		return nil, err
	}
	var rep sqlQueryReply
	if err := json.Unmarshal(resp, &rep); err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(rep.Rows))
	for _, r := range rep.Rows {
		row := make(map[string]any, len(rep.Columns))
		for i, col := range rep.Columns {
			if i < len(r) {
				row[col] = r[i]
			}
		}
		out = append(out, row)
	}
	return out, nil
}

// Execute runs INSERT/UPDATE/DELETE/DDL.
func (s SQLDB) Execute(sqlText string, args ...any) (SQLResult, error) {
	body := map[string]any{"db": s.name, "sql": sqlText, "args": args}
	resp, err := callBackbone("POST", "sql/execute", body)
	if err != nil {
		return SQLResult{}, err
	}
	var out SQLResult
	if err := json.Unmarshal(resp, &out); err != nil {
		return SQLResult{}, err
	}
	return out, nil
}

// SQLTx is a handle to an open transaction. Commit() or Rollback()
// must be called; the slice's idle-tx janitor will roll back any
// transaction left open for >30s.
type SQLTx struct {
	db    string
	token string
}

// Begin starts a transaction.
func (s SQLDB) Begin() (SQLTx, error) {
	resp, err := callBackbone("POST", "sql/begin", map[string]any{"db": s.name})
	if err != nil {
		return SQLTx{}, err
	}
	var out struct {
		Tx string `json:"tx"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return SQLTx{}, err
	}
	return SQLTx{db: s.name, token: out.Tx}, nil
}

// Query inside a transaction.
func (t SQLTx) Query(sqlText string, args ...any) ([]map[string]any, error) {
	body := map[string]any{"db": t.db, "sql": sqlText, "args": args, "tx": t.token}
	resp, err := callBackbone("POST", "sql/query", body)
	if err != nil {
		return nil, err
	}
	var rep sqlQueryReply
	if err := json.Unmarshal(resp, &rep); err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(rep.Rows))
	for _, r := range rep.Rows {
		row := make(map[string]any, len(rep.Columns))
		for i, col := range rep.Columns {
			if i < len(r) {
				row[col] = r[i]
			}
		}
		out = append(out, row)
	}
	return out, nil
}

// Execute inside a transaction.
func (t SQLTx) Execute(sqlText string, args ...any) (SQLResult, error) {
	body := map[string]any{"db": t.db, "sql": sqlText, "args": args, "tx": t.token}
	resp, err := callBackbone("POST", "sql/execute", body)
	if err != nil {
		return SQLResult{}, err
	}
	var out SQLResult
	if err := json.Unmarshal(resp, &out); err != nil {
		return SQLResult{}, err
	}
	return out, nil
}

// Commit closes the transaction durably.
func (t SQLTx) Commit() error {
	_, err := callBackbone("POST", "sql/commit", map[string]any{"tx": t.token})
	return err
}

// Rollback closes the transaction discarding its writes.
func (t SQLTx) Rollback() error {
	_, err := callBackbone("POST", "sql/rollback", map[string]any{"tx": t.token})
	return err
}
