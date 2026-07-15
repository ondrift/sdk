<?php
/**
 * Drift SDK for PHP Atomic functions.
 *
 * This single-file SDK provides:
 *   - \Drift\run($handler): Entry point that dispatches to deployed or local mode.
 *   - \Drift\Backbone\* — the B of the sacred A·B·C triad; the SOLE entrypoint
 *     for every STATE primitive: Secret, Cache, Nosql, queue, Blob, Lock,
 *     sql, Realtime. (There is no \Drift\Secret etc. — go through the
 *     \Drift\Backbone namespace.)
 *   - \Drift\Deed\* — the 4th pillar, identity, verified: KeyAuth, JWT,
 *     Vault, Link, Pocket. A peer of \Drift\Backbone, not one of its
 *     primitives.
 *   - \Drift\log($msg): Writes to stderr (captured by the runner as function logs).
 *   - \Drift\http_request(): Outbound HTTP from within a function.
 *   - \Drift\slice($name) / \Drift\caller_slice($req): slice-to-slice linking
 *     (unrelated to \Drift\Deed\Link, which enrolls a DEVICE for one
 *     identity — not the same thing as calling another SLICE).
 *
 * All backbone helpers use only PHP built-ins -- zero external dependencies.
 */

namespace Drift {

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

function run(callable $handler): void {
    if (getenv('DRIFT_RUNTIME')) {
        _run_deployed($handler);
    } else {
        _run_local($handler);
    }
}

function _run_deployed(callable $handler): void {
    $input = file_get_contents('php://stdin');
    $req = json_decode($input, true);
    if (is_array($req) && isset($req['query']) && is_string($req['query'])) {
        $parsed = [];
        parse_str($req['query'], $parsed);
        $req['query'] = $parsed;
    }
    $resp = $handler($req);
    fwrite(STDOUT, json_encode($resp));
}

function _run_local(callable $handler): void {
    $port = (int)(getenv('PORT') ?: '8080');
    $server = stream_socket_server("tcp://0.0.0.0:$port", $errno, $errstr);
    if (!$server) {
        fwrite(STDERR, "drift-sdk: failed to start server: $errstr ($errno)\n");
        exit(1);
    }
    fwrite(STDERR, "drift-sdk: local server starting on :$port\n");

    while ($client = @stream_socket_accept($server, -1)) {
        try {
            $request_line = fgets($client);
            if (!$request_line) { fclose($client); continue; }
            $parts = explode(' ', trim($request_line));
            if (count($parts) < 2) { fclose($client); continue; }
            $method = $parts[0];
            $path_str = $parts[1];

            $headers = [];
            while (($line = fgets($client)) && trim($line) !== '') {
                $pair = explode(': ', trim($line), 2);
                if (count($pair) === 2) {
                    $headers[strtolower($pair[0])] = $pair[1];
                }
            }

            $body = null;
            if (isset($headers['content-length'])) {
                $raw = fread($client, (int)$headers['content-length']);
                $decoded = json_decode($raw, true);
                $body = ($decoded !== null) ? $decoded : $raw;
            }

            $parsed = parse_url($path_str);
            $req = [
                'path' => $parsed['path'] ?? '/',
                'headers' => $headers,
                'query' => $parsed['query'] ?? '',
                'body' => $body,
            ];

            $resp = $handler($req);
            $status = $resp['status'] ?? 200;
            $out = json_encode($resp);

            $response = "HTTP/1.1 $status OK\r\n";
            $response .= "Content-Type: application/json\r\n";
            $response .= "Content-Length: " . strlen($out) . "\r\n";
            $response .= "\r\n";
            $response .= $out;

            fwrite($client, $response);
        } catch (\Throwable $e) {
            fwrite(STDERR, "drift-sdk: {$e->getMessage()}\n");
        } finally {
            @fclose($client);
        }
    }
}

// ---------------------------------------------------------------------------
// Backbone transport
// ---------------------------------------------------------------------------

$_backbone_url = null;

function _get_backbone_url(): string {
    global $_backbone_url;
    if ($_backbone_url === null) {
        $_backbone_url = getenv('BACKBONE_URL') ?: '';
    }
    return $_backbone_url;
}

$_deed_url = null;

// Deed has its own listener/port now (DEED_URL), separate from Backbone's
// — see _call_deed below.
function _get_deed_url(): string {
    global $_deed_url;
    if ($_deed_url === null) {
        $_deed_url = getenv('DEED_URL') ?: '';
    }
    return $_deed_url;
}

// _backbone_http issues a request over TCP (file_get_contents opens a fresh
// connection per call, so no persistent-connection thrashing concern
// between different hosts/ports). Returns [status_code, body]. Stdlib only —
// the http stream wrapper. $extra_headers is a one-off escape hatch for
// Drift\Deed\Pocket, the one call shape in this file that needs a bearer
// token attached (every other Backbone/Deed primitive is loopback-open); it
// defaults to [] so every existing call site is unaffected. $base defaults
// to Backbone's URL; _call_deed passes Deed's own URL instead, since Deed
// lives on a separate port.
function _backbone_http(string $method, string $path, ?string $body, string $content_type, array $extra_headers = [], ?string $base = null): array {
    $base = $base ?? _get_backbone_url();
    // TLS verification is explicit even though Backbone is loopback in
    // production — keeps the policy uniform with http_request().
    $opts = [
        'http' => ['method' => $method, 'ignore_errors' => true],
        'ssl'  => ['verify_peer' => true, 'verify_peer_name' => true],
    ];
    $header_str = '';
    if ($body !== null) {
        $header_str .= "Content-Type: $content_type\r\n";
    }
    foreach ($extra_headers as $name => $value) {
        $header_str .= "$name: $value\r\n";
    }
    if ($header_str !== '') {
        $opts['http']['header'] = $header_str;
    }
    if ($body !== null) {
        $opts['http']['content'] = $body;
    }
    $result = @file_get_contents("$base/$path", false, stream_context_create($opts));
    $status = 0;
    if (isset($http_response_header[0]) && preg_match('#^HTTP/\S+\s+(\d+)#', $http_response_header[0], $m)) {
        $status = (int) $m[1];
    }
    return [$status, $result === false ? null : $result];
}

function _call(string $method, string $path, $body = null) {
    if (_get_backbone_url() === '') return _call_local($method, $path, $body);
    [$status, $result] = _backbone_http($method, $path, $body !== null ? json_encode($body) : null, 'application/json');
    if ($status === 204 || $result === null || $result === '') return null;
    $decoded = json_decode($result, true);
    return ($decoded !== null) ? $decoded : $result;
}

function _call_raw(string $method, string $path, string $data_bytes, string $content_type = 'application/octet-stream'): ?string {
    if (_get_backbone_url() === '') return null;
    [$status, $result] = _backbone_http($method, $path, $data_bytes, $content_type);
    return ($status >= 200 && $status < 300) ? $result : null;
}

// In-memory backbone for local dev.
$_local_store = [
    'nosql' => [], 'cache' => [], 'queues' => [],
    'blobs' => [], 'locks' => [], 'next_id' => 0,
];

function _call_local(string $method, string $path, $body = null) {
    global $_local_store;
    $s = &$_local_store;

    $parts = explode('?', $path, 2);
    $base_path = $parts[0];
    $query = [];
    if (isset($parts[1])) parse_str($parts[1], $query);

    // NoSQL
    if ($base_path === 'write' && $method === 'POST') {
        $col = ($body ?? [])['collection'] ?? 'default';
        if (!isset($s['nosql'][$col])) $s['nosql'][$col] = [];
        $s['next_id']++;
        $key = (string)$s['next_id'];
        $s['nosql'][$col][$key] = $body;
        return ['key' => $key];
    }
    if ($base_path === 'read' && $method === 'GET') {
        $col = $query['collection'] ?? 'default';
        return ($s['nosql'][$col] ?? [])[$query['key'] ?? ''] ?? null;
    }
    if ($base_path === 'nosql/list' && $method === 'GET') {
        $col = $query['collection'] ?? 'default';
        $docs = $s['nosql'][$col] ?? [];
        $field = $query['field'] ?? null;
        $value = $query['value'] ?? null;
        $results = [];
        foreach ($docs as $doc) {
            if ($field !== null && (string)($doc[$field] ?? '') !== $value) continue;
            $results[] = $doc;
        }
        return $results;
    }
    if ($base_path === 'nosql/drop' && $method === 'POST') {
        unset($s['nosql'][$query['collection'] ?? 'default']);
        return null;
    }

    // Cache
    if ($base_path === 'cache/set' && $method === 'POST') {
        $s['cache'][($body ?? [])['key'] ?? ''] = ($body ?? [])['value'] ?? null;
        return null;
    }
    if ($base_path === 'cache/get' && $method === 'GET') {
        return $s['cache'][$query['key'] ?? ''] ?? null;
    }
    if ($base_path === 'cache/del') {
        unset($s['cache'][$query['key'] ?? '']);
        return null;
    }

    // Queue
    if ($base_path === 'queue/push' && $method === 'POST') {
        $name = ($body ?? [])['queue'] ?? '';
        if (!isset($s['queues'][$name])) $s['queues'][$name] = [];
        $s['queues'][$name][] = ($body ?? [])['body'] ?? null;
        return null;
    }
    if ($base_path === 'queue/pop' && $method === 'POST') {
        $name = ($body ?? [])['queue'] ?? '';
        if (empty($s['queues'][$name])) return null;
        return array_shift($s['queues'][$name]);
    }

    // Blob
    if ($base_path === 'blob/put' && $method === 'POST') {
        $s['blobs'][($body ?? [])['name'] ?? ''] = ($body ?? [])['data'] ?? null;
        return null;
    }
    if ($base_path === 'blob/get' && $method === 'GET') {
        return $s['blobs'][$query['name'] ?? ''] ?? null;
    }

    // Secret — in local dev, read from environment variables (loaded from .env by the CLI)
    if ($base_path === 'secret/get' && $method === 'GET') {
        $name = $query['name'] ?? '';
        $val = getenv($name);
        return $val !== false ? $val : null;
    }

    // Lock
    if ($base_path === 'lock/acquire' && $method === 'POST') {
        $name = ($body ?? [])['name'] ?? '';
        if (isset($s['locks'][$name])) return null;
        $s['next_id']++;
        $token = "local-lock-{$s['next_id']}";
        $s['locks'][$name] = $token;
        return ['token' => $token];
    }
    if ($base_path === 'lock/release' && $method === 'POST') {
        unset($s['locks'][($body ?? [])['name'] ?? '']);
        return null;
    }

    return null;
}

// ---------------------------------------------------------------------------
// JWT error type
// ---------------------------------------------------------------------------

/**
 * Thrown by Drift\Deed\JWT::verify on validation failure. ``reason`` is one
 * of the stable wire strings: malformed, bad_signature, expired,
 * not_yet_valid, wrong_algorithm, wrong_issuer, wrong_audience,
 * invalid_claims, missing_exp, internal_error. Kept at the top level (it is an
 * error type, not a state entrypoint — same reasoning that keeps it out of
 * \Drift\Deed too, even though JWT itself moved there).
 */
class JWTError extends \Exception {
    public string $reason;
    public function __construct(string $reason) {
        parent::__construct("jwt verify: $reason");
        $this->reason = $reason;
    }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log($msg): void {
    fwrite(STDERR, strval($msg) . "\n");
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

// Default 30s timeout. A function calling a hung remote shouldn't
// hold an Atomic invocation open longer than this; the runner's
// per-invocation deadline is the absolute ceiling.
//
// TLS verification (`verify_peer` + `verify_peer_name`) is set
// explicitly even though both default to true on PHP 5.6+ — making
// the intent visible to anyone reading this for security review.
function http_request(string $method, string $url, ?array $headers = null, $body = null, int $timeout = 30): array {
    $opts = [
        'http' => [
            'method'        => $method,
            'ignore_errors' => true,
            'timeout'       => $timeout,
        ],
        'ssl' => [
            'verify_peer'      => true,
            'verify_peer_name' => true,
        ],
    ];

    $header_str = '';
    if ($headers) {
        foreach ($headers as $k => $v) {
            $header_str .= "$k: $v\r\n";
        }
    }
    if ($body !== null) {
        $opts['http']['content'] = is_string($body) ? $body : json_encode($body);
    }
    if ($header_str) $opts['http']['header'] = $header_str;

    $context = stream_context_create($opts);
    $result = @file_get_contents($url, false, $context);

    $status = 0;
    if (isset($http_response_header[0]) && preg_match('/^HTTP\/\S+ (\d+)/', $http_response_header[0], $m)) {
        $status = (int)$m[1];
    }

    return ['status' => $status, 'body' => $result ?: ''];
}

// ─── SSE ────────────────────────────────────────────────────────────────────

function run_sse(callable $handler): void {
    if (getenv('DRIFT_RUNTIME')) {
        $input = file_get_contents('php://stdin');
        $req = json_decode($input, true) ?? [];
        $emit = function(string $event, $data) {
            if ($event) fwrite(STDOUT, "event: $event\n");
            fwrite(STDOUT, "data: " . json_encode($data) . "\n\n");
            fflush(STDOUT);
        };
        $handler($req, $emit);
        return;
    }
    _run_local_sse($handler);
}

// Local-dev SSE server. Mirrors `_run_local`'s socket plumbing but writes the
// HTTP response chunks incrementally and flushes after every emit so each
// SSE event reaches the client immediately. PHP's output buffering is the
// usual cause of "events arrive in one burst at the end" — fflush() defeats
// it on the per-connection socket level.
function _run_local_sse(callable $handler): void {
    $port = (int)(getenv('PORT') ?: '8080');
    $server = stream_socket_server("tcp://0.0.0.0:$port", $errno, $errstr);
    if (!$server) {
        fwrite(STDERR, "drift-sdk: failed to start SSE server: $errstr ($errno)\n");
        exit(1);
    }
    fwrite(STDERR, "drift-sdk: local SSE server starting on :$port\n");

    while ($client = @stream_socket_accept($server, -1)) {
        try {
            $request_line = fgets($client);
            if (!$request_line) { fclose($client); continue; }
            $parts = explode(' ', trim($request_line));
            if (count($parts) < 2) { fclose($client); continue; }
            $method = $parts[0];
            $path_str = $parts[1];

            $headers = [];
            while (($line = fgets($client)) && trim($line) !== '') {
                $pair = explode(': ', trim($line), 2);
                if (count($pair) === 2) {
                    $headers[strtolower($pair[0])] = $pair[1];
                }
            }

            $body = null;
            if (isset($headers['content-length'])) {
                $raw = fread($client, (int)$headers['content-length']);
                $decoded = json_decode($raw, true);
                $body = ($decoded !== null) ? $decoded : $raw;
            }

            $parsed = parse_url($path_str);
            $req = [
                'path' => $parsed['path'] ?? '/',
                'headers' => $headers,
                'query' => $parsed['query'] ?? '',
                'body' => $body,
            ];

            fwrite($client,
                "HTTP/1.1 200 OK\r\n" .
                "Content-Type: text/event-stream\r\n" .
                "Cache-Control: no-cache, no-transform\r\n" .
                "Connection: keep-alive\r\n" .
                "X-Accel-Buffering: no\r\n" .
                "\r\n"
            );
            fflush($client);

            $emit = function(string $event, $data) use ($client) {
                if ($event) fwrite($client, "event: $event\n");
                fwrite($client, "data: " . json_encode($data) . "\n\n");
                fflush($client);
            };

            try {
                $handler($req, $emit);
            } catch (\Throwable $e) {
                @fwrite($client, "event: error\ndata: " . json_encode(['error' => $e->getMessage()]) . "\n\n");
            }
        } catch (\Throwable $e) {
            fwrite(STDERR, "drift-sdk: {$e->getMessage()}\n");
        } finally {
            @fclose($client);
        }
    }
}

// ─── WebSocket ──────────────────────────────────────────────────────────────

class WsConn {
    public function read() {
        $line = fgets(STDIN);
        if ($line === false) return null;
        $line = trim($line);
        if ($line === '') return null;
        $decoded = json_decode($line, true);
        return ($decoded !== null) ? $decoded : $line;
    }

    public function write($data): void {
        fwrite(STDOUT, json_encode($data) . "\n");
        fflush(STDOUT);
    }

    public function write_raw(string $msg): void {
        fwrite(STDOUT, $msg . "\n");
        fflush(STDOUT);
    }
}

function run_ws(callable $handler): void {
    if (getenv('DRIFT_RUNTIME')) {
        $firstLine = fgets(STDIN);
        $req = $firstLine ? json_decode(trim($firstLine), true) : [];
        $conn = new WsConn();
        $handler($req, $conn);
    }
}

// ---------------------------------------------------------------------------
// Slice-to-slice linking (top-level; the seed of a future "D" pillar)
// ---------------------------------------------------------------------------
//
// Not Backbone — this is inter-slice networking, parked at the top level until
// the fourth pillar lands.

function _link_env_name(string $name): string {
    return 'DRIFT_LINK_' . preg_replace('/[^A-Z0-9]/', '_', strtoupper($name)) . '_URL';
}

class SliceClient {
    private string $name;

    public function __construct(string $name) { $this->name = $name; }

    private function _url(string $path): string {
        $base = getenv(_link_env_name($this->name));
        if ($base === false || $base === '') {
            throw new \RuntimeException(
                "drift: not linked to slice \"{$this->name}\" — run `drift slice link add {$this->name}`"
            );
        }
        return rtrim($base, '/') . '/' . ltrim($path, '/');
    }

    public function request(string $method, string $path, ?array $headers = null, $body = null): array {
        $h = array_merge(['X-Drift-Slice' => getenv('DRIFT_SLICE') ?: ''], $headers ?? []);
        return http_request($method, $this->_url($path), $h, $body);
    }

    public function get(string $path): array {
        return $this->request('GET', $path);
    }

    public function post(string $path, $body = null): array {
        return $this->request('POST', $path,
            ['Content-Type' => 'application/json'],
            $body === null ? null : json_encode($body));
    }
}

/**
 * A client for another slice you've LINKED to (`drift slice link`). The call
 * travels in-cluster and carries this slice's identity (X-Drift-Slice).
 */
function slice(string $name): SliceClient {
    return new SliceClient($name);
}

/** The linked slice that called this request, or "" if not via a link. */
function caller_slice(array $req): string {
    $headers = $req['headers'] ?? [];
    foreach ($headers as $k => $v) {
        if (strtolower($k) === 'x-drift-slice') return $v;
    }
    return '';
}

/** An environment variable value ("" if unset). */
function env(string $key): string {
    $v = getenv($key);
    return $v !== false ? $v : '';
}

} // namespace Drift

// ===========================================================================
// Backbone — the B of the sacred A·B·C triad. The SOLE entrypoint for every
// state primitive. Reach these as \Drift\Backbone\Secret::get(...),
// \Drift\Backbone\queue(...), \Drift\Backbone\Realtime::channel(...), etc.
// Nothing stateful lives in the top-level \Drift namespace.
//
// Identity — KeyAuth, JWT, Vault, Link, Pocket — lives under \Drift\Deed,
// a peer of Backbone rather than one of its primitives; see the
// \Drift\Deed namespace at the end of this file.
// ===========================================================================

namespace Drift\Backbone {

use function Drift\_call;
use function Drift\_call_raw;
use function Drift\_backbone_http;
use function Drift\_get_backbone_url;

// ---------------------------------------------------------------------------
// Secret
// ---------------------------------------------------------------------------

class Secret {
    /**
     * Read order:
     *   1. DRIFT_SECRET_<NAME> env var — set by the runner from the
     *      function's @atomic-secrets allowlist. Only path that works
     *      in production: backbone /secret/get is SAT-guarded and the
     *      subprocess does not hold the SAT.
     *   2. HTTP fallback — local-dev only. In production, returns 401.
     */
    public static function get(string $name): string {
        $env_val = getenv('DRIFT_SECRET_' . strtoupper($name));
        if ($env_val !== false) {
            return $env_val;
        }
        $resp = _call('GET', 'secret/get?name=' . urlencode($name));
        return is_string($resp) ? $resp : ($resp !== null ? json_encode($resp) : '');
    }

    public static function set(string $name, string $value): void {
        _call('POST', 'secret/set', ['name' => $name, 'value' => $value]);
    }

    public static function delete(string $name): void {
        _call('DELETE', 'secret/delete?name=' . urlencode($name));
    }
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

class Cache {
    public static function get(string $key) {
        return _call('GET', 'cache/get?key=' . urlencode($key));
    }

    public static function set(string $key, $value, int $ttl): void {
        $payload = ['key' => $key, 'value' => $value];
        if ($ttl > 0) $payload['ttl'] = $ttl;
        _call('POST', 'cache/set', $payload);
    }

    public static function delete(string $key): void {
        _call('DELETE', 'cache/del?key=' . urlencode($key));
    }
}

// ---------------------------------------------------------------------------
// NoSQL
// ---------------------------------------------------------------------------

class Nosql {
    public static function collection(string $name): NosqlCollection {
        return new NosqlCollection($name);
    }
}

class NosqlCollection {
    private string $name;

    public function __construct(string $name) { $this->name = $name; }

    public function insert($doc): string {
        $payload = ['collection' => $this->name];
        if (is_array($doc)) {
            $payload = array_merge($payload, $doc);
        } else {
            $payload['data'] = $doc;
        }
        $resp = _call('POST', 'write', $payload);
        return is_array($resp) ? ($resp['key'] ?? '') : '';
    }

    public function read(string $key) {
        return _call('GET', 'read?collection=' . urlencode($this->name) . '&key=' . urlencode($key));
    }

    public function get(string $id) {
        $path = 'nosql/list?collection=' . urlencode($this->name) . '&field=_id&value=' . urlencode($id);
        $resp = _call('GET', $path);
        if (is_array($resp) && !empty($resp)) return $resp[0];
        return null;
    }

    public function delete(string $key) {
        return _call('POST', 'nosql/delete?collection=' . urlencode($this->name) . '&key=' . urlencode($key));
    }

    public function list(?array $filter = null): array {
        $path = 'nosql/list?collection=' . urlencode($this->name);
        if ($filter) {
            foreach ($filter as $k => $v) {
                $path .= '&field=' . urlencode($k) . '&value=' . urlencode($v);
            }
        }
        $resp = _call('GET', $path);
        return is_array($resp) ? $resp : [];
    }

    public function drop(): void {
        _call('POST', 'nosql/drop?collection=' . urlencode($this->name));
    }
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

function queue(string $name): QueueHandle {
    return new QueueHandle($name);
}

class QueueHandle {
    private string $name;

    public function __construct(string $name) { $this->name = $name; }

    public function push($body): void {
        _call('POST', 'queue/push', ['queue' => $this->name, 'body' => $body]);
    }

    public function pop() {
        return _call('POST', 'queue/pop', ['queue' => $this->name]);
    }
}

// ---------------------------------------------------------------------------
// Blob
// ---------------------------------------------------------------------------

function _split_bucket_key(string $name): array {
    $i = strpos($name, '/');
    if ($i === false) return ['default', $name];
    return [substr($name, 0, $i), substr($name, $i + 1)];
}

class Blob {
    public static function put(string $name, $data, ?string $content_type = null): void {
        [$bucket, $key] = _split_bucket_key($name);
        $path = 'blob/put?bucket=' . urlencode($bucket) . '&key=' . urlencode($key);
        $bytes = is_string($data) ? $data : json_encode($data);
        _call_raw('POST', $path, $bytes, $content_type ?? 'application/octet-stream');
    }

    public static function get(string $name) {
        [$bucket, $key] = _split_bucket_key($name);
        if (_get_backbone_url() === '') return null;
        $path = 'blob/get?bucket=' . urlencode($bucket) . '&key=' . urlencode($key);
        [$status, $r] = _backbone_http('GET', $path, null, '');
        return ($status >= 200 && $status < 300) ? $r : null;
    }
}

// ---------------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------------

class Lock {
    public static function acquire(string $name, int $ttl): string {
        $resp = _call('POST', 'lock/acquire', ['name' => $name, 'ttl' => $ttl]);
        return ($resp ?? [])['token'] ?? '';
    }

    public static function release(string $name, string $token): void {
        _call('POST', 'lock/release', ['name' => $name, 'token' => $token]);
    }
}

// ---------------------------------------------------------------------------
// Realtime — pub/sub fan-out over the slice's Canvas WebSocket hub.
// ---------------------------------------------------------------------------
//
// Subscribers connect over WebSocket at the Canvas route /realtime/<name>;
// publish fans a message out to every connected subscriber.

class Realtime {
    public static function channel(string $name): RealtimeChannel {
        return new RealtimeChannel($name);
    }
}

class RealtimeChannel {
    private string $name;

    public function __construct(string $name) { $this->name = $name; }

    /** Publish a message to every subscriber. Returns the recipient count. */
    public function publish($message): int {
        $resp = _call('POST', 'realtime/publish', ['channel' => $this->name, 'message' => $message]);
        return is_array($resp) ? (int)($resp['recipients'] ?? 0) : 0;
    }

    /** The number of subscribers currently connected to this channel. */
    public function presence(): int {
        $resp = _call('GET', 'realtime/presence?channel=' . urlencode($this->name));
        return is_array($resp) ? (int)($resp['present'] ?? 0) : 0;
    }
}

// ─── SQL ────────────────────────────────────────────────────────────────────
//
// Per-slice SQLite databases addressed by name. Reached via
// \Drift\Backbone\sql($name). Wire shape: one JSON envelope per call.
// See docs/memos/backbone-sql.md.
//
//   $db = Drift\Backbone\sql('clinic');
//   $rows = $db->query('SELECT * FROM appointments WHERE slot >= ?', ['2026-05-01']);
//   $res = $db->execute('INSERT INTO appointments(...) VALUES(?, ?)', ['alice', '10:00']);
//   $db->transaction(function (Drift\Backbone\SqlTx $tx) {
//       $tx->execute('UPDATE appointments SET status=? WHERE id=?', ['confirmed', 7]);
//   });
//

class SqlDb {
    private string $name;

    public function __construct(string $name) {
        $this->name = $name;
    }

    public function query(string $sql, array $args = []): array {
        $resp = _call('POST', 'sql/query',
            ['db' => $this->name, 'sql' => $sql, 'args' => $args]) ?: [];
        $cols = $resp['columns'] ?? [];
        $rows = $resp['rows'] ?? [];
        $out = [];
        foreach ($rows as $r) {
            $assoc = [];
            foreach ($cols as $i => $c) {
                $assoc[$c] = $r[$i] ?? null;
            }
            $out[] = $assoc;
        }
        return $out;
    }

    public function execute(string $sql, array $args = []): array {
        return _call('POST', 'sql/execute',
            ['db' => $this->name, 'sql' => $sql, 'args' => $args]) ?: [];
    }

    public function begin(): SqlTx {
        $resp = _call('POST', 'sql/begin', ['db' => $this->name]) ?: [];
        return new SqlTx($this->name, $resp['tx'] ?? '');
    }

    public function transaction(callable $fn) {
        $tx = $this->begin();
        try {
            $out = $fn($tx);
            $tx->commit();
            return $out;
        } catch (\Throwable $e) {
            try { $tx->rollback(); } catch (\Throwable $_) { /* ignore */ }
            throw $e;
        }
    }
}

class SqlTx {
    private string $db;
    private string $token;

    public function __construct(string $db, string $token) {
        $this->db = $db;
        $this->token = $token;
    }

    public function query(string $sql, array $args = []): array {
        $resp = _call('POST', 'sql/query',
            ['db' => $this->db, 'sql' => $sql, 'args' => $args, 'tx' => $this->token]) ?: [];
        $cols = $resp['columns'] ?? [];
        $rows = $resp['rows'] ?? [];
        $out = [];
        foreach ($rows as $r) {
            $assoc = [];
            foreach ($cols as $i => $c) {
                $assoc[$c] = $r[$i] ?? null;
            }
            $out[] = $assoc;
        }
        return $out;
    }

    public function execute(string $sql, array $args = []): array {
        return _call('POST', 'sql/execute',
            ['db' => $this->db, 'sql' => $sql, 'args' => $args, 'tx' => $this->token]) ?: [];
    }

    public function commit(): void {
        _call('POST', 'sql/commit', ['tx' => $this->token]);
    }

    public function rollback(): void {
        _call('POST', 'sql/rollback', ['tx' => $this->token]);
    }
}

function sql(string $name): SqlDb {
    return new SqlDb($name);
}

} // namespace Drift\Backbone

// ===========================================================================
// Deed — the 4th pillar: identity, verified. A peer of \Drift\Backbone (not
// one of its primitives), reached as \Drift\Deed\KeyAuth, \Drift\Deed\JWT,
// \Drift\Deed\Vault, \Drift\Deed\Link, \Drift\Deed\Pocket.
//
// KeyAuth: passwordless Ed25519 device-key auth. JWT: general-purpose HS256
// sign/verify (KeyAuth mints its own tokens through it). Vault: an
// account-key-wrapped keyring. Link: multi-device attestation / enrollment /
// revocation. Pocket: E2EE per-identity app data, JWT-gated.
//
// Not to be confused with cross-slice calling (\Drift\slice($name) /
// \Drift\caller_slice($req), defined above in the top-level \Drift
// namespace) — that's inter-slice networking, a different, still-
// hypothetical future pillar. Deed\Link enrolls another DEVICE for the same
// identity; it has nothing to do with calling another SLICE.
// ===========================================================================

namespace Drift\Deed {

use function Drift\_backbone_http;
use function Drift\_get_deed_url;

// ---------------------------------------------------------------------------
// KeyAuth — passwordless Ed25519 device-key auth.
// ---------------------------------------------------------------------------
//
// uid = the public key. ``challenge`` mints a one-time nonce; the client signs
// the canonical {domain,nonce,pubkey}; ``verify`` checks the signature and
// returns THIS slice's session JWT. ``domain`` namespaces the signature per
// app (replay-safety). Moved here verbatim from \Drift\Backbone\KeyAuth —
// routes are unchanged, only the namespace moved.

class KeyAuth {
    public static function challenge(string $pubkey): string {
        $resp = _call_deed('POST', 'keyauth/challenge', ['pubkey' => $pubkey]);
        return is_array($resp) ? (string)($resp['nonce'] ?? '') : '';
    }

    public static function verify(string $pubkey, string $sig, string $domain): string {
        $resp = _call_deed('POST', 'keyauth/verify', ['pubkey' => $pubkey, 'sig' => $sig, 'domain' => $domain]);
        return is_array($resp) ? (string)($resp['token'] ?? '') : '';
    }
}

// ---------------------------------------------------------------------------
// JWT primitive
// ---------------------------------------------------------------------------
//
// HS256 minting + verification, signed with the slice's per-slice JKey. The
// signing key never leaves the slice's backbone process; all operations flow
// through loopback HTTP to backbone /jwt/{sign,verify,slice-id}. Moved here
// verbatim from \Drift\Backbone\JWT — routes and method surface are
// unchanged, only the namespace moved.
//
// Design: internal/todo/slice-jwt-primitive.md.

class JWT {
    /**
     * Sign a JWT with the slice's HS256 JKey. ``exp`` is required;
     * ``iat``, ``iss``, and ``jti`` are auto-set when null. ``custom``
     * is an associative array of app-specific claims that the platform
     * never inspects.
     */
    public static function issue(array $claims = []): string {
        $allowed = ['sub', 'iat', 'exp', 'nbf', 'iss', 'aud', 'jti', 'custom'];
        $body = [];
        foreach ($allowed as $k) {
            if (array_key_exists($k, $claims) && $claims[$k] !== null) {
                $body[$k] = $claims[$k];
            }
        }
        $resp = _call_deed('POST', 'jwt/sign', $body);
        return is_array($resp) ? (string)($resp['token'] ?? '') : '';
    }

    /**
     * Validate a token. Returns the parsed claims array on success;
     * throws Drift\JWTError on validation failure.
     */
    public static function verify(string $token, ?string $audience = null, ?string $allowed_issuer = null): array {
        $body = ['token' => $token];
        if ($audience !== null)       $body['audience']       = $audience;
        if ($allowed_issuer !== null) $body['allowed_issuer'] = $allowed_issuer;
        $resp = _call_deed('POST', 'jwt/verify', $body);
        if (!is_array($resp)) {
            throw new \Drift\JWTError('internal_error');
        }
        if (empty($resp['valid'])) {
            throw new \Drift\JWTError($resp['reason'] ?? 'internal_error');
        }
        return $resp['claims'] ?? [];
    }

    /** The slice's auto-set issuer string ("drift-slice-<user>-<slice>"). */
    public static function slice_id(): string {
        $resp = _call_deed('GET', 'jwt/slice-id');
        return is_array($resp) ? (string)($resp['slice_id'] ?? '') : '';
    }
}

// ---------------------------------------------------------------------------
// Vault — zero-knowledge recovery store.
// ---------------------------------------------------------------------------
//
// Opaque, user-scoped, append-only. The client encrypts the blob under a key
// derived from its recovery phrase (which the slice NEVER sees), so Drift
// stores the backup but cannot read it. Backed by Deed's own dedicated
// routes (deed/vault/put, deed/vault/get): AES-256-GCM at rest is defense
// in depth only — the blob must already be ciphertext before it arrives,
// since that's the actual source of Vault's confidentiality guarantee. No
// Driftfile declaration needed — this REWRITES and replaces the old
// generic-NoSQL-collection (`keyvault`) implementation entirely.

class Vault {
    /**
     * Append an opaque encrypted backup blob for uid. Append-only (a new
     * version each call); get() returns the newest.
     */
    public static function put(string $uid, $blob): void {
        _call_deed('POST', 'deed/vault/put', ['uid' => $uid, 'blob' => $blob]);
    }

    /**
     * Return the most recent backup blob for uid. Throws \RuntimeException
     * if uid has never written one (same "not found is an error"
     * convention as Deed\Pocket::get() below).
     */
    public static function get(string $uid) {
        $resp = _call_deed('GET', 'deed/vault/get?uid=' . urlencode($uid));
        return is_array($resp) ? ($resp['blob'] ?? null) : null;
    }
}

// ---------------------------------------------------------------------------
// Link — multi-device attestation / enrollment / revocation.
// ---------------------------------------------------------------------------
//
// Generalizes the enroll/attest/revoke pattern so an identity's KeyAuth
// session can move to a second, third, ... device. The signature
// parameters below ($sig, $attesting_pubkey, etc.) are produced entirely
// client-side — this SDK only forwards them, the same way KeyAuth::verify()
// forwards a signature it never computes itself. The one rule the whole
// design rests on: Deed verifies, it never decides — a device is only ever
// added on the strength of a signature from a device already active in the
// identity's registry.
//
// Not to be confused with cross-slice calling (\Drift\slice($name) /
// \Drift\caller_slice($req)) — this Link enrolls a DEVICE for one identity,
// it does not call another slice.

class Link {
    /**
     * Start a device-linking session for a not-yet-enrolled device's
     * pubkey (usually carried in a QR code alongside the pubkey). Returns
     * a session ID for an already-active device to present to attest().
     */
    public static function begin(string $pubkey): string {
        $resp = _call_deed('POST', 'deed/link/begin', ['pubkey' => $pubkey]);
        return is_array($resp) ? (string)($resp['session_id'] ?? '') : '';
    }

    /**
     * Have an already-active device vouch for the session's pending
     * device. $sig is the client's signature over the canonical
     * {domain,identity,new_pubkey} message — computed client-side, never
     * by this SDK.
     */
    public static function attest(string $identity, string $session_id, string $attesting_pubkey, string $sig): void {
        _call_deed('POST', 'deed/link/attest', [
            'identity'         => $identity,
            'session_id'       => $session_id,
            'attesting_pubkey' => $attesting_pubkey,
            'sig'              => $sig,
        ]);
    }

    /**
     * Poll a session the new device started with begin(), returning
     * whether an active device has attested it yet. Result:
     * ['status' => ..., 'identity' => ...] — identity is set only once
     * status === 'attested'.
     */
    public static function complete(string $session_id): array {
        $resp = _call_deed('POST', 'deed/link/complete', ['session_id' => $session_id]);
        return is_array($resp) ? $resp : ['status' => '', 'identity' => ''];
    }

    /**
     * Deactivate target_pubkey in identity's device registry. Any
     * currently-active device may revoke another (or itself);
     * $revoking_pubkey is the device doing the revoking, $sig its
     * signature over the canonical {domain,identity,target_pubkey}
     * message.
     */
    public static function revoke(string $identity, string $target_pubkey, string $revoking_pubkey, string $sig): void {
        _call_deed('POST', 'deed/link/revoke', [
            'identity'        => $identity,
            'target_pubkey'   => $target_pubkey,
            'revoking_pubkey' => $revoking_pubkey,
            'sig'             => $sig,
        ]);
    }
}

// ---------------------------------------------------------------------------
// Pocket — E2EE per-identity app data, JWT-gated.
// ---------------------------------------------------------------------------
//
// An app's actual data — E2EE, content-keyed, following an identity across
// every device Link has enrolled. The crypto work happens entirely
// client-side before anything reaches this primitive; Pocket never
// encrypts or decrypts the payload itself. Every call takes $token
// explicitly (the JWT KeyAuth::verify() returned) rather than holding
// hidden session state — matching the rest of this SDK's stateless posture
// inside an Atomic function invocation. The token's sub is the only
// identity a call can read or write under; there is no way to name a
// different one. Every call sends "Authorization: Bearer <token>".

class Pocket {
    /** Store $blob under $key for whichever identity $token resolves to. */
    public static function set(string $token, string $key, $blob): void {
        _call_deed('POST', 'deed/pocket/set', ['key' => $key, 'blob' => $blob], $token);
    }

    /**
     * Return the blob stored under $key for $token's identity. Throws
     * \RuntimeException if no such key exists.
     */
    public static function get(string $token, string $key) {
        $resp = _call_deed('GET', 'deed/pocket/get?key=' . urlencode($key), null, $token);
        return is_array($resp) ? ($resp['blob'] ?? null) : null;
    }

    /**
     * Remove $key for $token's identity. Throws \RuntimeException if no
     * such key exists.
     */
    public static function delete(string $token, string $key): void {
        _call_deed('POST', 'deed/pocket/delete', ['key' => $key], $token);
    }

    /**
     * Every key stored under $token's identity — never another identity's,
     * even by guessing.
     *
     * Named list_keys(), not list(): PHP 7+ technically permits `list` as
     * a method name (verified with `php -l`/`php` directly), but this file
     * avoids it anyway for clarity at the call site, following the same
     * snake_case naming already used elsewhere here (slice_id(),
     * caller_slice()).
     */
    public static function list_keys(string $token): array {
        $resp = _call_deed('GET', 'deed/pocket/list', null, $token);
        return is_array($resp) ? $resp : [];
    }
}

// _call_deed is \Drift\_call with a status check and, optionally, a bearer
// token attached — used by Deed's own dedicated routes (Vault, Link,
// Pocket), which need a hard "not found is an error" convention (unlike the
// older generic Backbone primitives such as Secret::get()/Blob::get(),
// which return an empty/null value for a missing item) and, for Pocket, an
// Authorization header (unlike every other loopback-open Backbone/Deed
// primitive). Requires a running slice (DEED_URL) — same as KeyAuth/JWT
// above, there is no local-dev in-memory fallback for Deed's own dedicated
// routes.
function _call_deed(string $method, string $path, $body = null, ?string $token = null) {
    $base = _get_deed_url();
    if ($base === '') {
        throw new \RuntimeException(
            'drift: deed requires a running slice (DEED_URL) — not available in local dev'
        );
    }
    $headers = $token !== null ? ['Authorization' => "Bearer $token"] : [];
    [$status, $result] = _backbone_http(
        $method, $path, $body !== null ? json_encode($body) : null, 'application/json', $headers, $base
    );
    if ($status >= 400) {
        $msg = trim((string)$result);
        if ($msg === '') $msg = "HTTP $status";
        throw new \RuntimeException("drift: backbone $path: HTTP $status: $msg");
    }
    if ($status === 204 || $result === null || $result === '') return null;
    $decoded = json_decode($result, true);
    return ($decoded !== null) ? $decoded : $result;
}

} // namespace Drift\Deed
