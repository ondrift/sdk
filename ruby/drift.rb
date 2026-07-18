# Drift SDK for Ruby Atomic functions.
#
# This single-file SDK provides:
#   - Drift.run(handler): Entry point that dispatches to deployed or local mode.
#   - Drift::Backbone — the B of the sacred A·B·C triad; the SOLE entrypoint for
#     every STATE primitive: Secret, Cache, Nosql, queue, Blob, Lock, sql,
#     Realtime. (There is no top-level Drift::Secret etc. — go through
#     Drift::Backbone.)
#   - Drift::Deed — identity, verified. The fourth pillar alongside Atomic /
#     Backbone / Canvas: KeyAuth, JWT, Vault, Link, Pocket. Not to be confused
#     with Drift.slice(name) below (inter-slice networking) -- Deed::Link
#     enrolls another DEVICE for one identity; it has nothing to do with
#     calling another SLICE.
#   - Drift.log(msg): Writes to stderr (captured by the runner as function logs).
#   - Drift.http_request(): Outbound HTTP from within a function.
#   - Drift.slice(name) / Drift.caller_slice(req): slice-to-slice linking.
#
# All backbone/deed helpers use only stdlib (net/http) -- zero external
# dependencies.

require 'json'
require 'net/http'
require 'socket'
require 'uri'

# `webrick` and `socket` are required lazily inside the local-dev paths
# (_run_local / _run_local_sse). Ruby 3.0+ removed webrick from stdlib;
# requiring it at file scope unconditionally crashes the deployed
# runtime, which doesn't ship webrick and doesn't need it (deployed
# mode is stdin → handler → stdout, no HTTP server).

module Drift

  # ---------------------------------------------------------------------------
  # Entry point
  # ---------------------------------------------------------------------------

  def self.run(handler)
    if ENV['DRIFT_RUNTIME']
      _run_deployed(handler)
    else
      _run_local(handler)
    end
  end

  def self._run_deployed(handler)
    req = JSON.parse($stdin.read)
    raw_query = req['query']
    if raw_query.is_a?(String)
      parsed = {}
      raw_query.split('&').each do |pair|
        next if pair.empty?
        k, v = pair.split('=', 2)
        parsed[URI.decode_www_form_component(k)] = URI.decode_www_form_component(v || '')
      end
      req['query'] = parsed
    end
    resp = handler.call(req)
    $stdout.write(JSON.generate(resp))
    $stdout.flush
  end

  def self._run_local(handler)
    require 'webrick'
    port = (ENV['PORT'] || '8080').to_i

    server = WEBrick::HTTPServer.new(Port: port, Logger: WEBrick::Log.new($stderr, WEBrick::Log::WARN), AccessLog: [])
    $stderr.puts "drift-sdk: local server starting on :#{port}"

    server.mount_proc '/' do |http_req, http_resp|
      headers = {}
      http_req.header.each { |k, v| headers[k.downcase] = v.is_a?(Array) ? v.first : v }

      body = nil
      if http_req.body && !http_req.body.empty?
        begin
          body = JSON.parse(http_req.body)
        rescue JSON::ParserError
          body = http_req.body
        end
      end

      req = {
        'path' => http_req.path,
        'headers' => headers,
        'query' => http_req.query_string || '',
        'body' => body,
      }

      resp = handler.call(req)
      status = resp['status'] || 200
      out = JSON.generate(resp)

      http_resp.status = status
      http_resp['Content-Type'] = 'application/json'
      http_resp.body = out
    end

    trap('INT') { server.shutdown }
    server.start
  end

  # ---------------------------------------------------------------------------
  # Backbone transport
  # ---------------------------------------------------------------------------

  @backbone_url = nil

  def self._get_backbone_url
    @backbone_url ||= ENV['BACKBONE_URL'] || ''
  end

  @deed_url = nil

  # Deed has its own listener/port now (DEED_URL), separate from
  # Backbone's -- see _call_deed/_call_deed_auth below.
  def self._get_deed_url
    @deed_url ||= ENV['DEED_URL'] || ''
  end

  # _backbone_http issues a request over TCP (Net::HTTP opens a fresh
  # connection per call, so no persistent-connection thrashing concern
  # between different hosts/ports). Returns [status_code, body]. Stdlib
  # only — Net::HTTP. `base:` defaults to Backbone's URL; _call_deed/
  # _call_deed_auth pass Deed's own URL instead, since Deed lives on a
  # separate port. `token:`, when given, sets an Authorization: Bearer
  # header -- the one call shape that needs it (Drift::Deed::Pocket, whose
  # routes are JWT-gated; see _call_deed_auth below). Every other caller
  # omits it.
  def self._backbone_http(method, path, body, content_type, token: nil, base: nil)
    base ||= _get_backbone_url
    uri = URI("#{base}/#{path}")
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = uri.scheme == 'https'
    req = Net::HTTP.const_get(method.capitalize).new(uri)
    if body
      req['Content-Type'] = content_type
      req.body = body
    end
    req['Authorization'] = "Bearer #{token}" if token
    resp = http.request(req)
    [resp.code.to_i, resp.body]
  end

  # Raises Drift::BackboneError on any non-2xx response -- shared by _call
  # and _call_raw (see BackboneError's doc comment above for why).
  def self._check_backbone_status!(code, resp_body, path)
    return unless code && code >= 400
    msg = resp_body.to_s.strip
    msg = "HTTP #{code}" if msg.empty?
    raise BackboneError, "drift: backbone #{path}: #{msg}"
  end

  def self._call(method, path, body = nil)
    return _call_local(method, path, body) if _get_backbone_url.empty?
    code, resp_body = _backbone_http(method, path, body ? JSON.generate(body) : nil, 'application/json')
    _check_backbone_status!(code, resp_body, path)
    return nil if code == 204 || resp_body.nil? || resp_body.empty?
    begin
      JSON.parse(resp_body)
    rescue JSON::ParserError
      resp_body
    end
  end

  def self._call_raw(method, path, data_bytes, content_type = 'application/octet-stream')
    return nil if _get_backbone_url.empty?
    code, resp_body = _backbone_http(method, path, data_bytes, content_type)
    _check_backbone_status!(code, resp_body, path)
    resp_body
  end

  # _call_deed / _call_deed_auth back Drift::Deed's Vault/Link/Pocket calls.
  # Unlike the general _call above (which never inspects the HTTP status
  # code at all -- see its comment), these raise Drift::DeedError on any
  # non-2xx response, mirroring the Go SDK's callBackboneHTTP: a 404 body
  # is an ERROR, not data, so e.g. Vault.get on an unwritten uid fails
  # loudly instead of silently returning nil. There's no in-memory
  # local-dev backing store for deed/* routes (none of the local-only
  # stubs in _call_local below handle them), so in local dev
  # fire-and-forget writes (Vault.put, Link.begin/attest/revoke) are
  # harmless no-ops, while accessor calls (Vault.get, Link.complete) fall
  # back to whatever their own not-a-Hash guard returns.
  def self._deed_response(code, resp_body, path)
    if code && code >= 400
      msg = resp_body.to_s.strip
      msg = "HTTP #{code}" if msg.empty?
      raise DeedError, "drift: deed #{path}: #{msg}"
    end
    return nil if code == 204 || resp_body.nil? || resp_body.empty?
    begin
      JSON.parse(resp_body)
    rescue JSON::ParserError
      resp_body
    end
  end

  def self._call_deed(method, path, body = nil)
    return nil if _get_deed_url.empty?
    code, resp_body = _backbone_http(method, path, body ? JSON.generate(body) : nil, 'application/json', base: _get_deed_url)
    _deed_response(code, resp_body, path)
  end

  # _call_deed_auth is _call_deed with a bearer token attached -- used only
  # by Deed::Pocket, whose routes are JWT-gated (unlike every other
  # loopback-open Backbone/Deed primitive). Local dev has no JWT
  # verification to check a token against, so -- same as KeyAuth/JWT
  # already -- Pocket simply isn't available without a running slice.
  def self._call_deed_auth(method, path, token, body = nil)
    if _get_deed_url.empty?
      raise DeedError, 'drift: deed pocket requires a running slice (DEED_URL) -- not available in local dev'
    end
    code, resp_body = _backbone_http(method, path, body ? JSON.generate(body) : nil, 'application/json', token: token, base: _get_deed_url)
    _deed_response(code, resp_body, path)
  end

  # In-memory backbone for local dev.
  @local_store = {
    'nosql' => {}, 'cache' => {}, 'queues' => {},
    'blobs' => {}, 'locks' => {}, 'next_id' => 0,
  }

  def self._call_local(method, path, body = nil)
    s = @local_store
    base_path, qs = path.split('?', 2)
    query = {}
    if qs
      qs.split('&').each do |pair|
        k, v = pair.split('=', 2)
        query[URI.decode_www_form_component(k)] = URI.decode_www_form_component(v || '')
      end
    end

    # NoSQL
    if base_path == 'write' && method == 'POST'
      col = (body || {})['collection'] || 'default'
      s['nosql'][col] ||= {}
      s['next_id'] += 1
      key = s['next_id'].to_s
      s['nosql'][col][key] = body
      return { 'key' => key }
    end
    if base_path == 'read' && method == 'GET'
      col = query['collection'] || 'default'
      return (s['nosql'][col] || {})[query['key'] || '']
    end
    if base_path == 'nosql/list' && method == 'GET'
      col = query['collection'] || 'default'
      docs = s['nosql'][col] || {}
      field = query['field']
      value = query['value']
      results = []
      docs.each_value do |doc|
        next if field && doc[field].to_s != value
        results << doc
      end
      return results
    end
    if base_path == 'nosql/drop' && method == 'POST'
      s['nosql'].delete(query['collection'] || 'default')
      return nil
    end

    # Cache
    if base_path == 'cache/set' && method == 'POST'
      s['cache'][(body || {})['key'] || ''] = (body || {})['value']
      return nil
    end
    if base_path == 'cache/get' && method == 'GET'
      return s['cache'][query['key'] || '']
    end
    if base_path == 'cache/del'
      s['cache'].delete(query['key'] || '')
      return nil
    end

    # Queue
    if base_path == 'queue/push' && method == 'POST'
      name = (body || {})['queue'] || ''
      s['queues'][name] ||= []
      s['queues'][name] << (body || {})['body']
      return nil
    end
    if base_path == 'queue/pop' && method == 'POST'
      name = (body || {})['queue'] || ''
      q = s['queues'][name] || []
      return nil if q.empty?
      return q.shift
    end

    # Blob
    if base_path == 'blob/put' && method == 'POST'
      s['blobs'][(body || {})['name'] || ''] = (body || {})['data']
      return nil
    end
    if base_path == 'blob/get' && method == 'GET'
      return s['blobs'][query['name'] || '']
    end

    # Secret — in local dev, read from environment variables (loaded from .env by the CLI)
    if base_path == 'secret/get' && method == 'GET'
      return ENV[query['name']]
    end

    # Lock
    if base_path == 'lock/acquire' && method == 'POST'
      name = (body || {})['name'] || ''
      return nil if s['locks'].key?(name)
      s['next_id'] += 1
      token = "local-lock-#{s['next_id']}"
      s['locks'][name] = token
      return { 'token' => token }
    end
    if base_path == 'lock/release' && method == 'POST'
      s['locks'].delete((body || {})['name'] || '')
      return nil
    end

    nil
  end

  # Raised by Drift::Deed::JWT.verify on validation failure. `reason` is one
  # of the stable wire strings: malformed, bad_signature, expired, not_yet_valid,
  # wrong_algorithm, wrong_issuer, wrong_audience, invalid_claims, missing_exp,
  # internal_error. Kept top-level (it is an error type, not a state entrypoint).
  class JWTError < StandardError
    attr_reader :reason
    def initialize(reason)
      super("jwt verify: #{reason}")
      @reason = reason
    end
  end

  # Raised by Drift::Deed's Vault/Link/Pocket calls on a non-2xx HTTP response
  # from a Deed route -- e.g. Vault.get/Pocket.get on a uid/key that never
  # wrote anything propagate the 404 as this error rather than returning nil
  # (see their doc comments). Kept top-level for the same reason as JWTError:
  # it is an error type, not a state/identity entrypoint.
  class DeedError < StandardError; end

  # Raised by any Backbone call (Secret, Cache, NoSQL, Queue, Blob, Lock, SQL,
  # Realtime) on a non-2xx HTTP response -- a 404/409/etc body is an ERROR,
  # not data, matching the Go SDK's callBackboneHTTP and Deed's DeedError
  # above. In particular a "not found" Get (Secret.get on an undeclared name,
  # Blob.get on an unwritten key, NoSQL Collection#read on an unknown key) now
  # raises instead of silently returning nil -- the same convention as every
  # other language's SDK.
  class BackboneError < StandardError; end

  # ===========================================================================
  # Backbone — the B of the sacred A·B·C triad. The SOLE entrypoint for every
  # state primitive. Use Drift::Backbone::Secret, Drift::Backbone.queue(...),
  # Drift::Backbone::Realtime, etc. — nothing stateful lives at the top level.
  # ===========================================================================

  module Backbone

    # -------------------------------------------------------------------------
    # Secret
    # -------------------------------------------------------------------------

    module Secret
      # Read order:
      #   1. DRIFT_SECRET_<NAME> env var — set by the runner from the
      #      function's @atomic-secrets allowlist. This is the only path
      #      that works in production: backbone /secret/get is SAT-guarded
      #      and the subprocess does not have the SAT.
      #   2. HTTP fallback — local-dev (`drift atomic run`) only. In
      #      production the call returns 401.
      def self.get(name)
        env_val = ENV["DRIFT_SECRET_#{name.upcase}"]
        return env_val unless env_val.nil?
        resp = Drift._call('GET', "secret/get?name=#{URI.encode_www_form_component(name)}")
        resp.is_a?(String) ? resp : (resp ? JSON.generate(resp) : '')
      end

      def self.set(name, value)
        Drift._call('POST', 'secret/set', { 'name' => name, 'value' => value })
      end

      def self.delete(name)
        Drift._call('DELETE', "secret/delete?name=#{URI.encode_www_form_component(name)}")
      end
    end

    # -------------------------------------------------------------------------
    # Cache
    # -------------------------------------------------------------------------

    module Cache
      def self.get(key)
        Drift._call('GET', "cache/get?key=#{URI.encode_www_form_component(key)}")
      end

      def self.set(key, value, ttl:)
        payload = { 'key' => key, 'value' => value }
        payload['ttl'] = ttl if ttl > 0
        Drift._call('POST', 'cache/set', payload)
      end

      def self.delete(key)
        Drift._call('DELETE', "cache/del?key=#{URI.encode_www_form_component(key)}")
      end
    end

    # -------------------------------------------------------------------------
    # NoSQL
    # -------------------------------------------------------------------------

    module Nosql
      def self.collection(name)
        Collection.new(name)
      end

      class Collection
        def initialize(name)
          @name = name
        end

        def insert(doc)
          payload = { 'collection' => @name }
          if doc.is_a?(Hash)
            payload.merge!(doc)
          else
            payload['data'] = doc
          end
          resp = Drift._call('POST', 'write', payload)
          resp.is_a?(Hash) ? (resp['key'] || '') : ''
        end

        def read(key)
          Drift._call('GET', "read?collection=#{URI.encode_www_form_component(@name)}&key=#{URI.encode_www_form_component(key)}")
        end

        def get(id)
          path = "nosql/list?collection=#{URI.encode_www_form_component(@name)}&field=_id&value=#{URI.encode_www_form_component(id)}"
          rows = Drift._call('GET', path)
          rows = rows.is_a?(Array) ? rows : []
          rows.first
        end

        def delete(key)
          Drift._call('POST', "nosql/delete?collection=#{URI.encode_www_form_component(@name)}&key=#{URI.encode_www_form_component(key)}")
        end

        def list(filter = nil)
          path = "nosql/list?collection=#{URI.encode_www_form_component(@name)}"
          if filter
            filter.each do |k, v|
              path += "&field=#{URI.encode_www_form_component(k)}&value=#{URI.encode_www_form_component(v)}"
            end
          end
          resp = Drift._call('GET', path)
          resp.is_a?(Array) ? resp : []
        end

        def drop
          Drift._call('POST', "nosql/drop?collection=#{URI.encode_www_form_component(@name)}")
        end
      end
    end

    # -------------------------------------------------------------------------
    # Queue
    # -------------------------------------------------------------------------

    def self.queue(name)
      QueueHandle.new(name)
    end

    class QueueHandle
      def initialize(name)
        @name = name
      end

      def push(body)
        Drift._call('POST', 'queue/push', { 'queue' => @name, 'body' => body })
      end

      def pop
        Drift._call('POST', 'queue/pop', { 'queue' => @name })
      end
    end

    # -------------------------------------------------------------------------
    # Blob
    # -------------------------------------------------------------------------

    module Blob
      def self._split(name)
        i = name.index('/')
        i ? [name[0...i], name[(i + 1)..]] : ['default', name]
      end

      def self.put(name, data, content_type: nil)
        bucket, key = _split(name)
        path = "blob/put?bucket=#{URI.encode_www_form_component(bucket)}&key=#{URI.encode_www_form_component(key)}"
        bytes = data.is_a?(String) ? data : data.to_s
        Drift._call_raw('POST', path, bytes, content_type || 'application/octet-stream')
      end

      def self.get(name)
        bucket, key = _split(name)
        return nil if Drift._get_backbone_url.empty?
        path = "blob/get?bucket=#{URI.encode_www_form_component(bucket)}&key=#{URI.encode_www_form_component(key)}"
        code, body = Drift._backbone_http('GET', path, nil, nil)
        Drift._check_backbone_status!(code, body, path)
        body
      end
    end

    # -------------------------------------------------------------------------
    # Lock
    # -------------------------------------------------------------------------

    module Lock
      def self.acquire(name, ttl:)
        resp = Drift._call('POST', 'lock/acquire', { 'name' => name, 'ttl' => ttl })
        (resp || {})['token'] || ''
      end

      def self.release(name, token)
        Drift._call('POST', 'lock/release', { 'name' => name, 'token' => token })
      end
    end

    # -------------------------------------------------------------------------
    # Realtime — pub/sub fan-out over the slice's Canvas WebSocket hub.
    # -------------------------------------------------------------------------
    #
    # Subscribers connect over WebSocket at the Canvas route /realtime/<name>;
    # publish fans a message out to every connected subscriber.

    module Realtime
      def self.channel(name)
        Channel.new(name)
      end

      class Channel
        def initialize(name)
          @name = name
        end

        # Publish a message to every subscriber. Returns the recipient count.
        def publish(message)
          resp = Drift._call('POST', 'realtime/publish', { 'channel' => @name, 'message' => message })
          resp.is_a?(Hash) ? (resp['recipients'] || 0) : 0
        end

        # The number of subscribers currently connected to this channel.
        def presence
          resp = Drift._call('GET', "realtime/presence?channel=#{URI.encode_www_form_component(@name)}")
          resp.is_a?(Hash) ? (resp['present'] || 0) : 0
        end
      end
    end

    # -------------------------------------------------------------------------
    # SQL — per-slice SQLite databases.
    # -------------------------------------------------------------------------

    def self.sql(name)
      Drift::SqlDb.new(name)
    end
  end

  # ===========================================================================
  # Deed — identity, verified. The fourth pillar alongside Atomic / Backbone /
  # Canvas -- a peer subsystem, not a Backbone primitive, with its own
  # loopback listener (DEED_URL) separate from Backbone's (BACKBONE_URL).
  #
  #   Drift::Deed::KeyAuth / JWT / Vault / Link / Pocket
  #
  # KeyAuth: passwordless Ed25519 device-key auth. JWT: general-purpose HS256
  # sign/verify (KeyAuth mints its own tokens through it). Vault: an
  # account-key-wrapped keyring. Link: multi-device attestation / enrollment /
  # revocation. Pocket: E2EE per-identity app data, JWT-gated.
  #
  # Not to be confused with cross-slice calling (Drift.slice(name) /
  # Drift.caller_slice, further below) -- that's inter-slice networking, an
  # unrelated concept. Deed::Link enrolls another DEVICE for the same
  # identity; it has nothing to do with calling another SLICE.
  # ===========================================================================

  module Deed

    # -------------------------------------------------------------------------
    # KeyAuth — passwordless Ed25519 device-key auth.
    # -------------------------------------------------------------------------
    #
    # uid = the public key. `challenge` mints a one-time nonce; the client signs
    # the canonical {domain,nonce,pubkey}; `verify` checks the signature and
    # returns THIS slice's session JWT. `domain` namespaces the signature per
    # app (replay-safety).

    module KeyAuth
      def self.challenge(pubkey)
        resp = Drift._call_deed('POST', 'keyauth/challenge', { 'pubkey' => pubkey })
        resp.is_a?(Hash) ? (resp['nonce'] || '') : ''
      end

      def self.verify(pubkey, sig, domain)
        resp = Drift._call_deed('POST', 'keyauth/verify', { 'pubkey' => pubkey, 'sig' => sig, 'domain' => domain })
        resp.is_a?(Hash) ? (resp['token'] || '') : ''
      end
    end

    # -------------------------------------------------------------------------
    # JWT — general-purpose HS256 sign/verify.
    # -------------------------------------------------------------------------
    #
    # Signed with the slice's per-slice JKey. The signing key never leaves the
    # slice's backbone process; all operations flow through loopback HTTP to
    # backbone /jwt/{sign,verify,slice-id}. KeyAuth mints its own session
    # tokens through this same primitive.
    #
    # Design: internal/todo/slice-jwt-primitive.md.

    module JWT
      # Sign a JWT with the slice's HS256 JKey. `exp` is required; `iat`,
      # `iss`, and `jti` are auto-set when nil. `custom` is a hash of
      # app-specific claims that the platform never inspects.
      def self.issue(sub: nil, exp: nil, iat: nil, nbf: nil, iss: nil, aud: nil, jti: nil, custom: nil)
        body = {}
        body['sub']    = sub    unless sub.nil?
        body['exp']    = exp    unless exp.nil?
        body['iat']    = iat    unless iat.nil?
        body['nbf']    = nbf    unless nbf.nil?
        body['iss']    = iss    unless iss.nil?
        body['aud']    = aud    unless aud.nil?
        body['jti']    = jti    unless jti.nil?
        body['custom'] = custom unless custom.nil?
        resp = Drift._call_deed('POST', 'jwt/sign', body)
        (resp || {})['token']
      end

      # Validate a token. Returns the parsed claims hash on success;
      # raises Drift::JWTError on validation failure.
      def self.verify(token, audience: nil, allowed_issuer: nil)
        body = { 'token' => token }
        body['audience']       = audience       unless audience.nil?
        body['allowed_issuer'] = allowed_issuer unless allowed_issuer.nil?
        resp = Drift._call_deed('POST', 'jwt/verify', body)
        raise JWTError.new('internal_error') unless resp.is_a?(Hash)
        raise JWTError.new(resp['reason'] || 'internal_error') unless resp['valid']
        resp['claims'] || {}
      end

      # The slice's auto-set issuer string ("drift-slice-<user>-<slice>").
      def self.slice_id
        resp = Drift._call_deed('GET', 'jwt/slice-id')
        resp.is_a?(Hash) ? (resp['slice_id'] || '') : ''
      end
    end

    # -------------------------------------------------------------------------
    # Vault — zero-knowledge recovery store.
    # -------------------------------------------------------------------------
    #
    # Opaque, user-scoped, append-only. The client encrypts the blob under a
    # key derived from its recovery phrase (which the slice NEVER sees), so
    # Drift stores the backup but cannot read it. Scoped to a uid the caller
    # supplies -- typically the authenticated KeyAuth pubkey. Backed by Deed's
    # own dedicated routes (deed/vault/*) -- no Driftfile declaration needed,
    # and no generic-NoSQL-collection scan behind it any more.

    module Vault
      # Append an opaque encrypted backup blob for uid. Append-only -- a new
      # version each call; get returns the newest.
      def self.put(uid, blob)
        Drift._call_deed('POST', 'deed/vault/put', { 'uid' => uid, 'blob' => blob })
      end

      # Return the most recent backup blob for uid. Raises Drift::DeedError if
      # uid has never written one (same "not found is an error" convention
      # observed by the other Deed accessors below -- see Pocket.get).
      def self.get(uid)
        resp = Drift._call_deed('GET', "deed/vault/get?uid=#{URI.encode_www_form_component(uid)}")
        raise DeedError, "drift: vault get: no blob for uid #{uid.inspect}" unless resp.is_a?(Hash)
        resp['blob']
      end
    end

    # -------------------------------------------------------------------------
    # Link — multi-device continuity.
    # -------------------------------------------------------------------------
    #
    # Generalizes the enroll/attest/revoke pattern so an identity's KeyAuth
    # session can move to a second, third, ... device. The signature
    # parameters below (sig, attesting_pubkey, etc.) are produced entirely
    # client-side -- this SDK only forwards them, the same way KeyAuth.verify
    # forwards a signature it never computes itself. The one rule the whole
    # design rests on: Deed verifies, it never decides -- a device is only
    # ever added on the strength of a signature from a device already active
    # in the identity's registry.
    #
    # Not to be confused with Drift.slice(name)/Drift.caller_slice further
    # below -- this Link enrolls a DEVICE for one identity, it does not call
    # another slice.

    module Link
      # Start a device-linking session for a not-yet-enrolled device's
      # pubkey (usually carried in a QR code alongside the pubkey). Returns
      # a session ID for an already-active device to present to attest.
      #
      # Named `begin`, not `start`, to track the wire route 1:1 -- `begin`
      # compiles fine as a plain method name given an explicit receiver
      # (it's a reserved word only in expression position); Drift::SqlDb#begin
      # elsewhere in this file is the same precedent already in production.
      #
      # metadata is an optional opaque string an attesting device can read
      # back via session_info -- e.g. an ephemeral key it should seal a
      # payload for. Deed never interprets it.
      def self.begin(pubkey, metadata: nil)
        body = { 'pubkey' => pubkey }
        body['metadata'] = metadata unless metadata.nil?
        resp = Drift._call_deed('POST', 'deed/link/begin', body)
        resp.is_a?(Hash) ? (resp['session_id'] || '') : ''
      end

      # A read-only, repeatable peek at a pending session -- what an
      # attesting device (which only ever learns session_id, from a
      # scanned/typed code) uses to learn new_pubkey (attest's message is
      # verified server-side against the session's own stored value, never
      # the request body, so the attester has to reconstruct it exactly)
      # and whatever opaque metadata the joining device passed to begin.
      # Returns { 'new_pubkey' => ..., 'metadata' => ... }.
      def self.session_info(session_id)
        resp = Drift._call_deed('POST', 'deed/link/session', { 'session_id' => session_id })
        resp.is_a?(Hash) ? resp : {}
      end

      # Render text (in practice, a Link session ID) as a scannable QR
      # code, returning inline SVG markup. Pure rendering -- no session or
      # identity involvement, so it works for any short string.
      def self.qr(text)
        resp = Drift._call_deed('POST', 'deed/link/qr', { 'text' => text })
        resp.is_a?(Hash) ? (resp['svg'] || '') : ''
      end

      # Have an already-active device vouch for the session's pending
      # device. `sig` is the client's signature over the canonical
      # {domain,identity,new_pubkey} message -- computed client-side, never
      # by this SDK.
      #
      # sealed is an optional opaque string relayed back once complete
      # reports "attested" -- e.g. a payload end-to-end-encrypted for
      # whatever key the joiner published as begin's metadata. Deed only
      # relays it, never opens it.
      def self.attest(identity, session_id, attesting_pubkey, sig, sealed: nil)
        body = {
          'identity' => identity, 'session_id' => session_id,
          'attesting_pubkey' => attesting_pubkey, 'sig' => sig,
        }
        body['sealed'] = sealed unless sealed.nil?
        Drift._call_deed('POST', 'deed/link/attest', body)
      end

      # Poll a session the new device started with begin, returning whether
      # an active device has attested it yet:
      # { 'status' => ..., 'identity' => ..., 'sealed' => ... } --
      # 'identity'/'sealed' are only set once status == "attested" ('sealed'
      # only if attest supplied one).
      def self.complete(session_id)
        resp = Drift._call_deed('POST', 'deed/link/complete', { 'session_id' => session_id })
        resp.is_a?(Hash) ? resp : {}
      end

      # Deactivate target_pubkey in identity's device registry. Any
      # currently-active device may revoke another (or itself);
      # revoking_pubkey is the device doing the revoking, sig its signature
      # over the canonical {domain,identity,target_pubkey} message.
      def self.revoke(identity, target_pubkey, revoking_pubkey, sig)
        Drift._call_deed('POST', 'deed/link/revoke', {
          'identity' => identity, 'target_pubkey' => target_pubkey,
          'revoking_pubkey' => revoking_pubkey, 'sig' => sig,
        })
      end
    end

    # -------------------------------------------------------------------------
    # Pocket — E2EE per-identity app data.
    # -------------------------------------------------------------------------
    #
    # An app's actual data -- content-keyed, following an identity across
    # every device Link has enrolled. The crypto work happens entirely
    # client-side before anything reaches this primitive; Pocket never
    # encrypts or decrypts the payload itself. Every call takes token
    # explicitly (the JWT KeyAuth.verify returned) rather than holding
    # hidden session state -- matching the rest of this SDK's stateless
    # posture inside an Atomic function invocation. The token's sub is the
    # only identity a call can read or write under; there is no way to name
    # a different one.

    module Pocket
      # Store blob under key for whichever identity token resolves to.
      def self.set(token, key, blob)
        Drift._call_deed_auth('POST', 'deed/pocket/set', token, { 'key' => key, 'blob' => blob })
      end

      # Return the blob stored under key for token's identity. Raises
      # Drift::DeedError if no such key exists.
      def self.get(token, key)
        resp = Drift._call_deed_auth('GET', "deed/pocket/get?key=#{URI.encode_www_form_component(key)}", token)
        raise DeedError, "drift: pocket get: no blob for key #{key.inspect}" unless resp.is_a?(Hash)
        resp['blob']
      end

      # Remove key for token's identity. Raises Drift::DeedError if no such
      # key exists.
      def self.delete(token, key)
        Drift._call_deed_auth('POST', 'deed/pocket/delete', token, { 'key' => key })
      end

      # Return every key stored under token's identity -- never another
      # identity's, even by guessing.
      def self.list(token)
        resp = Drift._call_deed_auth('GET', 'deed/pocket/list', token)
        resp.is_a?(Array) ? resp : []
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Logging
  # ---------------------------------------------------------------------------

  def self.log(msg)
    $stderr.puts msg.to_s
    $stderr.flush
  end

  # ---------------------------------------------------------------------------
  # HTTP client
  # ---------------------------------------------------------------------------

  # Default 30s timeout. A function calling a hung remote shouldn't
  # hold an Atomic invocation open longer than this; the runner's
  # per-invocation deadline is the absolute ceiling.
  def self.http_request(method, url, headers = {}, body = nil, timeout: 30)
    uri = URI(url)
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = uri.scheme == 'https'
    http.open_timeout = timeout
    http.read_timeout = timeout

    request = case method.upcase
    when 'GET'    then Net::HTTP::Get.new(uri)
    when 'POST'   then Net::HTTP::Post.new(uri)
    when 'PUT'    then Net::HTTP::Put.new(uri)
    when 'DELETE' then Net::HTTP::Delete.new(uri)
    when 'PATCH'  then Net::HTTP::Patch.new(uri)
    else Net::HTTP::Get.new(uri)
    end

    (headers || {}).each { |k, v| request[k] = v }
    request.body = body if body

    resp = http.request(request)
    { 'status' => resp.code.to_i, 'body' => resp.body }
  end

  # ─── SSE ──────────────────────────────────────────────────────────────────────

  def self.run_sse(&handler)
    if ENV['DRIFT_RUNTIME']
      req = JSON.parse($stdin.read)
      emit = ->(event, data) {
        $stdout.write("event: #{event}\n") if event
        $stdout.write("data: #{JSON.generate(data)}\n\n")
        $stdout.flush
      }
      handler.call(req, emit)
    else
      _run_local_sse(handler)
    end
  end

  # Local-dev SSE server. WEBrick buffers responses, which defeats streaming;
  # we use a raw TCPServer so each `emit` flushes to the wire immediately.
  # The handler is called with (req, emit) where emit is a lambda mirroring
  # the deployed-mode protocol.
  def self._run_local_sse(handler)
    require 'socket'
    port = (ENV['PORT'] || '8080').to_i
    server = TCPServer.new(port)
    $stderr.puts "drift-sdk: local SSE server starting on :#{port}"
    trap('INT') { server.close; exit }

    loop do
      sock = server.accept
      Thread.new(sock) do |s|
        begin
          request_line = s.gets
          next unless request_line
          _, path_query, _ = request_line.split(' ', 3)
          path, query = (path_query || '/').split('?', 2)

          headers = {}
          while (line = s.gets)
            break if line.strip.empty?
            k, v = line.split(':', 2)
            headers[k.downcase.strip] = (v || '').strip if k && v
          end

          body = nil
          if (cl = headers['content-length']&.to_i) && cl > 0
            raw = s.read(cl)
            begin
              body = JSON.parse(raw)
            rescue JSON::ParserError
              body = raw
            end
          end

          req = {
            'path' => path,
            'headers' => headers,
            'query' => query || '',
            'body' => body,
          }

          s.write("HTTP/1.1 200 OK\r\n" \
                  "Content-Type: text/event-stream\r\n" \
                  "Cache-Control: no-cache, no-transform\r\n" \
                  "Connection: keep-alive\r\n" \
                  "X-Accel-Buffering: no\r\n" \
                  "\r\n")
          s.flush

          emit = ->(event, data) {
            s.write("event: #{event}\n") if event
            s.write("data: #{JSON.generate(data)}\n\n")
            s.flush
          }

          begin
            handler.call(req, emit)
          rescue => e
            (s.write("event: error\ndata: #{JSON.generate({'error' => e.to_s})}\n\n") rescue nil)
          end
        ensure
          s.close rescue nil
        end
      end
    end
  end

  # ─── WebSocket ────────────────────────────────────────────────────────────────

  class WsConn
    def read
      line = $stdin.gets
      return nil unless line
      line = line.strip
      return nil if line.empty?
      begin
        JSON.parse(line)
      rescue JSON::ParserError
        line
      end
    end

    def write(data)
      $stdout.puts(JSON.generate(data))
      $stdout.flush
    end

    def write_raw(msg)
      $stdout.puts(msg)
      $stdout.flush
    end
  end

  def self.run_ws(&handler)
    if ENV['DRIFT_RUNTIME']
      first_line = $stdin.gets
      req = first_line ? JSON.parse(first_line) : {}
      conn = WsConn.new
      handler.call(req, conn)
    else
      _run_local(handler)
    end
  end

  # ---------------------------------------------------------------------------
  # Slice-to-slice linking (top-level; unrelated to Drift::Deed::Link)
  # ---------------------------------------------------------------------------
  #
  # Not Backbone, and NOT Deed — this is inter-slice networking: one slice
  # calling another slice it's linked to. Drift::Deed::Link (above) is a
  # different thing entirely: it enrolls another DEVICE under the same
  # identity. Same English word, unrelated concepts -- don't conflate them.

  def self._link_env_name(name)
    'DRIFT_LINK_' + name.upcase.gsub(/[^A-Z0-9]/, '_') + '_URL'
  end

  class SliceClient
    def initialize(name)
      @name = name
    end

    def _url(path)
      base = ENV[Drift._link_env_name(@name)]
      if base.nil? || base.empty?
        raise "drift: not linked to slice \"#{@name}\" — run `drift slice link add #{@name}`"
      end
      base.chomp('/') + '/' + path.to_s.sub(%r{\A/}, '')
    end

    def request(method, path, headers: {}, body: nil)
      h = { 'X-Drift-Slice' => (ENV['DRIFT_SLICE'] || '') }.merge(headers || {})
      Drift.http_request(method, _url(path), h, body)
    end

    def get(path)
      request('GET', path)
    end

    def post(path, body = nil)
      request('POST', path,
              headers: { 'Content-Type' => 'application/json' },
              body: body.nil? ? nil : JSON.generate(body))
    end
  end

  # A client for another slice you've LINKED to (`drift slice link`). The call
  # travels in-cluster and carries this slice's identity (X-Drift-Slice).
  def self.slice(name)
    SliceClient.new(name)
  end

  # The linked slice that called this request, or "" if not via a link.
  def self.caller_slice(req)
    headers = (req || {})['headers'] || {}
    headers.each { |k, v| return v if k.to_s.downcase == 'x-drift-slice' }
    ''
  end

  # An environment variable value ("" if unset).
  def self.env(key)
    ENV[key] || ''
  end

  # ─── SQL ────────────────────────────────────────────────────────────────────
  #
  # Per-slice SQLite databases. Reached via Drift::Backbone.sql(name). Wire
  # shape: one JSON envelope per call ({db, sql, args, tx?}).
  # See docs/memos/backbone-sql.md.
  #
  #   db = Drift::Backbone.sql("clinic")
  #   rows = db.query("SELECT * FROM appointments WHERE slot >= ?", ["2026-05-01"])
  #   res = db.execute("INSERT INTO appointments(...) VALUES(?, ?)", ["alice", "10:00"])
  #   db.transaction do |tx|
  #     tx.execute("UPDATE appointments SET status=? WHERE id=?", ["confirmed", 7])
  #   end
  #
  # SqlDb/SqlTx are implementation classes — reach them through Backbone.sql.

  class SqlDb
    def initialize(name)
      @name = name
    end

    def query(sql, args = [])
      resp = Drift._call('POST', 'sql/query',
                         { 'db' => @name, 'sql' => sql, 'args' => args }) || {}
      cols = resp['columns'] || []
      rows = resp['rows'] || []
      rows.map { |r| cols.zip(r).to_h }
    end

    def execute(sql, args = [])
      Drift._call('POST', 'sql/execute',
                  { 'db' => @name, 'sql' => sql, 'args' => args }) || {}
    end

    def begin
      resp = Drift._call('POST', 'sql/begin', { 'db' => @name }) || {}
      SqlTx.new(@name, resp['tx'])
    end

    def transaction
      tx = self.begin
      begin
        result = yield tx
        tx.commit
        result
      rescue StandardError
        begin
          tx.rollback
        rescue StandardError
          # idempotent: rollback failure is non-fatal
        end
        raise
      end
    end
  end

  class SqlTx
    def initialize(db, token)
      @db = db
      @token = token
    end

    def query(sql, args = [])
      resp = Drift._call('POST', 'sql/query',
                         { 'db' => @db, 'sql' => sql, 'args' => args, 'tx' => @token }) || {}
      cols = resp['columns'] || []
      rows = resp['rows'] || []
      rows.map { |r| cols.zip(r).to_h }
    end

    def execute(sql, args = [])
      Drift._call('POST', 'sql/execute',
                  { 'db' => @db, 'sql' => sql, 'args' => args, 'tx' => @token }) || {}
    end

    def commit
      Drift._call('POST', 'sql/commit', { 'tx' => @token })
    end

    def rollback
      Drift._call('POST', 'sql/rollback', { 'tx' => @token })
    end
  end

end
