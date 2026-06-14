# Drift SDK for Ruby Atomic functions.
#
# This single-file SDK provides:
#   - Drift.run(handler): Entry point that dispatches to deployed or local mode.
#   - Backbone helpers: Secret, Cache, Nosql, Queue, Blob, Lock.
#   - Drift.log(msg): Writes to stderr (captured by the runner as function logs).
#   - Drift.http_request(): Outbound HTTP from within a function.
#
# All backbone helpers use only stdlib (net/http) -- zero external dependencies.

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
        'method' => http_req.request_method,
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

  # _backbone_http issues a request to the backbone over either a Unix Domain
  # Socket (BACKBONE_URL=unix:///path — the slice default: lower latency, no TCP
  # surface) or TCP (http://host:port, used by `drift atomic run`). Returns
  # [status_code, body]. Stdlib only — Socket for UDS, Net::HTTP for TCP.
  def self._backbone_http(method, path, body, content_type)
    base = _get_backbone_url
    if base.start_with?('unix://')
      _backbone_uds(base.sub('unix://', ''), method, "/#{path}", body, content_type)
    else
      uri = URI("#{base}/#{path}")
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = uri.scheme == 'https'
      req = Net::HTTP.const_get(method.capitalize).new(uri)
      if body
        req['Content-Type'] = content_type
        req.body = body
      end
      resp = http.request(req)
      [resp.code.to_i, resp.body]
    end
  end

  def self._backbone_uds(sock_path, method, path, body, content_type)
    sock = UNIXSocket.new(sock_path)
    begin
      out = +"#{method} #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
      out << "Content-Type: #{content_type}\r\nContent-Length: #{body.bytesize}\r\n" if body
      out << "\r\n"
      out << body if body
      sock.write(out)
      raw = sock.read # Connection: close → server closes after the response
    ensure
      sock.close
    end
    _parse_http(raw || ''.b)
  end

  # _parse_http splits a raw HTTP/1.1 response into [status_code, body],
  # decoding Transfer-Encoding: chunked when present (Content-Length otherwise).
  def self._parse_http(raw)
    head, _, rest = raw.partition("\r\n\r\n")
    lines = head.split("\r\n")
    status = (lines[0].to_s.split(' ')[1] || '0').to_i
    if lines.any? { |l| d = l.downcase; d.start_with?('transfer-encoding:') && d.include?('chunked') }
      body = ''.b
      buf = rest
      loop do
        nl = buf.index("\r\n")
        break unless nl
        size = buf[0...nl].to_i(16)
        break if size <= 0
        start = nl + 2
        body << buf[start, size].to_s
        buf = buf[(start + size + 2)..] || ''
      end
      [status, body]
    else
      [status, rest]
    end
  end

  def self._call(method, path, body = nil)
    return _call_local(method, path, body) if _get_backbone_url.empty?
    code, resp_body = _backbone_http(method, path, body ? JSON.generate(body) : nil, 'application/json')
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
    (code && code >= 200 && code < 300) ? resp_body : nil
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

  # ---------------------------------------------------------------------------
  # Secret
  # ---------------------------------------------------------------------------

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

  # ---------------------------------------------------------------------------
  # Cache
  # ---------------------------------------------------------------------------

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

  # ---------------------------------------------------------------------------
  # NoSQL
  # ---------------------------------------------------------------------------

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

  # ---------------------------------------------------------------------------
  # Queue
  # ---------------------------------------------------------------------------

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

  # ---------------------------------------------------------------------------
  # Blob
  # ---------------------------------------------------------------------------

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
      (code && code >= 200 && code < 300) ? body : nil
    end
  end

  # ---------------------------------------------------------------------------
  # Lock
  # ---------------------------------------------------------------------------

  module Lock
    def self.acquire(name, ttl:)
      resp = Drift._call('POST', 'lock/acquire', { 'name' => name, 'ttl' => ttl })
      (resp || {})['token'] || ''
    end

    def self.release(name, token)
      Drift._call('POST', 'lock/release', { 'name' => name, 'token' => token })
    end
  end

  # ---------------------------------------------------------------------------
  # JWT primitive
  # ---------------------------------------------------------------------------
  #
  # HS256 minting + verification, signed with the slice's per-slice JKey. The
  # signing key never leaves the slice's backbone process; all operations flow
  # through loopback HTTP to backbone /jwt/{sign,verify,slice-id}.
  #
  # Design: internal/todo/slice-jwt-primitive.md.

  # Raised by Drift::JWT.verify on validation failure. `reason` is one of the
  # stable wire strings: malformed, bad_signature, expired, not_yet_valid,
  # wrong_algorithm, wrong_issuer, wrong_audience, invalid_claims,
  # missing_exp, internal_error.
  class JWTError < StandardError
    attr_reader :reason
    def initialize(reason)
      super("jwt verify: #{reason}")
      @reason = reason
    end
  end

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
      resp = Drift._call('POST', 'jwt/sign', body)
      (resp || {})['token']
    end

    # Validate a token. Returns the parsed claims hash on success;
    # raises Drift::JWTError on validation failure.
    def self.verify(token, audience: nil, allowed_issuer: nil)
      body = { 'token' => token }
      body['audience']       = audience       unless audience.nil?
      body['allowed_issuer'] = allowed_issuer unless allowed_issuer.nil?
      resp = Drift._call('POST', 'jwt/verify', body)
      raise JWTError.new('internal_error') unless resp.is_a?(Hash)
      raise JWTError.new(resp['reason'] || 'internal_error') unless resp['valid']
      resp['claims'] || {}
    end

    # The slice's auto-set issuer string ("drift-slice-<user>-<slice>").
    def self.slice_id
      resp = Drift._call('GET', 'jwt/slice-id')
      resp.is_a?(Hash) ? (resp['slice_id'] || '') : ''
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
          method, path_query, _ = request_line.split(' ', 3)
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
            'method' => method,
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

end

module Drift

  # ─── SQL ────────────────────────────────────────────────────────────────────
  #
  # Per-slice SQLite databases. Wire shape: one JSON envelope per call
  # ({db, sql, args, tx?}). See docs/memos/backbone-sql.md.
  #
  #   db = Drift.sql("clinic")
  #   rows = db.query("SELECT * FROM appointments WHERE slot >= ?", ["2026-05-01"])
  #   res = db.execute("INSERT INTO appointments(...) VALUES(?, ?)", ["alice", "10:00"])
  #   db.transaction do |tx|
  #     tx.execute("UPDATE appointments SET status=? WHERE id=?", ["confirmed", 7])
  #   end
  #

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

  def self.sql(name)
    SqlDb.new(name)
  end
end
