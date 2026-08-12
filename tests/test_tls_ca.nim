import std/[
  asyncdispatch,
  asyncnet,
  net,
  os,
  unittest,
]

include "../nmqtt_ng.nim"

const
  TestTimeoutMs = 3000

type
  TlsTestServer = ref object
    listener: AsyncSocket
    sslContext: SslContext
    port: Port
    running: bool

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc tlsFixturePath(name: string): string =
  result = currentSourcePath().parentDir / "tls" / name

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc handleTlsClient(client: AsyncSocket) {.async.} =
  try:
    let connectPacket = await client.recv(4096)
    if connectPacket.len == 0:
      return

    # MQTT 3.1.1 CONNACK: session present = 0, return code = accepted.
    await client.send("\x20\x02\x00\x00")

    while true:
      let data = await client.recv(4096)
      if data.len == 0:
        break

  except CatchableError:
    discard

  finally:
    if not client.isClosed():
      client.close()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc runTlsTestServer(server: TlsTestServer) {.async.} =
  while server.running:
    try:
      let client = await server.listener.accept()
      wrapConnectedSocket(
        server.sslContext,
        client,
        handshakeAsServer,
      )
      asyncCheck handleTlsClient(client)

    except CatchableError:
      if not server.running:
        break

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc newTlsTestServer(): TlsTestServer =
  let
    # Use an unbuffered socket so recv() returns the MQTT CONNECT bytes
    # currently available instead of waiting to fill the requested buffer.
    listener = newAsyncSocket(buffered = false)
    sslContext = newContext(
      verifyMode = CVerifyNone,
      certFile = tlsFixturePath("server.crt"),
      keyFile = tlsFixturePath("server.key"),
    )

  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(0), "127.0.0.1")
  listener.listen()

  result = TlsTestServer(
    listener: listener,
    sslContext: sslContext,
    port: listener.getLocalAddr()[1],
    running: true,
  )

  asyncCheck result.runTlsTestServer()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc stop(server: TlsTestServer) =
  server.running = false
  if not server.listener.isClosed():
    server.listener.close()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc waitForState(
    ctx: MqttCtx,
    state: State,
    timeoutMs = TestTimeoutMs
): Future[bool] {.async.} =
  var elapsed = 0

  while elapsed < timeoutMs:
    if ctx.state == state:
      return true

    await sleepAsync(20)
    elapsed += 20

  result = ctx.state == state

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc connectWithTimeout(
    ctx: MqttCtx,
    timeoutMs = TestTimeoutMs
): Future[void] {.async.} =
  let connection = ctx.connect()

  if not await withTimeout(connection, timeoutMs):
    raise newException(IOError, "timed out while connecting to TLS test broker")

  await connection

suite "TLS CA verification":
  test "connects when the broker certificate is signed by the configured CA":
    proc run() {.async.} =
      let server = newTlsTestServer()
      defer:
        server.stop()

      let ctx = newMqttCtx("nmqttTlsValidCa")
      ctx.setHost("localhost", server.port.int, true)
      ctx.setSslCaFile(tlsFixturePath("ca.crt"))

      await ctx.connectWithTimeout()

      check await ctx.waitForState(Connected)
      check not ctx.s.isNil
      check not ctx.ssl.isNil

      await ctx.disconnect()

    waitFor run()

  test "rejects a broker certificate signed by another CA":
    proc run() {.async.} =
      let server = newTlsTestServer()
      defer:
        server.stop()

      let ctx = newMqttCtx("nmqttTlsWrongCa")
      ctx.setHost("localhost", server.port.int, true)
      ctx.setSslCaFile(tlsFixturePath("wrong_ca.crt"))

      var failed = false
      try:
        await ctx.connectWithTimeout()
      except CatchableError:
        failed = true

      check failed
      check ctx.state == Error
      check ctx.s.isNil
      check ctx.ssl.isNil

    waitFor run()

  test "rejects a certificate for a different hostname":
    proc run() {.async.} =
      let server = newTlsTestServer()
      defer:
        server.stop()

      let ctx = newMqttCtx("nmqttTlsWrongHost")
      ctx.setHost("127.0.0.1", server.port.int, true)
      ctx.setSslCaFile(tlsFixturePath("ca.crt"))

      var failed = false
      try:
        await ctx.connectWithTimeout()
      except CatchableError:
        failed = true

      check failed
      check ctx.state == Error
      check ctx.s.isNil
      check ctx.ssl.isNil

    waitFor run()

  test "keeps legacy unverified TLS behavior when no CA file is configured":
    proc run() {.async.} =
      let server = newTlsTestServer()
      defer:
        server.stop()

      let ctx = newMqttCtx("nmqttTlsNoCa")
      ctx.setHost("127.0.0.1", server.port.int, true)

      await ctx.connectWithTimeout()

      check await ctx.waitForState(Connected)
      await ctx.disconnect()

    waitFor run()

  test "automatic reconnect recovers after the CA configuration is corrected":
    proc run() {.async.} =
      let server = newTlsTestServer()
      defer:
        server.stop()

      let ctx = newMqttCtx("nmqttTlsReconnect")
      ctx.setHost("localhost", server.port.int, true)
      ctx.setSslCaFile(tlsFixturePath("wrong_ca.crt"))

      await ctx.start()
      check await ctx.waitForState(Error)

      ctx.setSslCaFile(tlsFixturePath("ca.crt"))

      check await ctx.waitForState(Connected, 5000)
      await ctx.disconnect()

    waitFor run()
