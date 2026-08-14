import std/[
  asyncdispatch,
  asyncnet,
  net,
  unittest,
]

import ../nmqtt_ng

const
  TestTimeoutMs = 5000

type
  TestBroker = ref object
    listener: AsyncSocket
    port: Port
    running: bool
    acceptedConnections: int

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc recvExact(socket: AsyncSocket, length: int): Future[string] {.async.} =
  result = newStringOfCap(length)

  while result.len < length:
    let chunk = await socket.recv(length - result.len)
    if chunk.len == 0:
      raise newException(IOError, "unexpected EOF from MQTT client")
    result.add(chunk)

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc recvRemainingLength(socket: AsyncSocket): Future[int] {.async.} =
  var
    multiplier = 1
    value = 0

  for _ in 0 ..< 4:
    let byteValue = (await socket.recvExact(1))[0].uint8
    value += (byteValue and 0x7f).int * multiplier

    if (byteValue and 0x80) == 0:
      return value

    multiplier *= 128

  raise newException(IOError, "invalid MQTT remaining length")

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc recvPacket(socket: AsyncSocket): Future[(uint8, string)] {.async.} =
  let
    fixedHeader = (await socket.recvExact(1))[0].uint8
    remainingLength = await socket.recvRemainingLength()
    body = await socket.recvExact(remainingLength)

  result = (fixedHeader, body)

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc handleClient(client: AsyncSocket) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")

    await client.send("\x20\x02\x00\x00")

    while true:
      let (fixedHeader, _) = await client.recvPacket()
      case fixedHeader shr 4
      of 12:
        await client.send("\xD0\x00")
      of 14:
        break
      else:
        discard

  except CatchableError:
    discard

  finally:
    if not client.isClosed():
      client.close()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc runBroker(broker: TestBroker) {.async.} =
  while broker.running:
    try:
      let client = await broker.listener.accept()
      broker.acceptedConnections.inc()
      asyncCheck handleClient(client)

    except CatchableError:
      if not broker.running:
        break

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc newTestBroker(): TestBroker =
  let listener = newAsyncSocket(buffered = false)

  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(0), "127.0.0.1")
  listener.listen()

  result = TestBroker(
    listener: listener,
    port: listener.getLocalAddr()[1],
    running: true,
  )

  asyncCheck result.runBroker()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc stop(broker: TestBroker) =
  broker.running = false
  if not broker.listener.isClosed():
    broker.listener.close()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc waitUntil(
    predicate: proc(): bool,
    timeoutMs = TestTimeoutMs,
): Future[bool] {.async.} =
  var elapsed = 0

  while elapsed < timeoutMs:
    if predicate():
      return true

    await sleepAsync(20)
    elapsed += 20

  result = predicate()

suite "Explicit disconnect state regression":
  test "reports publish state blocked after explicit disconnect":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttDisconnectStateRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      var
        lastConnected = false
        lastPublishState = psBlocked
        stateEvents = 0

      proc onState(connected: bool, publishState: PublishState) =
        lastConnected = connected
        lastPublishState = publishState
        stateEvents.inc()

      check ctx.registerStateCallback(onState)

      await ctx.start()
      check await waitUntil(proc(): bool = ctx.isConnected())

      let eventsBeforeDisconnect = stateEvents
      await ctx.disconnect()

      check not ctx.isConnected()
      check stateEvents > eventsBeforeDisconnect
      check not lastConnected
      check lastPublishState == psBlocked

    waitFor run()

  test "keeps a restarted connection alive after explicit disconnect":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttDisconnectRestartRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      await ctx.start()
      check await waitUntil(proc(): bool = ctx.isConnected())

      for cycle in 1 .. 3:
        await ctx.disconnect()
        check not ctx.isConnected()

        await ctx.start()
        check await waitUntil(proc(): bool = ctx.isConnected())

        # Give the receive worker from the previous transport time to finish.
        # A stale worker must not clean up the newly connected socket.
        await sleepAsync(100)
        check ctx.isConnected()
        check broker.acceptedConnections >= cycle + 1

      await ctx.disconnect()
      check not ctx.isConnected()

    waitFor run()

