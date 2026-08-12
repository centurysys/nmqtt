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
    connectionCount: int

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
proc handleClient(client: AsyncSocket, connectionNumber: int) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")

    # MQTT 3.1.1 CONNACK: session present = 0, return code = accepted.
    await client.send("\x20\x02\x00\x00")

    if connectionNumber == 1:
      # Simulate a broker/network-side disconnect after a valid MQTT session
      # has already been established. The client must leave Connected state
      # and let its reconnect worker create a fresh transport.
      await sleepAsync(100)
      return

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
      broker.connectionCount.inc()
      asyncCheck handleClient(client, broker.connectionCount)

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
    timeoutMs = TestTimeoutMs
): Future[bool] {.async.} =
  var elapsed = 0

  while elapsed < timeoutMs:
    if predicate():
      return true

    await sleepAsync(20)
    elapsed += 20

  result = predicate()

suite "Automatic reconnect regression":
  test "reconnects after an established broker connection is closed":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttReconnectRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      var connectedEvents = 0

      proc onState(connected: bool, publishState: PublishState) =
        if connected:
          connectedEvents.inc()

      check ctx.registerStateCallback(onState)

      await ctx.start()

      check await waitUntil(proc(): bool = connectedEvents >= 1)
      check await waitUntil(proc(): bool = connectedEvents >= 2)
      check broker.connectionCount >= 2
      check ctx.isConnected()

      await ctx.disconnect()

    waitFor run()
