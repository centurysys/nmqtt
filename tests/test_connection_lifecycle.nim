import std/[
  asyncdispatch,
  asyncnet,
  net,
  tables,
  unittest,
]

import ../nmqtt_ng

const
  TestTimeoutMs = 8000

type
  BrokerMode = enum
    bmStable
    bmReconnectBackoff
    bmPendingConnAck

  TestBroker = ref object
    listener: AsyncSocket
    port: Port
    running: bool
    mode: BrokerMode
    connectionCount: int
    connectPacketSeen: bool
    clients: Table[int, AsyncSocket]

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
proc runStableSession(client: AsyncSocket) {.async.} =
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

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc handleClient(
    broker: TestBroker,
    client: AsyncSocket,
    connectionNumber: int,
) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")

    broker.connectPacketSeen = true

    case broker.mode
    of bmStable:
      await client.runStableSession()

    of bmReconnectBackoff:
      case connectionNumber
      of 1:
        # Establish one valid session, then simulate a remote transport loss.
        await client.send("\x20\x02\x00\x00")
        await sleepAsync(100)

      of 2:
        # Reject the first reconnect. The client enters the reconnect-policy
        # delay here; an explicit disconnect during that delay must invalidate
        # the worker before it can create connection #3.
        await client.send("\x20\x02\x00\x05")

      else:
        await client.runStableSession()

    of bmPendingConnAck:
      if connectionNumber == 1:
        # Keep the MQTT CONNECT unanswered. The explicit disconnect in the test
        # must make both the CONNACK watchdog and reconnect worker stale.
        while true:
          let (fixedHeader, _) = await client.recvPacket()
          if (fixedHeader shr 4) == 14:
            break
      else:
        # Any later connection is already a regression, but make it a valid
        # session so the test cannot hang if one is accidentally created.
        await client.runStableSession()

  except CatchableError:
    discard

  finally:
    if broker.clients.hasKey(connectionNumber) and
        broker.clients[connectionNumber] == client:
      broker.clients.del(connectionNumber)

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
      let connectionNumber = broker.connectionCount
      broker.clients[connectionNumber] = client
      asyncCheck broker.handleClient(client, connectionNumber)

    except CatchableError:
      if not broker.running:
        break

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc newTestBroker(mode: BrokerMode): TestBroker =
  let listener = newAsyncSocket(buffered = false)

  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(0), "127.0.0.1")
  listener.listen()

  result = TestBroker(
    listener: listener,
    port: listener.getLocalAddr()[1],
    running: true,
    mode: mode,
    clients: initTable[int, AsyncSocket](),
  )

  asyncCheck result.runBroker()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc closeConnection(broker: TestBroker, connectionNumber: int) =
  if not broker.clients.hasKey(connectionNumber):
    return

  let client = broker.clients[connectionNumber]
  if not client.isClosed():
    client.close()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc stop(broker: TestBroker) =
  broker.running = false

  if not broker.listener.isClosed():
    broker.listener.close()

  for _, client in broker.clients.pairs:
    if not client.isClosed():
      client.close()

  broker.clients.clear()

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

suite "MQTT connection lifecycle stress regression":
  test "explicit disconnect cancels a reconnect backoff":
    proc run() {.async.} =
      let broker = newTestBroker(bmReconnectBackoff)
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttDisconnectBackoffRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      await ctx.start()

      check await waitUntil(proc(): bool = ctx.isConnected())
      check await waitUntil(proc(): bool = broker.connectionCount >= 2)

      # Give the rejected CONNACK time to reach runRx() and register its retry
      # delay, but stop well before the one-second reconnect deadline.
      await sleepAsync(150)
      check broker.connectionCount == 2

      await ctx.disconnect()
      check not ctx.isConnected()

      let connectionsAtStop = broker.connectionCount

      # Wait beyond the initial reconnect delay and one runConnect polling
      # interval. A stale reconnect worker must not create connection #3.
      await sleepAsync(1300)

      check broker.connectionCount == connectionsAtStop
      check not ctx.isConnected()

    waitFor run()

  test "explicit disconnect cancels a pending CONNACK watchdog":
    proc run() {.async.} =
      let broker = newTestBroker(bmPendingConnAck)
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttPendingConnAckDisconnectRegression")
      ctx.setHost("127.0.0.1", broker.port.int)
      ctx.setConnAckTimeout(250)

      await ctx.start()

      check await waitUntil(
        proc(): bool =
          broker.connectionCount == 1 and broker.connectPacketSeen
      )

      # Disconnect while the transport is still in Connecting. The watchdog
      # wakes after this point, but its lifecycle generation is already stale.
      await ctx.disconnect()
      check not ctx.isConnected()

      await sleepAsync(1300)

      check broker.connectionCount == 1
      check not ctx.isConnected()

    waitFor run()

  test "repeated stop and start leaves exactly one reconnect worker":
    proc run() {.async.} =
      let broker = newTestBroker(bmStable)
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttLifecycleStressRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      await ctx.start()
      check await waitUntil(proc(): bool = ctx.isConnected())
      check broker.connectionCount == 1

      for cycle in 1 .. 5:
        await ctx.disconnect()
        check not ctx.isConnected()

        # A second disconnect while already disabled must be harmless while
        # still invalidating any worker that somehow survived the first one.
        await ctx.disconnect()
        check not ctx.isConnected()

        await ctx.start()
        check await waitUntil(
          proc(): bool =
            broker.connectionCount >= cycle + 1 and ctx.isConnected()
        )

        await sleepAsync(50)
        check broker.connectionCount == cycle + 1
        check ctx.isConnected()

      let activeConnection = broker.connectionCount

      # Drop the final live transport from the broker side. Exactly one current
      # runConnect worker must create exactly one replacement connection.
      broker.closeConnection(activeConnection)

      check await waitUntil(
        proc(): bool =
          broker.connectionCount >= activeConnection + 1 and ctx.isConnected()
      )

      # Give all stale workers more than one polling interval to prove that no
      # second replacement connection appears later.
      await sleepAsync(1200)

      check broker.connectionCount == activeConnection + 1
      check ctx.isConnected()

      await ctx.disconnect()

    waitFor run()
