import std/[
  asyncdispatch,
  asyncnet,
  net,
  times,
  unittest,
]

import ../nmqtt_ng

const
  TestTimeoutMs = 8000


type
  TestBroker = ref object
    listener: AsyncSocket
    port: Port
    running: bool
    connectionCount: int
    connectionTimes: seq[float]

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
proc publishMsgId(fixedHeader: uint8, body: string): uint16 =
  let qos = (fixedHeader shr 1) and 0x03
  if qos == 0:
    raise newException(ValueError, "PUBLISH packet does not contain a message ID")

  if body.len < 4:
    raise newException(ValueError, "PUBLISH packet is too short")

  let topicLength = (body[0].uint8.uint16 shl 8) or body[1].uint8.uint16
  let msgIdOffset = 2 + topicLength.int

  if body.len < msgIdOffset + 2:
    raise newException(ValueError, "PUBLISH packet does not contain a complete message ID")

  result =
    (body[msgIdOffset].uint8.uint16 shl 8) or
    body[msgIdOffset + 1].uint8.uint16

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc sendPubAck(client: AsyncSocket, msgId: uint16) {.async.} =
  var pubAck = newString(4)
  pubAck[0] = 0x40.char
  pubAck[1] = 0x02.char
  pubAck[2] = (msgId shr 8).uint8.char
  pubAck[3] = (msgId and 0xff).uint8.char
  await client.send(pubAck)

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc handleClient(client: AsyncSocket, connectionNumber: int) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")

    await client.send("\x20\x02\x00\x00")

    if connectionNumber == 1:
      # The first connection becomes Connected but performs no acknowledged
      # MQTT exchange. Its disconnect therefore advances reconnect backoff.
      await sleepAsync(100)
      return

    if connectionNumber == 2:
      # A valid QoS 1 PUBACK proves that the MQTT session is operational. The
      # following disconnect must therefore restart backoff from one second.
      while true:
        let (fixedHeader, body) = await client.recvPacket()
        case fixedHeader shr 4
        of 3:
          let msgId = publishMsgId(fixedHeader, body)
          await client.sendPubAck(msgId)
          await sleepAsync(100)
          return
        of 12:
          await client.send("\xD0\x00")
        of 14:
          return
        else:
          discard

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
      broker.connectionTimes.add(epochTime())
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
    timeoutMs = TestTimeoutMs,
): Future[bool] {.async.} =
  var elapsed = 0

  while elapsed < timeoutMs:
    if predicate():
      return true

    await sleepAsync(20)
    elapsed += 20

  result = predicate()

suite "Operational session reconnect backoff regression":
  test "PUBACK resets reconnect backoff after a usable MQTT session":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttOperationalBackoffRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      var connectedEvents = 0

      proc onState(connected: bool, publishState: PublishState) =
        if connected:
          connectedEvents.inc()

      check ctx.registerStateCallback(onState)

      await ctx.start()

      check await waitUntil(proc(): bool = connectedEvents >= 2)

      await ctx.publish(
        topic = "test/operational",
        message = "payload",
        qos = 1,
      )

      check await waitUntil(proc(): bool = broker.connectionCount >= 3)
      check await waitUntil(proc(): bool = ctx.isConnected())
      check broker.connectionTimes.len >= 3

      let
        firstRetryDelay = broker.connectionTimes[1] - broker.connectionTimes[0]
        operationalRetryDelay = broker.connectionTimes[2] - broker.connectionTimes[1]

      check firstRetryDelay >= 0.8
      check operationalRetryDelay >= 0.8
      check operationalRetryDelay < 1.8

      await ctx.disconnect()

    waitFor run()
