import std/[
  asyncdispatch,
  asyncnet,
  net,
  options,
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
    publishCount: int
    firstMsgId: uint16
    resentMsgId: uint16
    resentWithDup: bool

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
proc publishMsgId(body: string): uint16 =
  if body.len < 4:
    raise newException(ValueError, "MQTT PUBLISH packet is too short")

  let topicLen =
    (body[0].uint8.int shl 8) or
    body[1].uint8.int
  let offset = 2 + topicLen

  if body.len < offset + 2:
    raise newException(ValueError, "MQTT PUBLISH packet has no message ID")

  result =
    (body[offset].uint8.uint16 shl 8) or
    body[offset + 1].uint8.uint16

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
proc handleClient(
    broker: TestBroker,
    client: AsyncSocket,
    connectionNumber: int,
) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")

    await client.send("\x20\x02\x00\x00")

    while true:
      let (fixedHeader, body) = await client.recvPacket()
      let packetType = fixedHeader shr 4

      case packetType
      of 3:
        let msgId = publishMsgId(body)
        broker.publishCount.inc()

        if connectionNumber == 1:
          broker.firstMsgId = msgId
          # Leave QoS 1 work unacknowledged and drop the transport. The client
          # must resend it immediately after the next CONNACK.
          return

        broker.resentMsgId = msgId
        broker.resentWithDup = (fixedHeader and 0x08) != 0
        await client.sendPubAck(msgId)

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
      asyncCheck broker.handleClient(client, broker.connectionCount)

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

suite "Pending publish reconnect regression":
  test "resends an unacknowledged QoS 1 publish after reconnect":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttPublishReconnectRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      await ctx.start()
      check await waitUntil(proc(): bool = ctx.isConnected())

      let published = await ctx.publishId(
        topic = "test/pending",
        message = "payload",
        qos = 1,
      )

      check published.isSome
      check await waitUntil(proc(): bool = broker.publishCount >= 2)
      check broker.connectionCount >= 2
      check broker.firstMsgId != 0
      check broker.resentMsgId == broker.firstMsgId
      check broker.resentWithDup
      check await waitUntil(proc(): bool = ctx.msgQueue() == 0)
      check ctx.isConnected()

      await ctx.disconnect()

    waitFor run()
