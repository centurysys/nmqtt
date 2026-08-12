import std/[
  asyncdispatch,
  asyncnet,
  net,
  options,
  unittest,
]

import ../nmqtt_ng

const
  TestTimeoutMs = 3000

type
  TestBroker = ref object
    listener: AsyncSocket
    port: Port
    running: bool

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
proc handleClient(client: AsyncSocket) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")

    # MQTT 3.1.1 CONNACK: session present = 0, return code = accepted.
    await client.send("\x20\x02\x00\x00")

    while true:
      let (fixedHeader, body) = await client.recvPacket()
      let packetType = fixedHeader shr 4

      case packetType
      of 3:
        let msgId = publishMsgId(fixedHeader, body)
        var pubAck = newString(4)
        pubAck[0] = 0x40.char
        pubAck[1] = 0x02.char
        pubAck[2] = (msgId shr 8).uint8.char
        pubAck[3] = (msgId and 0xff).uint8.char
        await client.send(pubAck)
      of 12:
        # PINGRESP
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
    timeoutMs = TestTimeoutMs
): Future[bool] {.async.} =
  var elapsed = 0

  while elapsed < timeoutMs:
    if predicate():
      return true

    await sleepAsync(20)
    elapsed += 20

  result = predicate()

suite "PUBACK callback regression":
  test "publishId matches the PUBACK callback message ID":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttPubAckRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      await ctx.connect()
      check await waitUntil(proc(): bool = ctx.isConnected())

      var
        callbackCalled = false
        callbackMsgId: uint16
        callbackPktType = Notype

      proc onWork(msgId: uint16, pktType: PktType) =
        callbackCalled = true
        callbackMsgId = msgId
        callbackPktType = pktType

      check ctx.registerCallback(onWork)

      let published = await ctx.publishId(
        topic = "test/puback",
        message = "payload",
        qos = 1,
      )

      check published.isSome
      if published.isSome:
        let publishedMsgId = published.get()

        check await waitUntil(proc(): bool = callbackCalled)
        check callbackMsgId == publishedMsgId
        check callbackPktType == PubAck
        check ctx.msgQueue() == 0

      await ctx.disconnect()

    waitFor run()
