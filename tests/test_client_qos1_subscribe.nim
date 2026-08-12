import std/[
  asyncdispatch,
  asyncnet,
  net,
  options,
  unittest,
]

import ../nmqtt_ng

const
  TestTimeoutMs = 4000

type
  TestBroker = ref object
    listener: AsyncSocket
    port: Port
    running: bool
    incomingPubAckOk: bool
    incomingPubAckMsgId: uint16

proc recvExact(socket: AsyncSocket, length: int): Future[string] {.async.} =
  result = newStringOfCap(length)
  while result.len < length:
    let chunk = await socket.recv(length - result.len)
    if chunk.len == 0:
      raise newException(IOError, "unexpected EOF from MQTT client")
    result.add(chunk)

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

proc recvPacket(socket: AsyncSocket): Future[(uint8, string)] {.async.} =
  let
    fixedHeader = (await socket.recvExact(1))[0].uint8
    remainingLength = await socket.recvRemainingLength()
    body = await socket.recvExact(remainingLength)
  result = (fixedHeader, body)

proc packetMsgId(body: string): uint16 =
  if body.len < 2:
    raise newException(ValueError, "MQTT packet does not contain a message ID")
  result = (body[0].uint8.uint16 shl 8) or body[1].uint8.uint16

proc publishMsgId(fixedHeader: uint8, body: string): uint16 =
  let qos = (fixedHeader shr 1) and 0x03
  if qos == 0 or body.len < 4:
    raise newException(ValueError, "PUBLISH packet does not contain a message ID")

  let topicLength = (body[0].uint8.uint16 shl 8) or body[1].uint8.uint16
  let msgIdOffset = 2 + topicLength.int
  if body.len < msgIdOffset + 2:
    raise newException(ValueError, "PUBLISH packet contains an incomplete message ID")

  result =
    (body[msgIdOffset].uint8.uint16 shl 8) or
    body[msgIdOffset + 1].uint8.uint16

proc sendSubAck(socket: AsyncSocket, msgId: uint16, qos: uint8) {.async.} =
  var packet = newString(5)
  packet[0] = 0x90.char
  packet[1] = 0x03.char
  packet[2] = (msgId shr 8).uint8.char
  packet[3] = (msgId and 0xff).uint8.char
  packet[4] = qos.char
  await socket.send(packet)

proc sendPubAck(socket: AsyncSocket, msgId: uint16) {.async.} =
  var packet = newString(4)
  packet[0] = 0x40.char
  packet[1] = 0x02.char
  packet[2] = (msgId shr 8).uint8.char
  packet[3] = (msgId and 0xff).uint8.char
  await socket.send(packet)

proc sendIncomingPublish(socket: AsyncSocket, msgId: uint16) {.async.} =
  const
    Topic = "server/in"
    Payload = "ack"

  var body = newStringOfCap(2 + Topic.len + 2 + Payload.len)
  body.add(((Topic.len shr 8) and 0xff).uint8.char)
  body.add((Topic.len and 0xff).uint8.char)
  body.add(Topic)
  body.add((msgId shr 8).uint8.char)
  body.add((msgId and 0xff).uint8.char)
  body.add(Payload)

  if body.len >= 128:
    raise newException(ValueError, "test PUBLISH packet unexpectedly too large")

  var packet = newStringOfCap(2 + body.len)
  packet.add(0x32.char) # PUBLISH, QoS 1, DUP=0, RETAIN=0
  packet.add(body.len.uint8.char)
  packet.add(body)
  await socket.send(packet)

proc handleClient(broker: TestBroker, client: AsyncSocket) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")
    await client.send("\x20\x02\x00\x00")

    let (subscribeHeader, subscribeBody) = await client.recvPacket()
    if subscribeHeader != 0x82'u8:
      raise newException(ValueError, "expected MQTT SUBSCRIBE packet")
    let subscribeMsgId = packetMsgId(subscribeBody)
    await client.sendSubAck(subscribeMsgId, 1)

    let (publishHeader, publishBody) = await client.recvPacket()
    if (publishHeader shr 4) != 3 or ((publishHeader shr 1) and 0x03) != 1:
      raise newException(ValueError, "expected outgoing QoS 1 PUBLISH packet")
    let outgoingMsgId = publishMsgId(publishHeader, publishBody)

    # MQTT defines client-to-server and server-to-client packet identifiers as
    # independent namespaces. Exercise the legal case where both directions use
    # the same value while the client's outgoing PUBLISH is still unacknowledged.
    await client.sendIncomingPublish(outgoingMsgId)

    let (pubAckHeader, pubAckBody) = await client.recvPacket()
    broker.incomingPubAckMsgId = packetMsgId(pubAckBody)
    broker.incomingPubAckOk =
      pubAckHeader == 0x40'u8 and broker.incomingPubAckMsgId == outgoingMsgId

    # Now acknowledge the client's original PUBLISH. Its work item must still
    # exist; the incoming server-side packet identifier must not have collided
    # with or replaced it.
    await client.sendPubAck(outgoingMsgId)

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

proc runBroker(broker: TestBroker) {.async.} =
  while broker.running:
    try:
      let client = await broker.listener.accept()
      asyncCheck broker.handleClient(client)
    except CatchableError:
      if not broker.running:
        break

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

proc stop(broker: TestBroker) =
  broker.running = false
  if not broker.listener.isClosed():
    broker.listener.close()

proc waitUntil(predicate: proc(): bool, timeoutMs = TestTimeoutMs): Future[bool] {.async.} =
  var elapsed = 0
  while elapsed < timeoutMs:
    if predicate():
      return true
    await sleepAsync(20)
    elapsed += 20
  result = predicate()

suite "Client incoming QoS 1 regression":
  test "acknowledges subscribed QoS 1 publish without packet ID collision":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttClientQos1Regression")
      ctx.setHost("127.0.0.1", broker.port.int)

      var receivedMessage = false
      proc onMessage(topic: string, message: string) =
        if topic == "server/in" and message == "ack":
          receivedMessage = true

      await ctx.connect()
      check await waitUntil(proc(): bool = ctx.isConnected())
      await ctx.subscribe("server/in", 1, onMessage)

      let published = await ctx.publishId("client/out", "payload", qos = 1)
      check published.isSome

      check await waitUntil(proc(): bool = receivedMessage)
      check await waitUntil(proc(): bool = broker.incomingPubAckOk)
      check await waitUntil(proc(): bool = ctx.msgQueue() == 0)

      if published.isSome:
        check broker.incomingPubAckMsgId == published.get()

      await ctx.disconnect()

    waitFor run()
