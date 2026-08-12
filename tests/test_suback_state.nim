import std/[
  asyncdispatch,
  asyncnet,
  net,
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

proc sendSubAck(socket: AsyncSocket, msgId: uint16, returnCode: uint8) {.async.} =
  var packet = newString(5)
  packet[0] = 0x90.char
  packet[1] = 0x03.char
  packet[2] = (msgId shr 8).uint8.char
  packet[3] = (msgId and 0xff).uint8.char
  packet[4] = returnCode.char
  await socket.send(packet)

proc handleClient(client: AsyncSocket) {.async.} =
  try:
    let (connectHeader, _) = await client.recvPacket()
    if (connectHeader shr 4) != 1:
      raise newException(ValueError, "expected MQTT CONNECT packet")
    await client.send("\x20\x02\x00\x00")

    let (subHeader1, subBody1) = await client.recvPacket()
    if subHeader1 != 0x82'u8:
      raise newException(ValueError, "expected first MQTT SUBSCRIBE packet")

    let (subHeader2, subBody2) = await client.recvPacket()
    if subHeader2 != 0x82'u8:
      raise newException(ValueError, "expected second MQTT SUBSCRIBE packet")

    # Return one successful and one rejected subscription. Both acknowledgements
    # must release their queue entries and allow the publish watermark to recover.
    await client.sendSubAck(packetMsgId(subBody1), 1)
    await client.sendSubAck(packetMsgId(subBody2), 0x80)

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
      asyncCheck handleClient(client)
    except CatchableError:
      if not broker.running:
        break

proc newTestBroker(): TestBroker =
  let listener = newAsyncSocket(buffered = false)
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(0), "127.0.0.1")
  listener.listen()

  result = TestBroker(listener: listener, port: listener.getLocalAddr()[1], running: true)
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

suite "SUBACK handling regression":
  test "releases subscription work and restores publish readiness":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttSubAckStateRegression")
      ctx.setHost("127.0.0.1", broker.port.int)
      check ctx.setQueueWatermarks(2, 1)

      var
        connected = false
        publishState = psBlocked

      proc onState(isConnected: bool, state: PublishState) =
        connected = isConnected
        publishState = state

      check ctx.registerStateCallback(onState)
      await ctx.connect()
      check await waitUntil(proc(): bool = connected)

      proc ignoreMessage(topic: string, message: string) =
        discard

      await ctx.subscribe("sub/ok", 1, ignoreMessage)
      await ctx.subscribe("sub/rejected", 1, ignoreMessage)

      check await waitUntil(proc(): bool = publishState == psBlocked)
      check await waitUntil(proc(): bool = ctx.msgQueue() == 0)
      check await waitUntil(proc(): bool = publishState == psReady)

      await ctx.disconnect()

    waitFor run()
