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
    subscribeCount: int
    resubscribeCount: int
    pendingPublishReceived: bool

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
proc packetMsgId(body: string): uint16 =
  if body.len < 2:
    raise newException(ValueError, "MQTT packet does not contain a message ID")

  result =
    (body[0].uint8.uint16 shl 8) or
    body[1].uint8.uint16

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc sendSubAck(client: AsyncSocket, msgId: uint16, qos: uint8) {.async.} =
  var subAck = newString(5)
  subAck[0] = 0x90.char
  subAck[1] = 0x03.char
  subAck[2] = (msgId shr 8).uint8.char
  subAck[3] = (msgId and 0xff).uint8.char
  subAck[4] = qos.char
  await client.send(subAck)

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

    # MQTT 3.1.1 CONNACK: session present = 0, return code = accepted.
    await client.send("\x20\x02\x00\x00")

    while true:
      let (fixedHeader, body) = await client.recvPacket()
      let packetType = fixedHeader shr 4

      case packetType
      of 3:
        if connectionNumber == 1:
          # Keep this QoS 1 publish pending by deliberately not sending PUBACK,
          # then drop the transport. The reconnect must still restore the
          # subscription even though publish work remains in the queue.
          broker.pendingPublishReceived = true
          return
      of 8:
        let msgId = packetMsgId(body)
        broker.subscribeCount.inc()
        if connectionNumber >= 2:
          broker.resubscribeCount.inc()
        await client.sendSubAck(msgId, 1)
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

suite "Subscription reconnect regression":
  test "restores subscriptions while QoS 1 publish work is pending":
    proc run() {.async.} =
      let broker = newTestBroker()
      defer:
        broker.stop()

      let ctx = newMqttCtx("nmqttResubscribeRegression")
      ctx.setHost("127.0.0.1", broker.port.int)

      await ctx.start()
      check await waitUntil(proc(): bool = ctx.isConnected())

      proc onMessage(topic: string, message: string) =
        discard

      await ctx.subscribe("test/reconnect", 1, onMessage)
      check await waitUntil(proc(): bool = broker.subscribeCount >= 1)

      let published = await ctx.publishId(
        topic = "test/pending",
        message = "payload",
        qos = 1,
      )

      check published.isSome
      check await waitUntil(proc(): bool = broker.pendingPublishReceived)
      check await waitUntil(proc(): bool = broker.connectionCount >= 2)
      check await waitUntil(proc(): bool = broker.resubscribeCount >= 1)
      check ctx.isConnected()
      check ctx.msgQueue() >= 1

      await ctx.disconnect()

    waitFor run()
