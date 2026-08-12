import std/[
  options,
  unittest,
]

import ../nmqttngpkgs/work_queue

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc addPending(queue: WorkQueue, msgId: MsgId) =
  let work = newWork(
    wk = PubWork,
    typ = Publish,
    msgId = msgId,
    qos = 1,
    topic = "test/msg-id",
    message = "payload",
  )
  check queue.enqueue(work)

suite "MQTT message ID allocation":
  test "starts at one and never allocates zero":
    let queue = newWorkQueue()

    let next = queue.nextAvailableMsgId(MsgId(0))

    check next.isSome
    if next.isSome:
      check next.get() == MsgId(1)

  test "wraps from 65535 to one":
    let queue = newWorkQueue()

    let next = queue.nextAvailableMsgId(high(MsgId))

    check next.isSome
    if next.isSome:
      check next.get() == MsgId(1)

  test "skips message IDs that are still queued across wrap-around":
    let queue = newWorkQueue()
    queue.addPending(high(MsgId))
    queue.addPending(MsgId(1))

    let next = queue.nextAvailableMsgId(high(MsgId) - MsgId(1))

    check next.isSome
    if next.isSome:
      check next.get() == MsgId(2)
