import std/lists
import std/monotimes
import std/options
import std/strformat
import std/tables
import std/times

type
  MsgId* = uint16
  Qos* = range[0 .. 2]
  PktType* = enum
    Notype      =  0
    Connect     =  1
    ConnAck     =  2
    Publish     =  3
    PubAck      =  4
    PubRec      =  5
    PubRel      =  6
    PubComp     =  7
    Subscribe   =  8
    SubAck      =  9
    Unsubscribe = 10
    Unsuback    = 11
    PingReq     = 12
    PingResp    = 13
    Disconnect  = 14
  WorkKind* = enum
    PubWork
    SubWork
  WorkState* = enum
    WorkNew
    WorkSent
    WorkAcked
  PubCallback* = object
    cb*: proc(topic: string, message: string)
    qos*: Qos
  WorkCallback* = object
    cb*: proc(msgId: uint16, pktType: PktType)
  WorkMeta = object
    createdAt: MonoTime
    lastSentAt: MonoTime
    retryCount: uint16
  WorkObj = object
    state*: WorkState
    msgId*: MsgId
    topic*: string
    qos*: Qos
    dup*: bool
    typ*: PktType
    flags*: uint16 #when defined(broker)
    meta*: WorkMeta
    case wk*: WorkKind
    of PubWork:
      retain*: bool
      message*: string
    of SubWork:
      discard
  Work* = ref WorkObj
  WorkQueueObj* = object
    list: DoublyLinkedList[Work]
    byId: TableRef[MsgId, DoublyLinkedNode[Work]]
  WorkQueue* = ref WorkQueueObj

# ------------------------------------------------------------------------------
# Constructor:
# ------------------------------------------------------------------------------
proc newWork*(wk: WorkKind, typ: PktType, msgId = MsgId(0), topic = "",
    qos = Qos(0), retain = false, message = "", flags: uint16 = 0,
    state = WorkNew): Work {.inline.} =
  result = Work(wk: wk, msgId: msgId, topic: topic, qos: qos, typ: typ,
      flags: flags, state: state)
  case wk
  of PubWork:
    result.retain = retain
    result.message = message
  else:
    discard
  result.meta.createdAt = getMonoTime()

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc setLastSentAt*(self: Work, incRetry: bool = false) =
  self.meta.lastSentAt = getMonoTime()
  if incRetry:
    self.meta.retryCount.inc()

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc getDuration(self: Work): Duration {.inline.} =
  let now = getMonoTime()
  result = now - self.meta.lastSentAt

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc elapsedInMilliseconds*(self: Work): int64 =
  result = self.getDuration.inMilliseconds()

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc elapsedInSeconds*(self: Work): int64 =
  result = self.getDuration.inSeconds()

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc `$`*(self: Work): string =
  result = &"Work[msgId: {self.msgId}, lastSent: {self.meta.lastSentAt}," &
      &" elapsed: {self.elapsedInMilliseconds} msec," &
      &" retryCount: {self.meta.retryCount}]"

# ------------------------------------------------------------------------------
# Constructor:
# ------------------------------------------------------------------------------
proc newWorkQueue*(): WorkQueue =
  new result
  result.list = initDoublyLinkedList[Work]()
  result.byId = newTable[MsgId, DoublyLinkedNode[Work]](128)

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc contains*(self: WorkQueue, msgId: MsgId): bool =
  result = self.byId.contains(msgId)

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc contains*(self: WorkQueue, work: Work): bool =
  let msgId = work.msgId
  result = self.byId.contains(msgId)

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc nextAvailableMsgId*(self: WorkQueue, current: MsgId): Option[MsgId] =
  ## Return the next MQTT packet identifier that is not currently in use.
  ##
  ## MQTT packet identifiers are in the range 1..65535. Identifier 0 is
  ## reserved and must never be allocated. IDs that are still present in the
  ## work queue are also skipped so an unacknowledged packet cannot collide
  ## with a newly queued packet after sequence wrap-around.
  var candidate = current

  for _ in 0 ..< high(MsgId).int:
    if candidate == high(MsgId):
      candidate = MsgId(1)
    else:
      candidate.inc()

    if not self.contains(candidate):
      return some(candidate)

  result = none(MsgId)

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc enqueue*(self: WorkQueue, work: Work): bool =
  if self.contains(work) or work.msgId == 0:
    return
  let node = newDoublyLinkedNode[Work](work)
  self.list.append(node)
  self.byId[work.msgId] = node
  result = true

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
proc getEntry(self: WorkQueue, msgId: MsgId): Option[DoublyLinkedNode[Work]] =
  if self.contains(msgId):
    result = some(self.byId[msgId])

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc get*(self: WorkQueue, msgId: MsgId): Option[Work] =
  let entry_opt = self.getEntry(msgId)
  if entry_opt.isSome:
    let work = entry_opt.get.value
    result = some(work)

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc remove*(self: WorkQueue, msgId: MsgId): bool =
  let entry_opt = self.getEntry(msgId)
  if entry_opt.isSome:
    let entry = entry_opt.get()
    self.list.remove(entry)
    self.byId.del(msgId)
    result = true

# ------------------------------------------------------------------------------
# API: get and remove
# ------------------------------------------------------------------------------
proc pop*(self: WorkQueue, msgId: MsgId): Option[Work] =
  let work_opt = self.get(msgId)
  if work_opt.isSome:
    discard self.remove(msgId)
    result = work_opt

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc moveToBack*(self: WorkQueue, msgId: MsgId, resend: bool = false): bool =
  let work_opt = self.pop(msgId)
  if work_opt.isSome:
    let work = work_opt.get()
    if resend:
      work.setLastSentAt(incRetry = true)
    result = self.enqueue(work)

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc len*(self: WorkQueue): int =
  result = self.byId.len()

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
iterator pairs*(self: WorkQueue): tuple[msgId: MsgId, val: Work] =
  for entry in self.list.items:
    let msgId = entry.msgId
    yield (msgId: msgId, val: entry)

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc msgIds*(self: WorkQueue): seq[MsgId] =
  result = newSeqOfCap[MsgId](self.len)
  for entry in self.list.items:
    result.add(entry.msgId)
