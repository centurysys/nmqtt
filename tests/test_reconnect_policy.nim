import std/unittest

import ../nmqttngpkgs/reconnect_policy

suite "Reconnect policy":
  test "uses capped exponential backoff":
    var policy = newReconnectPolicy()
    var delays: seq[int]

    for _ in 0 ..< 8:
      let decision = policy.registerFailure("connection refused")
      delays.add(decision.delaySec)

    check delays == @[1, 2, 4, 8, 16, 30, 30, 30]

  test "suppresses repeated errors at the maximum delay":
    var policy = newReconnectPolicy()

    for _ in 0 ..< 5:
      let decision = policy.registerFailure("connection refused")
      check decision.shouldLog

    let firstMaxDelay = policy.registerFailure("connection refused")
    check firstMaxDelay.delaySec == 30
    check firstMaxDelay.shouldLog
    check firstMaxDelay.suppressedFailures == 0

    for _ in 0 ..< 19:
      let decision = policy.registerFailure("connection refused")
      check decision.delaySec == 30
      check not decision.shouldLog

    let periodicLog = policy.registerFailure("connection refused")
    check periodicLog.delaySec == 30
    check periodicLog.shouldLog
    check periodicLog.suppressedFailures == 19

  test "logs a changed error immediately":
    var policy = newReconnectPolicy()

    for _ in 0 ..< 6:
      discard policy.registerFailure("connection refused")

    for _ in 0 ..< 4:
      let decision = policy.registerFailure("connection refused")
      check not decision.shouldLog

    let changed = policy.registerFailure("certificate verify failed")
    check changed.delaySec == 30
    check changed.shouldLog
    check changed.suppressedFailures == 4

  test "reset restarts backoff after a successful connection":
    var policy = newReconnectPolicy()

    for _ in 0 ..< 6:
      discard policy.registerFailure("connection refused")

    policy.reset()

    let decision = policy.registerFailure("connection refused")
    check decision.delaySec == 1
    check decision.shouldLog
    check decision.suppressedFailures == 0
