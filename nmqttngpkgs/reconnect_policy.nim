type
  ReconnectDecision* = object
    delaySec*: int
    shouldLog*: bool
    suppressedFailures*: int

  ReconnectPolicy* = object
    nextDelaySec: int
    maxDelayFailures: int
    suppressedFailures: int
    lastError: string

const
  ReconnectInitialDelaySec* = 1
  ReconnectMaxDelaySec* = 30
  ReconnectMaxDelayLogInterval* = 20

# ------------------------------------------------------------------------------
# Constructor:
# ------------------------------------------------------------------------------
proc newReconnectPolicy*(): ReconnectPolicy =
  result.nextDelaySec = ReconnectInitialDelaySec

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc reset*(self: var ReconnectPolicy) =
  ## Reset retry delay and log suppression after a successful connection.
  self = newReconnectPolicy()

# ------------------------------------------------------------------------------
# API:
# ------------------------------------------------------------------------------
proc registerFailure*(
    self: var ReconnectPolicy,
    errorMessage: string,
): ReconnectDecision =
  ## Record a failed reconnect attempt and return the retry decision.
  ##
  ## Retry delay grows as 1, 2, 4, 8, 16, 30 seconds and then stays at
  ## 30 seconds. Once the maximum delay is reached, repeated identical errors
  ## are logged only once every 20 attempts (about ten minutes). A changed
  ## error is always logged immediately.
  if self.nextDelaySec <= 0:
    self.nextDelaySec = ReconnectInitialDelaySec

  result.delaySec = self.nextDelaySec

  if self.nextDelaySec < ReconnectMaxDelaySec:
    result.shouldLog = true
    self.lastError = errorMessage
    self.maxDelayFailures = 0
    self.suppressedFailures = 0
    self.nextDelaySec = min(
      self.nextDelaySec * 2,
      ReconnectMaxDelaySec,
    )
    return

  if errorMessage != self.lastError:
    result.shouldLog = true
    result.suppressedFailures = self.suppressedFailures
    self.lastError = errorMessage
    self.maxDelayFailures = 1
    self.suppressedFailures = 0
    return

  self.maxDelayFailures.inc()

  if (self.maxDelayFailures - 1) mod ReconnectMaxDelayLogInterval == 0:
    result.shouldLog = true
    result.suppressedFailures = self.suppressedFailures
    self.suppressedFailures = 0
  else:
    self.suppressedFailures.inc()
