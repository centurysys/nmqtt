# nmqtt_ng - Native Nim MQTT client library and binaries

`nmqtt_ng` is a Century Systems fork of `nmqtt`. The package keeps the
original MQTT broker, publisher and subscriber sources while extending the
client library for long-running embedded and gateway applications.

* [Version 1.1.1](#version-111)
* [Install](#Install)
* [Binaries](#Binaries)
  * [nmqtt](#nmqtt)
  * [nmqtt_password](#nmqtt_password)
  * [nmqtt_pub](#nmqtt_pub)
  * [nmqtt_sub](#nmqtt_sub)
* [Library](#Library)
  * [Examples](#Examples)
  * [Procs](#Procs)


# Version 1.1.1

Version 1.1.1 focuses on MQTT client reliability for long-running
applications. The main changes are:

- verified TLS when a CA file is configured, including DNS hostname and IP
  address verification; TLS without a CA file keeps the legacy unverified
  behavior for compatibility
- automatic reconnect after transport loss, rejected CONNACK, and missing
  CONNACK, with capped exponential backoff and repeated-error log suppression
- PINGRESP timeout detection so half-open connections are discarded and
  re-established
- restoration of subscriptions after reconnect, independent of pending
  publish work
- immediate retransmission of unacknowledged QoS 1 publishes after reconnect,
  preserving the packet identifier and setting the DUP flag
- packet identifier allocation restricted to 1..65535 while avoiding IDs that
  are still in use
- corrected incoming QoS 1 acknowledgement handling so broker-originated
  packet identifiers do not conflict with client-originated publish IDs
- validation of SUBACK results and consistent publish-state updates as queued
  work completes
- consistent connection and publish-state notification after explicit
  disconnect

The reconnect and queue changes are primarily intended for the client-side
`publish` / `subscribe` use case.


# Install

You can install the fork directly with Nimble:
```text
$ nimble install https://github.com/centurysys/nmqtt
```

or clone and install it locally:
```text
$ git clone https://github.com/centurysys/nmqtt.git
$ cd nmqtt
$ nimble install
```

# Binaries

The package provides 4 MQTT binaries:
1) `nmqtt` -> Broker
2) `nmqtt_password` -> Password utility for the broker
3) `nmqtt_pub` -> MQTT publisher
4) `nmqtt_sub` -> MQTT subscriber


## nmqtt

A default configuration file is provided in `config/nmqtt.conf`. You can copy and paste this file to a desired location, or run `nimble setup nmqtt` which will guide you through it.

```
$ nmqtt --help
nmqtt version 1.1.1

nmqtt is a MQTT v3.1.1 broker

USAGE
  nmqtt [options]
  nmqtt [-c /path/to/config.conf]
  nmqtt [-h hostIP -p port]

CONFIG
  Use the configuration file for detailed settings,
  such as SSL, adjusting keep alive timer, etc. or
  specify options at the command line.

  To add and delete users from the password file
  please use nmqtt_password:
    - nmqtt_password -a|-b|-d [options]

OPTIONS
  -?, --help          print this cligen-erated help
  -c=, --config=      absolute path to the config file. Overrides all other options.
  -h=, --host=        IP-address to serve the broker on.
  -p=, --port=        network port to accept connecting from.
  -v=, --verbosity=   verbosity from 0-3.
  --max-conn=         max simultaneous connections. Defaults to no limit.
  --clientid-maxlen=  max lenght of clientid. Defaults to 65535.
  --clientid-spaces   allow spaces in clientid. Defaults to false.
  --clientid-empty    allow empty clientid and assign random id. Defaults to false.
  --client-kickold    kick old client, if new client has same clientid. Defaults to false.
  --clientid-pass     pass clientid in payload {clientid:payload}. Defaults to false.
  --password-file=    absolute path to the password file
  --ssl               activate ssl for the broker - requires --ssl-cert and --ssl-key.
  --ssl-cert=         absolute path to the ssl certificate.
  --ssl-key=          absolute path to the ssl key.
```


## nmqtt_password
```
$ nmqtt_password --help
nmqtt_password is a user and password manager for nmqtt
nmqtt_password is based upon nmqtt version 1.1.1

USAGE
  nmqtt_password -a {password_file.conf} {username}
  nmqtt_password -b {password_file.conf} {username} {password}
  nmqtt_password -d {password_file.conf} {username}

CONFIG
  Add or delete users from nmqtt password file.

OPTIONS
  -?, --help     print this cligen-erated help
  -a, --adduser  add a new user to the password file.
  -b, --batch    run in batch mode to allow passing passwords on the command line.
  -d, --deluser  delete a user from the password file.
```


## nmqtt_pub
```
$ ./nmqtt_pub --help
nmqtt_pub is a MQTT client for publishing messages to a MQTT-broker.
nmqtt_pub is based upon nmqtt version 1.1.1

Usage:
  nmqtt_pub [options] -t {topic} -m {message}
  nmqtt_pub [-h host -p port -u username -P password] -t {topic} -m {message}

OPTIONS
  -?, --help         print this cligen-erated help
  -h=, --host=       IP-address of the broker.
  -p=, --port=       network port to connect too.
  --ssl              use ssl.
  -c=, --clientid=   your connection ID. Defaults to nmqttpub- appended with processID.
  -u=, --username=   provide a username
  -P=, --password=   provide a password
  -t=, --topic=      mqtt topic to publish to.
  -m=, --msg=        message payload to send.
  -q=, --qos=        quality of service level to use for all messages.
  -r, --retain       retain messages on the broker.
  --repeat=          repeat the publish N times.
  --repeatdelay=     if using --repeat, wait N seconds between publish. Defaults to 0.
  --willtopic=       set the will's topic
  --willmsg=         set the will's message
  --willqos=         set the will's quality of service
  --willretain       set to retain the will message
  -v=, --verbosity=  set the verbosity level from 0-2. Defaults to 0.
```


## nmqtt_sub
```
$ ./nmqtt_sub --help
nmqtt_sub is a MQTT client that will subscribe to a topic on a MQTT-broker.
nmqtt_sub is based upon nmqtt version 1.1.1

Usage:
  nmqtt_sub [options] -t {topic}
  nmqtt_sub [-h host -p port -u username -P password] -t {topic}

OPTIONS
  -?, --help         print this cligen-erated help
  -h=, --host=       IP-address of the broker. Defaults to 127.0.0.1
  -p=, --port=       network port to connect too. Defaults to 1883.
  --ssl              use ssl.
  -c=, --clientid=   your connection ID. Defaults to nmqttsub- appended with processID.
  -u=, --username=   provide a username
  -P=, --password=   provide a password
  -t=, --topic=      MQTT topic to subscribe too. For multipe topics, separate them by comma.
  -q=, --qos=        quality of service level to use for all messages. Defaults to 0.
  -k=, --keepalive=  keep alive in seconds for this client. Defaults to 60.
  --removeretained   clear any retained messages on the topic
  --willtopic=       set the will's topic
  --willmsg=         set the will's message
  --willqos=         set the will's quality of service
  --willretain       set to retain the will message
  -v=, --verbosity=  set the verbosity level from 0-2. Defaults to 0.
```


# Library

This library includes all the needed proc's for publishing MQTT messages to
a MQTT-broker and for subscribing to a topic on a MQTT-broker. The library supports MQTT QoS 0, 1 and 2 for publishing and subscribing, and supports retained messages.

## Examples

### Subscribe to topic
```nim
import nmqtt_ng, asyncdispatch

let ctx = newMqttCtx("nmqttClient")
ctx.set_host("test.mosquitto.org", 1883)
#ctx.set_auth("username", "password")
#ctx.set_ping_interval(30)
#ctx.set_ssl_certificates("cert.crt", "private.key")

proc mqttSub() {.async.} =
  await ctx.start()
  proc on_data(topic: string, message: string) =
    echo "got ", topic, ": ", message

  await ctx.subscribe("nmqtt", 2, on_data)

asyncCheck mqttSub()
runForever()
```

### TLS and Mutual TLS with CA verification

TLS is enabled by passing `true` as the third argument to `set_host`.
Client certificates are configured separately from the CA certificate
used to verify the remote broker.

```nim
import nmqtt_ng, asyncdispatch

let ctx = newMqttCtx("nmqttTlsClient")

ctx.set_host("broker.example.com", 8883, true)

# Client certificate and private key for Mutual TLS authentication.
ctx.set_ssl_certificates(
  "/etc/nmqtt/client.crt",
  "/etc/nmqtt/client.key",
)

# CA certificate used to verify the broker certificate.
ctx.set_ssl_ca_file("/etc/nmqtt/root-ca.crt")

proc mqttTls() {.async.} =
  await ctx.start()
  await ctx.publish("nmqtt", "hello", 1)

asyncCheck mqttTls()
runForever()
```

When a CA file is configured, nmqtt verifies both the broker certificate
chain and the expected DNS hostname or IP address.

For backwards compatibility, TLS without `set_ssl_ca_file` keeps the
previous unverified TLS behavior. Applications that require authenticated
TLS should always configure a CA file.

### Publish msg
```nim
proc mqttPub() {.async.} =
  await ctx.start()
  await ctx.publish("nmqtt", "hallo", 2)
  await sleepAsync 500
  await ctx.disconnect()

waitFor mqttPub()
```

### Subscribe and publish
```nim
proc mqttSubPub() {.async.} =
  await ctx.start()

  # Callback when receiving on the topic
  proc on_data(topic: string, message: string) =
    echo "got ", topic, ": ", message

  # Subscribe to topic the topic `nmqtt`
  await ctx.subscribe("nmqtt", 2, on_data)
  await sleepAsync 500

  # Publish a message to the topic `nmqtt`
  await ctx.publish("nmqtt", "hallo", 2)
  await sleepAsync 500

  # Disconnect
  await ctx.disconnect()

waitFor mqttSubPub()
```



## Procs

### newMqttCtx*

```nim
proc newMqttCtx*(clientId: string): MqttCtx =
```

Initiate a new MQTT client


____

### set_ping_interval*

```nim
proc set_ping_interval*(ctx: MqttCtx, txInterval: int) =
```

Set the client's keepalive ping interval in seconds. Default is 60 seconds.
If a PINGRESP is not received before the next keepalive interval, the client
closes the stalled transport and lets the automatic reconnect worker recover
the connection.

____

### set_conn_ack_timeout*

```nim
proc set_conn_ack_timeout*(ctx: MqttCtx, timeoutMs: int) =
```

Set the maximum time to wait for CONNACK after sending CONNECT. The default is
10 seconds. If the timeout expires, the connection is closed and retried using
the normal reconnect backoff policy.

____

### set_ssl_certificates*

```nim
proc set_ssl_certificates*(ctx: MqttCtx, sslCert: string, sslKey: string) =
```

Sets the client certificate and private key files used for Mutual TLS
authentication.

This configures the credentials presented by the MQTT client to the broker.
Use `set_ssl_ca_file` separately to verify the broker certificate.


____

### set_ssl_ca_file*

```nim
proc set_ssl_ca_file*(ctx: MqttCtx, sslCaFile: string) =
```

Sets the CA certificate file used to verify the remote broker.

When a CA file is configured, TLS peer verification is enabled and the
broker certificate is checked against the configured DNS hostname or IP
address.

If no CA file is configured, TLS keeps the previous unverified behavior
for backwards compatibility.


____

### set_host*

```nim
proc set_host*(ctx: MqttCtx, host: string, port: int=1883, sslOn=false) =
```

Set the MQTT host


____

### set_auth*

```nim
proc set_auth*(ctx: MqttCtx, username: string, password: string) =
```

Set the authentication for the host


____

### set_will*

```nim
proc set_will*(ctx: MqttCtx, topic, msg: string, qos=0, retain=false) =
```

Set the clients will.


____

### connect*

```nim
proc connect*(ctx: MqttCtx) {.async.} =
```

Connect to the broker.


____

### start*

```nim
proc start*(ctx: MqttCtx) {.async.} =
```

Auto-connect and reconnect to the broker. The client will try to
reconnect when the state is `Disconnected` or `Error`. The `Error`-state
happens, when the broker is down, but the client will try to reconnect
until the broker is up again.


____

### disconnect*

```nim
proc disconnect*(ctx: MqttCtx) {.async.} =
```

Disconnect from the broker.


____

### publish_id*

```nim
proc publish_id*(ctx: MqttCtx, topic: string, message: string,
                 qos=0, retain=false): Future[Option[MsgId]]
```

Queues a publish operation and returns its MQTT message ID when the work
was accepted.

The returned message ID can be matched with the callback registered by
`register_callback`. For QoS 1, a `PubAck` callback is invoked with the
same message ID after the broker acknowledges the publish.


____

### publish*

```nim
proc publish*(ctx: MqttCtx, topic: string, message: string, qos=0, retain=false) {.async.} =
```

Publish a message.

**Required:**
  - topic: string
  - message: string

**Optional:**
  - qos: int     = 0, 1 or 2
  - retain: bool = true or false

**Publish message:**
```nim
ctx.publish(topic = "nmqtt", message = "Hey there", qos = 0, retain = true)
```

**Remove retained message on topic:**

Set the `message` to _null_.
```nim
ctx.publish(topic = "nmqtt", message = "", qos = 0, retain = true)
```


____

### subscribe*

```nim
proc subscribe*(ctx: MqttCtx, topic: string, qos: int, callback: PubCallback): Future[void] =
```

Subscribe to a topic

Access the callback with:
```nim
proc callbackName(topic: string, message: string) =
  echo "Topic: ", topic, ": ", message
```

____


### unsubscribe*

```nim
proc unsubscribe*(ctx: MqttCtx, topic: string): Future[void] =
```

Unsubscribe from a topic.


____

### isConnected*

```nim
proc isConnected*(ctx: MqttCtx): bool =
```

Returns true, if the client is connected to the broker.


____

### register_callback*

```nim
proc register_callback*(
  ctx: MqttCtx,
  callback: proc(msgId: uint16, pktType: PktType)
): bool
```

Registers a callback for completion of queued MQTT work.

For a QoS 1 publish, the callback is invoked with `pktType == PubAck`.
The `msgId` matches the ID returned by `publish_id`.

Only one work callback can be registered. The proc returns `false` if a
callback has already been registered.

Use `unregister_callback` before registering a replacement callback.


____

### unregister_callback*

```nim
proc unregister_callback*(ctx: MqttCtx) =
```

Removes the registered work callback.


____

### msgQueue*

```nim
proc msgQueue*(ctx: MqttCtx): int =
```

Returns the number of unfinished packages, which still are in the work queue.
This includes all publish and subscribe packages, which has not been fully
send, acknowledged or completed.

You can use this to ensure, that all your of messages are sent, before
exiting your program.


____
