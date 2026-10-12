# C++ API

```
#include <qtnats.h>
```
All definitions are contained in the `QtNats` namespace.
Subjects, queue groups, stream, and consumer names should be provided as `QString`s; these are converted to UTF-8
before they are passed to nats.c, in accordance with the [NATS Client Protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol).

## Client Class
Represents a connection to a NATS server/cluster. All timeout values are in milliseconds.

Inherits: `QObject`

### Public Functions
```cpp
explicit Client(QObject* parent = nullptr);
void connectToServer(const Options& opts);
void connectToServer(const QUrl& address);
void close() noexcept;
void publish(const Message& msg);
Message request(const Message& msg, int64_t timeout = 2000);
QFuture<Message> asyncRequest(const Message& msg, int64_t timeout = 2000);
Subscription* subscribe(const QString& subject);
Subscription* subscribe(const QString& subject, const QString& queueGroup);
bool ping(int64_t timeout = 10000) noexcept;
QUrl currentServer() const;
ConnectionStatus status() const;
QString errorString() const;
static QByteArray newInbox();
JetStream* jetStream(const JsOptions& options = JsOptions());
natsConnection* getNatsConnection() const;
```

### Signals
```cpp
void errorOccurred(natsStatus error, const QString& text);
void statusChanged(ConnectionStatus status);
```

## Subscription Class
Represents a NATS subscription. Do not create the object yourself - use the factory functions `Client::subscribe`,
`JetStream::subscribe` or `JetStream::subscribeAsync`. Deleting it unsubscribes; it is safe while messages are still
being delivered.

Inherits: `QObject`

### Signals
```cpp
void received(const Message& message);
void errorOccurred(natsStatus error, const QString& text);
void ready();
void subscribeFailed(natsStatus error, jsErrCode jsErr, const QString& text);
```
`received` is emitted on a cnats delivery thread, so use the default (auto) connection type to handle it on the
receiver's thread. A slot connected with `Qt::DirectConnection` delays the Subscription's destruction until it returns;
`Qt::BlockingQueuedConnection` can deadlock.

`errorOccurred` carries asynchronous errors cnats reports for this subscription, such as a slow consumer or a failed
attempt to recreate an ordered consumer (cnats keeps retrying). `Client::errorOccurred` is emitted for them too.

`ready` and `subscribeFailed` are emitted only by subscriptions created with `JetStream::subscribeAsync`.
## Options Struct
A simple autocompletion-friendly wrapper over [cnats](http://nats-io.github.io/nats.c/group__opts_group.html) connection options.
## Message Struct
Represents a NATS message.
### Public Functions
```cpp
Message() {}
Message(const QString& in_subject, const QByteArray& in_data);
explicit Message(natsMsg* cmsg) noexcept;
bool isIncoming() const;
void ack();
void nack(int64_t delay = -1);
void inProgress();
void terminate();
```
### Public Members
```cpp
QString subject;
QString reply;
QByteArray data;
MessageHeaders headers; //QMultiHash<QString, QByteArray>
```

## JetStream Class
Represents a JetStream context. Created by `Client`.
### Public Functions
```cpp
JsPublishAck publish(const Message& msg, const JsPublishOptions& opts);
JsPublishAck publish(const Message& msg, int64_t timeout = -1);
void asyncPublish(const Message& msg, const JsPublishOptions& opts);
void asyncPublish(const Message& msg, int64_t timeout = -1);
void waitForPublishCompleted(int64_t timeout = -1);
Subscription* subscribe(const QString& subject, const QString& stream, const QString& push_consumer);
Subscription* subscribe(const QString& subject, const JsSubOptions& subOpts, const std::optional<JsOptions>& opts = std::nullopt);
Subscription* subscribeAsync(const QString& subject, const JsSubOptions& subOpts, const std::optional<JsOptions>& opts = std::nullopt);
PullSubscription* pullSubscribe(const QString& subject, const QString& stream, const QString& pull_consumer);
```
The `JsSubOptions` overloads give full control over the push subscription, e.g. an ordered, ephemeral consumer with
`subOpts.ordered = true` and a deliver policy in `subOpts.config`. Leave `stream` unset to look it up from the subject.

`subscribe` blocks for the server round trips and throws `JetStreamException` on failure. Messages delivered before
you connect to `received` are lost, including the retained messages a new consumer replays at once.

`subscribeAsync` returns immediately and sets the subscription up on a thread pool once control returns to the event
loop, so slots connected right after the call see every message. It reports the outcome with `Subscription::ready`
or `Subscription::subscribeFailed` instead of throwing; messages may arrive before `ready`. The Subscription may be
deleted at any time.
### Signals
```cpp
void errorOccurred(natsStatus error, jsErrCode jsErr, const QString& text, const Message& msg);
```
## PullSubscription class
```cpp
QList<Message> fetch(int batch = 1, int64_t timeout = 5000);
```
## JsPublishOptions Struct
Options to publish a message to JetStream.
### Public Members
```cpp
int64_t timeout
QString msgID
QString expectStream
QString expectLastMessageID
uint64_t expectLastSequence
uint64_t expectLastSubjectSequence
bool expectNoMessage
```
## JsPublishAck Struct
JetStream acknowledgment.
### Public Members
```cpp
QString stream
uint64_t sequence
QString domain
bool duplicate
```

# Error reporting
All synchronous errors are reported with exceptions.
Asynchronous errors are reported with signals.
## Exception Class
Inherits: `QException`

Thrown by core NATS functions.
```cpp
const natsStatus errorCode;
```
The error code reported by [cnats](http://nats-io.github.io/nats.c/status_8h.html).

```cpp
const char* what() const noexcept override
```
Returns human-readable description of the error.
## JetStreamException Class
Inherits: `Exception`

Thrown in case of JetStream-specific failures. Extends the Exception class with `const jsErrCode jsError` field.


# QML API
## NatsClient QML Type
### Properties
```qml
serverUrl: string
status: string (read-only)
```
### Methods
```qml
connectToServer()
disconnectFromServer()
subscription subscribe(string subject)
publish(string subject, string message)
string request(string subject, string message)
```
The `subscription` object has only `received(string payload)` signal.
