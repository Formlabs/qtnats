/*
 * Copyright(c) 2021-2022 Petro Kazmirchuk https://github.com/Kazmirchuk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <QList>
#include <QMap>

#include <QByteArray>
#include <QFuture>
#include <QMultiHash>
#include <QObject>
#include <QSemaphore>
#include <QUrl>

#include <nats/nats.h>

#include <qtnats/qtnats_export.h>

namespace QtNats {
QTNATS_EXPORT Q_NAMESPACE // we need the "export" directive due to https://bugreports.qt.io/browse/QTBUG-68014

    using MessageHeaders = QMultiHash<QString, QByteArray>;

// need to throw it from QFuture; otherwise it would be derived from std::runtime_error
// in fact, QException inherits from std::exception, although it's not documented
class Exception : public QException {
public:
    Exception(natsStatus s) : errorCode(s) {}

    void raise() const override { throw *this; }
    Exception* clone() const override { return new Exception(*this); }
    const char* what() const noexcept override { return natsStatus_GetText(errorCode); }

    const natsStatus errorCode;
};

class JetStreamException : public Exception {
public:
    JetStreamException(natsStatus s, jsErrCode js) : Exception(s), jsError(js), errorText(initText(js)) {}

    void raise() const override { throw *this; }
    JetStreamException* clone() const override { return new JetStreamException(*this); }
    const char* what() const noexcept override { return errorText.constData(); }

    const jsErrCode jsError;

private:
    QByteArray initText(jsErrCode js) { return QString("%1: %2").arg(Exception::what()).arg(js).toLatin1(); }

    const QByteArray errorText;
};

// =============================================================================
// Data types  (analogous to nats.h types)
// =============================================================================
#pragma region Data types

enum class ConnectionStatus {
    Disconnected = NATS_CONN_STATUS_DISCONNECTED,
    Connecting = NATS_CONN_STATUS_CONNECTING,
    Connected = NATS_CONN_STATUS_CONNECTED,
    Closed = NATS_CONN_STATUS_CLOSED,
    Reconnecting = NATS_CONN_STATUS_RECONNECTING,
    DrainingSubs = NATS_CONN_STATUS_DRAINING_SUBS,
    DrainingPubs = NATS_CONN_STATUS_DRAINING_PUBS
};
Q_ENUM_NS(ConnectionStatus)

enum class JsAckPolicy {
    Explicit = js_AckExplicit,
    None = js_AckNone,
    All = js_AckAll,
};
Q_ENUM_NS(JsAckPolicy)

enum class JsDeliverPolicy {
    All = js_DeliverAll,
    Last = js_DeliverLast,
    New = js_DeliverNew,
    ByStartSequence = js_DeliverByStartSequence,
    ByStartTime = js_DeliverByStartTime,
    LastPerSubject = js_DeliverLastPerSubject,
};
Q_ENUM_NS(JsDeliverPolicy)

enum class JsReplayPolicy {
    Instant = js_ReplayInstant,
    Original = js_ReplayOriginal,
};
Q_ENUM_NS(JsReplayPolicy)

struct JsConsumerConfig {
    QString name;
    QString durable;
    QString description;

    // nullopt = leave at existing consumer's value (for jsSubOptions bind); 0 = DeliverAll / AckExplicit /
    // ReplayInstant
    std::optional<JsDeliverPolicy> deliverPolicy;
    std::optional<JsAckPolicy> ackPolicy;
    std::optional<JsReplayPolicy> replayPolicy;

    uint64_t optStartSeq = 0; ///< Sequence to start from when DeliverPolicy = ByStartSequence
    int64_t optStartTime = 0; ///< UTC nanoseconds since epoch; used when DeliverPolicy = ByStartTime

    int64_t ackWait = 0;    ///< How long to wait for ack before redelivery, nanoseconds
    int64_t maxDeliver = 0; ///< Maximum number of delivery attempts
    QList<int64_t> backOff; ///< Redelivery intervals, nanoseconds

    QString filterSubject;   ///< Subject filter for this consumer
    uint64_t rateLimit = 0;      ///< Rate limit in bits per second
    QString sampleFrequency; ///< Percentage of messages to sample for observability

    int64_t maxWaiting = 0;    ///< Maximum number of outstanding pull requests
    int64_t maxAckPending = 0; ///< Maximum number of unacknowledged messages

    bool flowControl = false;
    int64_t heartbeat = 0; ///< Heartbeat interval, nanoseconds
    bool headersOnly = false;

    // Pull-based options
    int64_t maxRequestBatch = 0;    ///< Maximum pull request batch size
    int64_t maxRequestExpires = 0;  ///< Maximum pull request expiration, nanoseconds
    int64_t maxRequestMaxBytes = 0; ///< Maximum pull request byte limit

    // Push-based options
    QString deliverSubject;
    QString deliverGroup;

    int64_t inactiveThreshold = 0; ///< Ephemeral consumer inactivity threshold, nanoseconds
    int64_t replicas = 0;
    bool memoryStorage = false;

    // Added in NATS 2.10
    QList<QString> filterSubjects; ///< Multiple subject filters
    QMap<QString, QByteArray> metadata;

    // Added in NATS 2.11
    int64_t pauseUntil = 0; ///< Suspend consumer until this UTC nanosecond timestamp
};

struct JsOptionsPublishAsync {
    int64_t maxPending = 0;  ///< Maximum outstanding async publishes inflight at one time (0 = no limit)
    int64_t stallWait = 200; ///< Milliseconds to wait in PublishAsync when MaxPending is reached
    // Note: AckHandler/ErrHandler are not exposed here — connect to JetStream::errorOccurred instead
};

struct JsOptionsPullSubscribeAsync {
    int64_t timeout = 0;   ///< Auto-unsubscribe after this many milliseconds
    int maxMessages = 0;   ///< Auto-unsubscribe after this many messages
    int64_t maxBytes = 0;  ///< Auto-unsubscribe after this many bytes
    bool noWait = false;   ///< Receive only messages already on server; don't wait for more
    int64_t heartbeat = 0; ///< Server heartbeat interval (ms) to detect communication failures
    int fetchSize = 0;     ///< Messages per automatic fetch request (default flow control)
    int keepAhead = 0;     ///< Pre-fetch this many messages before current request is fulfilled
    // Note: CompleteHandler is not exposed here — use a signal on PullSubscription instead
    // Note: NextHandler is not exposed here — it overrides cnats's internal fetch flow control
};

struct JsOptionsStreamInfo {
    bool deletedDetails = false; ///< Include list of deleted message sequences
    QString subjectsFilter;  ///< Filter subjects returned in stream state
};

struct JsOptionsStreamPurge {
    QString subject;   ///< Subject to match against messages for the purge command
    uint64_t sequence = 0; ///< Purge up to but not including this sequence
    uint64_t keep = 0;     ///< Number of messages to keep
};

struct JsOptionsStream {
    JsOptionsStreamPurge purge;
    JsOptionsStreamInfo info;
};

struct JsOptions {
    QString prefix = "$JS.API";                 ///< JetStream API prefix
    QString domain;                             ///< Changes the domain part of the JetStream API prefix
    int64_t timeout = 5000;                         ///< Milliseconds to wait for JetStream API requests
    JsOptionsPublishAsync publishAsync;             ///< extra options for #js_PublishAsync
    JsOptionsPullSubscribeAsync pullSubscribeAsync; ///< extra options for #js_PullSubscribeAsync
    JsOptionsStream stream;                         ///< Optional stream options
};

struct JsPublishAck {
    QString stream;
    uint64_t sequence = 0;
    QString domain;
    bool duplicate = false;
};

struct JsPublishOptions {
    int64_t timeout = -1;            ///< Milliseconds to wait for publish response; default uses context's Wait value
    QString msgID;               ///< Message ID used for de-duplication
    QString expectStream;        ///< Expected stream to respond from the publish call
    QString expectLastMessageID; ///< Expected last message ID in the stream
    uint64_t expectLastSequence = 0; ///< Expected last message sequence in the stream
    uint64_t expectLastSubjectSequence = 0; ///< Expected last message sequence for the subject in the stream
    bool expectNoMessage = false;           ///< Expected no message (sequence == 0) for the subject in the stream
};

struct JsSubOptions {
    QString stream;   ///< Bind subscription to this stream name
    QString consumer; ///< Bind to this existing consumer (must be pre-created)
    QString queue;    ///< Queue group name for queue subscriptions
    bool ordered = false; ///< If true, creates an ordered ephemeral consumer
    JsConsumerConfig config;
    // Note: ManualAck is not exposed here — managed internally by the subscription type
};

struct QTNATS_EXPORT Message {
    Message() = default;

    Message(QString in_subject, const QByteArray& in_data) : subject(std::move(in_subject)), data(in_data) {}

    explicit Message(natsMsg* cmsg) noexcept;

    bool isIncoming() const { return bool(m_natsMsg); }

    // JetStream acknowledgments
    void ack();
    void nack(int64_t delay = -1); // ms
    void inProgress();
    void terminate();

    QString subject;
    QByteArray reply;
    QByteArray data;
    // NB! 1. headers are case-sensitive
    // 2. cnats does NOT preserve the order of headers
    MessageHeaders headers;

private:
    std::shared_ptr<natsMsg> m_natsMsg;
};

struct QTNATS_EXPORT Options {
    QList<QUrl> servers;
    QString user;
    QString password;
    QString token;
    QString name;
    bool secure = false;
    bool verbose = false;
    bool pedantic = false;
    bool allowReconnect = true;
    bool randomize = true; // NB! reverted option
    bool echo = true;      // NB! reverted option

    // Defaults mirror the NATS_OPTS_DEFAULT_* constants from cnats's private opts.h,
    // hardcoded here to avoid depending on that internal header.
    int64_t timeout = 2000;                    // NATS_OPTS_DEFAULT_TIMEOUT
    int64_t pingInterval = 120000;             // NATS_OPTS_DEFAULT_PING_INTERVAL
    int maxPingsOut = 2;                       // NATS_OPTS_DEFAULT_MAX_PING_OUT
    int ioBufferSize = 32768;                  // NATS_OPTS_DEFAULT_IO_BUF_SIZE
    int maxReconnect = 60;                     // NATS_OPTS_DEFAULT_MAX_RECONNECT
    int64_t reconnectWait = 2000;              // NATS_OPTS_DEFAULT_RECONNECT_WAIT
    int reconnectBufferSize = 8 * 1024 * 1024; // NATS_OPTS_DEFAULT_RECONNECT_BUF_SIZE
    int maxPendingMessages = 65536;            // NATS_OPTS_DEFAULT_MAX_PENDING_MSGS

    // mTLS options
    QString certFile; // Client certificate file path
    QString keyFile;  // Client private key file path
    QString caFile;   // CA certificate for server verification
};

#pragma endregion

// =============================================================================
// Classes
// =============================================================================
#pragma region Classes

class Subscription;
class JetStream;
class PullSubscription;

class QTNATS_EXPORT Client : public QObject {
    Q_OBJECT
    Q_DISABLE_COPY(Client)

public:
    explicit Client(QObject* parent = nullptr);

    ~Client() noexcept override;

    Client(Client&&) = delete;

    Client& operator=(Client&&) = delete;

    void connectToServer(const Options& opts);

    void connectToServer(const QUrl& address);

    void close() noexcept;

    void publish(const Message& msg);

    Message request(const Message& msg, int64_t timeout = 2000);

    QFuture<Message> asyncRequest(const Message& msg, int64_t timeout = 2000);

    Subscription* subscribe(const QString& subject);

    Subscription* subscribe(const QString& subject, const QString& queueGroup);

    bool ping(int64_t timeout = 10000) noexcept; // ms

    QUrl currentServer() const;

    ConnectionStatus status() const;

    QString errorString() const;

    static QByteArray newInbox();

    JetStream* jetStream(const JsOptions& options = JsOptions());

    natsConnection* getNatsConnection() const { return m_conn; }

Q_SIGNALS:
    void errorOccurred(natsStatus error, const QString& text);

    void statusChanged(ConnectionStatus status);

private:
    natsConnection* m_conn = nullptr;
    QSemaphore semaphore;

    // Prevent QFutureInterface leak when close() destroys subscriptions
    // before asyncRequest callbacks fire.
    std::vector<std::shared_ptr<QFutureInterface<Message> > > m_pendingAsyncRequests;

    static void closedConnectionHandler(natsConnection* nc, void* closure);
};

class QTNATS_EXPORT Subscription : public QObject {
    Q_OBJECT
    Q_DISABLE_COPY(Subscription)

public:
    ~Subscription() noexcept override;

    Subscription(Subscription&&) = delete;

    Subscription& operator=(Subscription&&) = delete;

Q_SIGNALS:
    void received(Message message);

private:
    Subscription(QObject* parent) : QObject(parent) {}

    natsSubscription* m_sub = nullptr;
    friend class Client;
    friend class JetStream;
};

class QTNATS_EXPORT PullSubscription : public QObject {
    Q_OBJECT
    Q_DISABLE_COPY(PullSubscription)

public:
    ~PullSubscription() noexcept override;

    PullSubscription(PullSubscription&&) = delete;

    PullSubscription& operator=(PullSubscription&&) = delete;

    QList<Message> fetch(int batch = 1, int64_t timeout = 5000);

private:
    PullSubscription(QObject* parent) : QObject(parent) {}

    natsSubscription* m_sub = nullptr;
    friend class JetStream;
};

class QTNATS_EXPORT JetStream : public QObject {
    Q_OBJECT
    Q_DISABLE_COPY(JetStream)

public:
    ~JetStream() noexcept override;

    JetStream(JetStream&&) = delete;

    JetStream& operator=(JetStream&&) = delete;

    JsPublishAck publish(const Message& msg, const JsPublishOptions& opts);

    JsPublishAck publish(const Message& msg, int64_t timeout = -1);

    void asyncPublish(const Message& msg, const JsPublishOptions& opts);

    void asyncPublish(const Message& msg, int64_t timeout = -1);

    void waitForPublishCompleted(int64_t timeout = -1);

    Subscription* subscribe(const QString& subject, const QString& stream, const QString& consumer);

    PullSubscription* pullSubscribe(const QString& subject, const QString& stream, const QString& consumer);

    jsCtx* getJsContext() const { return m_jsCtx; }

Q_SIGNALS:
    void errorOccurred(natsStatus error, jsErrCode jsErr, const QString& text, Message msg);

private:
    JetStream(QObject* parent) : QObject(parent) {}

    jsCtx* m_jsCtx = nullptr;

    JsPublishAck doPublish(const Message& msg, jsPubOptions* opts);

    void doAsyncPublish(const Message& msg, jsPubOptions* opts);

    friend class Client;
};

#pragma endregion

} // namespace QtNats

Q_DECLARE_METATYPE(QtNats::Message)
