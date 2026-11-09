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

#include <algorithm>
#include <mutex>
#include <utility>

#include <QFutureInterface>
#include <QThread>
#include <QPointer>
#include <QThreadPool>

#include "qtnats/qtnats.h"
#include "qtnats/qtnats_p.h"

using namespace QtNats;

// need to pass it through queued signal-slot connections
static const int messageTypeId = qRegisterMetaType<Message>();

// =============================================================================
// Data types  (analogous to nats.h types)
// =============================================================================
#pragma region Data types

MessageHeaders Message::readHeaders(natsMsg* msg) {
    return readHeaderFields(
        [msg](const char*** keys, int* count) { return natsMsgHeader_Keys(msg, keys, count); },
        [msg](const char* key, const char*** vals, int* count) { return natsMsgHeader_Values(msg, key, vals, count); }
    );
}

// Empty payloads stay non-null.
static QByteArray readData(const natsMsg* msg) {
    const int length = natsMsg_GetDataLength(msg);
    return length > 0 ? QByteArray(natsMsg_GetData(msg), length) : QByteArray("", 0);
}

Message::Message(natsMsg* msg)
    : m_natsMsg{msg, &natsMsg_Destroy}
    , subject{QString::fromUtf8(natsMsg_GetSubject(msg))}
    , reply{QByteArray(natsMsg_GetReply(msg))}
    , data{readData(msg)}
    , headers{readHeaders(msg)} {}

#pragma endregion

// =============================================================================
// Classes
// =============================================================================
#pragma region Classes

namespace {

// Their threads mostly wait on the network, so they can outnumber the cores.
class ConnectionThreadPool : public QThreadPool {
public:
    ConnectionThreadPool() { setMaxThreadCount(64); }
};
class SubscribeThreadPool : public QThreadPool {
public:
    SubscribeThreadPool() { setMaxThreadCount(32); }
};
Q_GLOBAL_STATIC(ConnectionThreadPool, globalConnectionPool)
Q_GLOBAL_STATIC(SubscribeThreadPool, globalSubscribePool)

// Owns an asyncRequest's QFutureInterface for cnats. Freed in onComplete, after the subscription's last callback.
using FutureHolder = std::shared_ptr<QFutureInterface<Message>>;

void asyncRequestCallback(natsConnection* /*nc*/, natsSubscription* natsSub, natsMsg* msg, void* closure) {
    const FutureHolder& future_iface = *static_cast<FutureHolder*>(closure);

    if (msg) {
        if (natsMsg_IsNoResponders(msg)) {
            future_iface->reportException(Exception(NATS_NO_RESPONDERS));
            natsMsg_Destroy(msg);
        } else {
            const Message m(msg);
            future_iface->reportResult(m);
        }
    } else {
        future_iface->reportException(Exception(NATS_TIMEOUT));
    }
    future_iface->reportFinished();
    natsSubscription_Destroy(natsSub);
}

// Also runs when the connection closes before a reply arrives.
void asyncRequestComplete(void* closure) {
    auto* const holder = static_cast<FutureHolder*>(closure);
    if (!(*holder)->isFinished()) {
        (*holder)->reportException(Exception(NATS_CONNECTION_CLOSED));
        (*holder)->reportFinished();
    }
    delete holder;
}

ConnectionCore& coreFrom(void* closure) { return *static_cast<ConnectionCore*>(closure); }

// The caller holds core.mutex.
void setStatus(ConnectionCore& core, ConnectionStatus status) {
    core.status = status;
    if (core.client)
        Q_EMIT core.client->statusChanged(status);
}

void errorHandler(natsConnection* /*nc*/, natsSubscription* subscription, natsStatus err, void* closure) {
    ConnectionCore& core = coreFrom(closure);
    const QString text = lastErrorText(err);
    {
        const std::lock_guard lock(core.mutex);
        if (core.client)
            Q_EMIT core.client->errorOccurred(err, text);
    }

    if (!subscription)
        return;
    // cnats keeps `subscription` alive during this callback, so no other subscription can reuse its address.
    SubscriptionRelay* const relay = core.registry->retain(subscription);
    if (!relay)
        return;
    relay->withTarget([&](Subscription& sub) { Q_EMIT sub.errorOccurred(err, text); });
    relay->release();
}

void closedHandler(natsConnection* /*nc*/, void* closure) {
    ConnectionCore& core = coreFrom(closure);
    std::shared_ptr<ConnectionCore> cnatsRef;
    {
        const std::lock_guard lock(core.mutex);
        setStatus(core, ConnectionStatus::Closed);
        cnatsRef = std::move(core.cnatsRef);
    }
}

void reconnectedHandler(natsConnection* /*nc*/, void* closure) {
    ConnectionCore& core = coreFrom(closure);
    const std::lock_guard lock(core.mutex);
    setStatus(core, ConnectionStatus::Connected);
}

void disconnectedHandler(natsConnection* /*nc*/, void* closure) {
    ConnectionCore& core = coreFrom(closure);
    const std::lock_guard lock(core.mutex);
    setStatus(core, ConnectionStatus::Disconnected);
}

void closeNow(natsConnection* conn) {
    natsConnection_Close(conn);
    natsConnection_Destroy(conn);
}

void closeInBackground(natsConnection* conn) {
    connectionPool().start([conn] { closeNow(conn); });
}

// Stores a new connection in `core`. Returns false if its Client let go while it connected; the caller then
// closes `conn`.
bool adopt(const std::shared_ptr<ConnectionCore>& core, natsConnection* conn) {
    const std::lock_guard lock(core->mutex);
    core->cnatsRef = core;
    if (!core->client)
        return false;
    core->conn = conn;
    // A disconnect may already have been reported; the reconnected callback then reports Connected.
    if (core->status == ConnectionStatus::Connecting)
        setStatus(*core, ConnectionStatus::Connected);
    return true;
}

// cnats makes no callbacks for a failed connect, so this reports it.
void reportFailedConnect(ConnectionCore& core, natsStatus s) {
    const QString text = lastErrorText(s);
    const std::lock_guard lock(core.mutex);
    core.lastError = text;
    if (core.client)
        Q_EMIT core.client->errorOccurred(s, text);
    setStatus(core, ConnectionStatus::Closed);
}

void connectInBackground(const std::shared_ptr<ConnectionCore>& core, natsOptions* options) {
    natsConnection* conn = nullptr;
    const natsStatus s = natsConnection_Connect(&conn, options);
    if (s != NATS_OK) {
        reportFailedConnect(*core, s);
        return;
    }
    if (!adopt(core, conn))
        closeNow(conn);
}

// Detaches `core` from its Client. Returns its connection, which the caller closes, and the status it had.
std::pair<natsConnection*, ConnectionStatus> detach(std::shared_ptr<ConnectionCore>& core) noexcept {
    if (!core)
        return {nullptr, ConnectionStatus::Closed};
    std::pair<natsConnection*, ConnectionStatus> detached;
    {
        const std::lock_guard lock(core->mutex);
        core->client = nullptr;
        detached = {std::exchange(core->conn, nullptr), std::exchange(core->status, ConnectionStatus::Closed)};
    }
    core.reset();
    return detached;
}

} // namespace

QThreadPool& QtNats::connectionPool() { return *globalConnectionPool(); }
QThreadPool& QtNats::subscribePool() { return *globalSubscribePool(); }

Client::Client(QObject* parent)
    : QObject(parent)
    , m_registry(std::make_shared<SubscriptionRegistry>()) {
    // Open the library here, before any pool thread uses it: cnats fails a thread that arrives while another
    // thread is still opening it.
    static std::once_flag opened;
    std::call_once(opened, [] {
        nats_Open(-1);
        const int cpuCoresCount = QThread::idealThreadCount(); // this function may fail, thus the check
        if (cpuCoresCount >= 2) {
            nats_SetMessageDeliveryPoolSize(cpuCoresCount);
        }
    });
}

Client::~Client() noexcept { close(); }

std::pair<std::shared_ptr<ConnectionCore>, std::shared_ptr<natsOptions>> Client::startConnecting(
    const Options& opts
) {
    auto core = std::make_shared<ConnectionCore>();
    core->registry = m_registry;
    std::shared_ptr<natsOptions> natsOpts = convertAndHandle(opts, [&](NatsOptsPtr& c) {
        natsOptions* const nats_opts = c.get();

        // don't create a thread for each subscription, since we may have a lot of subscriptions
        // number of threads in the pool is set by nats_SetMessageDeliveryPoolSize in the constructor
        checkError(natsOptions_UseGlobalMessageDelivery(nats_opts, true));

        checkError(natsOptions_SetErrorHandler(nats_opts, &errorHandler, core.get()));
        checkError(natsOptions_SetClosedCB(nats_opts, &closedHandler, core.get()));
        checkError(natsOptions_SetDisconnectedCB(nats_opts, &disconnectedHandler, core.get()));
        checkError(natsOptions_SetReconnectedCB(nats_opts, &reconnectedHandler, core.get()));
        return std::shared_ptr<natsOptions>(c.release(), NatsOptsDeleter{});
    });

    // Closing emits statusChanged(Closed), and a slot may destroy this Client. The core then stays detached, and the
    // connect closes whatever it gets.
    const QPointer<Client> self(this);
    closeAsync();
    if (!self)
        return {core, natsOpts};

    core->client = this;
    m_core = core;
    Q_EMIT statusChanged(ConnectionStatus::Connecting);
    // A slot may have destroyed this Client here too, so the callers use only what this returns.
    return {core, natsOpts};
}

void Client::connectToServer(const Options& opts) {
    const auto started = startConnecting(opts);
    const std::shared_ptr<ConnectionCore>& core = started.first;
    natsConnection* conn = nullptr;
    const natsStatus s = natsConnection_Connect(&conn, started.second.get());
    if (s != NATS_OK) {
        reportFailedConnect(*core, s);
        throw Exception(s);
    }
    if (!adopt(core, conn))
        closeNow(conn);
}

void Client::connectToServer(const QUrl& address) {
    Options connOpts;
    connOpts.servers += address;
    connectToServer(connOpts);
}

void Client::connectToServerAsync(const Options& opts) {
    auto started = startConnecting(opts);
    connectionPool().start([core = std::move(started.first), natsOpts = std::move(started.second)] {
        connectInBackground(core, natsOpts.get());
    });
}

void Client::connectToServerAsync(const QUrl& address) {
    Options connOpts;
    connOpts.servers += address;
    connectToServerAsync(connOpts);
}

void Client::close() noexcept {
    const auto [conn, status] = detach(m_core);
    if (conn)
        closeNow(conn);

    // cnats fails these too once it has delivered the subscriptions' last callbacks, which may be later.
    for (const auto& pending : m_pendingAsyncRequests) {
        if (const auto future_iface = pending.lock(); future_iface && !future_iface->isFinished()) {
            future_iface->reportException(Exception(NATS_CONNECTION_CLOSED));
            future_iface->reportFinished();
        }
    }
    m_pendingAsyncRequests.clear();

    if (status != ConnectionStatus::Closed)
        Q_EMIT statusChanged(ConnectionStatus::Closed);
}

void Client::closeAsync() noexcept {
    const auto [conn, status] = detach(m_core);
    if (conn)
        closeInBackground(conn);
    m_pendingAsyncRequests.clear();

    if (status != ConnectionStatus::Closed)
        Q_EMIT statusChanged(ConnectionStatus::Closed);
}

natsConnection* Client::getNatsConnection() const {
    if (!m_core)
        return nullptr;
    const std::lock_guard lock(m_core->mutex);
    return m_core->conn;
}

natsConnection* Client::connection() const {
    natsConnection* const conn = getNatsConnection();
    if (!conn)
        throw Exception(NATS_NOT_YET_CONNECTED);
    return conn;
}

void Client::publish(const Message& msg) {
    natsConnection* const conn = connection();
    convertAndHandle(msg, nullptr, [&](const auto& c) {
        checkError(natsConnection_PublishMsg(conn, c.get()));
    });
}

Message Client::request(const Message& msg, NatsTimeout timeout) {
    natsConnection* const conn = connection();
    return convertAndHandle(msg, nullptr, [&](const auto& c) {
        natsMsg* replyMsg;
        checkError(natsConnection_RequestMsg(&replyMsg, conn, c.get(), timeout.count()));
        return fromC(NatsMsgPtr(replyMsg));
    });
}

QFuture<Message> Client::asyncRequest(const Message& msg, NatsTimeout timeout) {
    natsConnection* const conn = connection();
    const QByteArray inbox = Client::newInbox();

    // QFutureInterface is undocumented; Qt6 provides QPromise instead
    // based on https://stackoverflow.com/questions/59197694/qt-how-to-create-a-qfuture-from-a-thread
    auto* const holder = new FutureHolder(std::make_shared<QFutureInterface<Message>>());
    natsSubscription* subscription = nullptr;
    const natsStatus s = natsConnection_SubscribeTimeout(
        &subscription, conn, inbox.constData(), timeout.count(), &asyncRequestCallback, holder
    );
    if (s != NATS_OK) {
        delete holder;
        throw Exception(s);
    }
    (void)natsSubscription_SetOnCompleteCB(subscription, &asyncRequestComplete, holder);
    (*holder)->reportStarted();
    // Once the request is published, cnats may free the holder at any time.
    QFuture<Message> f = (*holder)->future();
    m_pendingAsyncRequests.erase(
        std::remove_if(
            m_pendingAsyncRequests.begin(),
            m_pendingAsyncRequests.end(),
            [](const std::weak_ptr<QFutureInterface<Message>>& pending) { return pending.expired(); }
        ),
        m_pendingAsyncRequests.end()
    );
    m_pendingAsyncRequests.push_back(*holder);

    try {
        checkError(natsSubscription_AutoUnsubscribe(subscription, 1));
        // can't do msg.reply = inbox; publish(msg); because "msg" is constant
        convertAndHandle(msg, inbox.constData(), [&](const auto& p) {
            checkError(natsConnection_PublishMsg(conn, p.get()));
        });
    } catch (...) {
        natsSubscription_Destroy(subscription);
        throw;
    }
    return f;
}

Subscription* Client::subscribe(const QString& subject) {
    natsConnection* const conn = connection();
    // avoid a memory leak if checkError throws
    // can't use make_unique because Subscription's constructor is private
    auto sub = std::unique_ptr<Subscription>(new Subscription(m_registry));
    natsSubscription* natsSub = nullptr;
    checkError(
        natsConnection_Subscribe(&natsSub, conn, subject.toUtf8().constData(), &subscriptionCallback, sub->m_relay)
    );
    (void)attachRelay(sub->m_relay, natsSub);
    sub->setParent(this);
    return sub.release();
}

Subscription* Client::subscribe(const QString& subject, const QString& queueGroup) {
    natsConnection* const conn = connection();
    auto sub = std::unique_ptr<Subscription>(new Subscription(m_registry));
    natsSubscription* natsSub = nullptr;
    checkError(natsConnection_QueueSubscribe(
        &natsSub,
        conn,
        subject.toUtf8().constData(),
        queueGroup.toUtf8().constData(),
        &subscriptionCallback,
        sub->m_relay
    ));
    (void)attachRelay(sub->m_relay, natsSub);
    sub->setParent(this);
    return sub.release();
}

bool Client::ping(NatsTimeout timeout) noexcept {
    natsConnection* const conn = getNatsConnection();
    if (!conn)
        return false;
    const natsStatus s = natsConnection_FlushTimeout(conn, timeout.count());
    return (s == NATS_OK);
}

QUrl Client::currentServer() const {
    natsConnection* const conn = getNatsConnection();
    if (!conn)
        return QUrl();
    char buffer[500];
    const natsStatus s = natsConnection_GetConnectedUrl(conn, buffer, sizeof(buffer));
    if (s != NATS_OK) {
        return QUrl();
    }
    return QUrl(QString::fromUtf8(buffer));
}

ConnectionStatus Client::status() const {
    if (!m_core)
        return ConnectionStatus::Closed;
    const std::lock_guard lock(m_core->mutex);
    return m_core->status;
}

QString Client::errorString() const {
    if (!m_core)
        return QString();
    const std::lock_guard lock(m_core->mutex);
    if (!m_core->conn)
        return m_core->lastError;
    const char* buffer = nullptr;
    natsConnection_GetLastError(m_core->conn, &buffer);
    return QString::fromUtf8(buffer);
}

QByteArray Client::newInbox() {
    natsInbox* inbox = nullptr;
    natsInbox_Create(&inbox);
    const QByteArray result(inbox);
    natsInbox_Destroy(inbox);
    return result;
}

Subscription::Subscription(std::shared_ptr<SubscriptionRegistry> registry)
    : QObject(nullptr)
    , m_relay(new SubscriptionRelay(this, std::move(registry))) {}

Subscription::~Subscription() noexcept {
    natsSubscription* sub = nullptr;
    {
        const std::lock_guard lock(m_relay->mutex);
        m_relay->target = nullptr;
        sub = std::exchange(m_relay->sub, nullptr);
    }
    if (sub) {
        m_relay->registry->remove(sub);
        natsSubscription_Destroy(sub);
    }
    m_relay->release();
}

#pragma endregion
