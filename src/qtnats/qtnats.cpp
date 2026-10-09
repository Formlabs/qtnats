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

#include <QFutureInterface>
#include <QThread>
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

static void asyncRequestCallback(natsConnection* /*nc*/, natsSubscription* natsSub, natsMsg* msg, void* closure) {
    auto* const future_iface = reinterpret_cast<QFutureInterface<Message>*>(closure);

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
    // Do NOT delete here — the Client's m_pendingAsyncRequests vector owns the shared_ptr,
    // preventing premature destruction even if Client::close() runs concurrently.
    natsSubscription_Destroy(natsSub);
}

void Client::errorHandler(natsConnection* /*nc*/, natsSubscription* subscription, natsStatus err, void* closure) {
    auto* const c = reinterpret_cast<Client*>(closure);
    const QString text = lastErrorText(err);
    Q_EMIT c->errorOccurred(err, text);

    if (!subscription)
        return;
    // cnats keeps `subscription` alive during this callback, so no other subscription can reuse its address.
    SubscriptionRelay* const relay = c->m_registry->retain(subscription);
    if (!relay)
        return;
    relay->withTarget([&](Subscription& sub) { Q_EMIT sub.errorOccurred(err, text); });
    relay->release();
}

void Client::closedConnectionHandler(natsConnection* /*nc*/, void* closure) {
    auto* const c = reinterpret_cast<Client*>(closure);
    // can ask for last error here?
    Q_EMIT c->statusChanged(ConnectionStatus::Closed);
    c->semaphore.release();
}

static void reconnectedHandler(natsConnection* /*nc*/, void* closure) {
    auto* const c = reinterpret_cast<Client*>(closure);
    Q_EMIT c->statusChanged(ConnectionStatus::Connected);
}

static void disconnectedHandler(natsConnection* /*nc*/, void* closure) {
    auto* const c = reinterpret_cast<Client*>(closure);
    Q_EMIT c->statusChanged(ConnectionStatus::Disconnected);
}

Client::Client(QObject* parent)
    : QObject(parent)
    , semaphore(1)
    , m_registry(std::make_shared<SubscriptionRegistry>())
    , m_subscribePool(new QThreadPool(this)) {
    m_subscribePool->setMaxThreadCount(4);
    const int cpuCoresCount = QThread::idealThreadCount(); // this function may fail, thus the check
    if (cpuCoresCount >= 2) {
        nats_SetMessageDeliveryPoolSize(cpuCoresCount);
    }
}

Client::~Client() noexcept { close(); }

void Client::connectToServer(const Options& opts) {
    convertAndHandle(opts, [&](const auto& c) {
        natsOptions* const nats_opts = c.get();

        // don't create a thread for each subscription, since we may have a lot of subscriptions
        // number of threads in the pool is set by nats_SetMessageDeliveryPoolSize above
        natsOptions_UseGlobalMessageDelivery(nats_opts, true);

        natsOptions_SetErrorHandler(nats_opts, &errorHandler, this);
        natsOptions_SetClosedCB(nats_opts, &closedConnectionHandler, this);
        natsOptions_SetDisconnectedCB(nats_opts, &disconnectedHandler, this);
        natsOptions_SetReconnectedCB(nats_opts, &reconnectedHandler, this);

        Q_EMIT statusChanged(ConnectionStatus::Connecting);
        checkError(natsConnection_Connect(&m_conn, nats_opts));
        Q_EMIT statusChanged(ConnectionStatus::Connected);
        // TODO handle reopening
    });
}

void Client::connectToServer(const QUrl& address) {
    Options connOpts;
    connOpts.servers += address;
    connectToServer(connOpts);
}

void Client::close() noexcept {
    if (!m_conn) {
        return;
    }
    // sync this thread with closedConnectionHandler otherwise I get a crash when trying to Q_EMIT
    // c->statusChanged(ConnectionStatus::Closed);
    semaphore.acquire();
    natsConnection_Close(m_conn);
    semaphore.acquire(); // here we'll wait until the callback is done
    semaphore.release();
    natsConnection_Destroy(m_conn);
    m_conn = nullptr;

    // After natsConnection_Destroy, no more callbacks will fire.
    // Resolve any async-request futures that were never completed.
    for (auto& fi : m_pendingAsyncRequests) {
        if (!fi->isFinished()) {
            fi->reportException(Exception(NATS_CONNECTION_CLOSED));
            fi->reportFinished();
        }
    }
    m_pendingAsyncRequests.clear();
}

void Client::publish(const Message& msg) {
    convertAndHandle(msg, nullptr, [&](const auto& c) {
        checkError(natsConnection_PublishMsg(m_conn, c.get()));
    });
}

Message Client::request(const Message& msg, NatsTimeout timeout) {
    return convertAndHandle(msg, nullptr, [&](const auto& c) {
        natsMsg* replyMsg;
        checkError(natsConnection_RequestMsg(&replyMsg, m_conn, c.get(), timeout.count()));
        return fromC(NatsMsgPtr(replyMsg));
    });
}

QFuture<Message> Client::asyncRequest(const Message& msg, NatsTimeout timeout) {
    // QFutureInterface is undocumented; Qt6 provides QPromise instead
    // based on https://stackoverflow.com/questions/59197694/qt-how-to-create-a-qfuture-from-a-thread
    auto future_iface = std::make_shared<QFutureInterface<Message> >();
    const QByteArray inbox = Client::newInbox();

    natsSubscription* subscription = nullptr;

    checkError(natsConnection_SubscribeTimeout(
        &subscription, m_conn, inbox.constData(), timeout.count(), &asyncRequestCallback, future_iface.get()
    ));
    try {
        checkError(natsSubscription_AutoUnsubscribe(subscription, 1));
        // can't do msg.reply = inbox; publish(msg); because "msg" is constant
        convertAndHandle(msg, inbox.constData(), [&](const auto& p) {
            checkError(natsConnection_PublishMsg(m_conn, p.get()));
        });
    } catch (...) {
        // Destroy the subscription before the shared_ptr destroys the QFutureInterface,
        // otherwise the subscription callback would fire against freed memory.
        natsSubscription_Destroy(subscription);
        throw;
    }

    future_iface->reportStarted();
    auto f = future_iface->future();

    // Purge completed futures, then track this one so close() can resolve it
    // if the connection is torn down before the callback fires.
    m_pendingAsyncRequests.erase(
        std::remove_if(
            m_pendingAsyncRequests.begin(),
            m_pendingAsyncRequests.end(),
            [](const std::shared_ptr<QFutureInterface<Message> >& fi) { return fi->isFinished(); }
        ),
        m_pendingAsyncRequests.end()
    );
    m_pendingAsyncRequests.push_back(std::move(future_iface));

    return f;
}

Subscription* Client::subscribe(const QString& subject) {
    // avoid a memory leak if checkError throws
    // can't use make_unique because Subscription's constructor is private
    auto sub = std::unique_ptr<Subscription>(new Subscription(m_registry));
    natsSubscription* natsSub = nullptr;
    checkError(
        natsConnection_Subscribe(&natsSub, m_conn, subject.toUtf8().constData(), &subscriptionCallback, sub->m_relay)
    );
    (void)attachRelay(sub->m_relay, natsSub);
    sub->setParent(this);
    return sub.release();
}

Subscription* Client::subscribe(const QString& subject, const QString& queueGroup) {
    auto sub = std::unique_ptr<Subscription>(new Subscription(m_registry));
    natsSubscription* natsSub = nullptr;
    checkError(natsConnection_QueueSubscribe(
        &natsSub,
        m_conn,
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
    const natsStatus s = natsConnection_FlushTimeout(m_conn, timeout.count());
    return (s == NATS_OK);
}

QUrl Client::currentServer() const {
    char buffer[500];
    const natsStatus s = natsConnection_GetConnectedUrl(m_conn, buffer, sizeof(buffer));
    if (s != NATS_OK) {
        return QUrl();
    }
    return QUrl(QString::fromUtf8(buffer));
}

ConnectionStatus Client::status() const { return ConnectionStatus(natsConnection_Status(m_conn)); }

QString Client::errorString() const {
    // TODO handle when m_conn==nullptr ?
    const char* buffer = nullptr;
    natsConnection_GetLastError(m_conn, &buffer);
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
