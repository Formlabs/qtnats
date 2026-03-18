/* Copyright(c) 2021-2022 Petro Kazmirchuk https://github.com/Kazmirchuk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License.You may obtain a copy of the License at http ://www.apache.org/licenses/LICENSE-2.0 Unless required by
applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the License for the specific language
governing permissions and  limitations under the License.
*/

#include <algorithm>

#include <QFutureInterface>
#include <QThread>

#include "qtnats/qtnats.h"
#include "qtnats/qtnats_p.h"

using namespace QtNats;

static QString getNatsErrorText(natsStatus status) {
    if (status == NATS_OK)
        return QString();

    return QString::fromUtf8(natsStatus_GetText(status));
}

// need to pass it through queued signal-slot connections
static const int messageTypeId = qRegisterMetaType<Message>();

// =============================================================================
// Data types  (analogous to nats.h types)
// =============================================================================
#pragma region Data types

Message::Message(natsMsg* msg) noexcept : m_natsMsg(msg, &natsMsg_Destroy) {
    subject = QByteArray(natsMsg_GetSubject(msg));
    data = QByteArray(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    reply = QByteArray(natsMsg_GetReply(msg));

    const char** keys = nullptr;
    int keyCount = 0;

    natsStatus s = natsMsgHeader_Keys(msg, &keys, &keyCount);
    if (s != NATS_OK || keyCount == 0)
        return;

    // handle message headers
    for (int i = 0; i < keyCount; i++) {
        const char** values = nullptr;
        int valueCount = 0;
        s = natsMsgHeader_Values(msg, keys[i], &values, &valueCount);
        if (s != NATS_OK)
            continue;
        const QByteArray key(keys[i]);

        for (int j = 0; j < valueCount; j++) {
            const QByteArray value(values[j]);
            headers.insert(key, value);
        }
        free(values);
    }

    free(keys);
};

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

static void errorHandler(natsConnection* /*nc*/, natsSubscription* /*subscription*/, natsStatus err, void* closure) {
    auto* const c = reinterpret_cast<Client*>(closure);
    Q_EMIT c->errorOccurred(err, getNatsErrorText(err));
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

Client::Client(QObject* parent) : QObject(parent), semaphore(1) {
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

Message Client::request(const Message& msg, int64_t timeout) {
    return convertAndHandle(msg, nullptr, [&](const auto& c) {
        natsMsg* replyMsg;
        checkError(natsConnection_RequestMsg(&replyMsg, m_conn, c.get(), timeout));
        return fromC(NatsMsgPtr(replyMsg));
    });
}

QFuture<Message> Client::asyncRequest(const Message& msg, int64_t timeout) {
    // QFutureInterface is undocumented; Qt6 provides QPromise instead
    // based on https://stackoverflow.com/questions/59197694/qt-how-to-create-a-qfuture-from-a-thread
    auto future_iface = std::make_shared<QFutureInterface<Message> >();
    const QByteArray inbox = Client::newInbox();

    natsSubscription* subscription = nullptr;

    checkError(natsConnection_SubscribeTimeout(
        &subscription, m_conn, inbox.constData(), timeout, &asyncRequestCallback, future_iface.get()
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

Subscription* Client::subscribe(const QByteArray& subject) {
    // avoid a memory leak if checkError throws
    // can't use make_unique because Subscription's constructor is private
    auto sub = std::unique_ptr<Subscription>(new Subscription(nullptr));
    checkError(natsConnection_Subscribe(&sub->m_sub, m_conn, subject.constData(), &subscriptionCallback, sub.get()));
    sub->setParent(this);
    return sub.release();
}

Subscription* Client::subscribe(const QByteArray& subject, const QByteArray& queueGroup) {
    auto sub = std::unique_ptr<Subscription>(new Subscription(nullptr));
    checkError(natsConnection_QueueSubscribe(
        &sub->m_sub, m_conn, subject.constData(), queueGroup.constData(), &subscriptionCallback, sub.get()
    ));
    sub->setParent(this);
    return sub.release();
}

bool Client::ping(int64_t timeout) noexcept {
    const natsStatus s = natsConnection_FlushTimeout(m_conn, timeout);
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

Subscription::~Subscription() noexcept { natsSubscription_Destroy(m_sub); }

#pragma endregion
