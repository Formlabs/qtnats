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

#include <qtnats/qtnats.h>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>
#include <vector>

#include <QCoreApplication>
#include <QElapsedTimer>
#include <QMetaEnum>
#include <QProcess>
#include <QSignalSpy>

#include <QtTest>

using namespace std;
using namespace QtNats;

template <typename T>
QString enumToString(T value) {
    int castValue = static_cast<int>(value);
    return QMetaEnum::fromType<T>().valueToKey(castValue);
}

namespace {

const QUrl localServer("nats://localhost:4222");
// Nothing listens on port 1.
const QUrl closedPort("nats://127.0.0.1:1");
// TEST-NET-1 is never assigned, so a connect either times out or fails at once, depending on the network.
const QUrl unroutable("nats://192.0.2.1:4222");
// Generous for CI; a blocking call would take the whole connect timeout.
constexpr qint64 nonBlockingMs = 50;

// connectToServer() returns before the connection is up.
[[nodiscard]] bool connectAndWait(Client& c, const QUrl& url = localServer) {
    c.connectToServer(url);
    return QTest::qWaitFor([&c] { return c.status() == ConnectionStatus::Connected; }, 5000);
}

Options optionsFor(const QUrl& url) {
    Options options;
    options.servers = {url};
    options.timeout = NatsTimeout{1000};
    return options;
}

} // namespace

class CoreTestCase : public QObject {
    Q_OBJECT

    QProcess natsServer;

    void startServer();

private Q_SLOTS:
    void initTestCase();
    void cleanupTestCase();

    void subscribe();
    void deleteFromDirectSlot();
    void emptyPayload();
    void request();
    void asyncRequest();
    void pendingRequestFailsOnClose();
    void connectReportsConnectingThenConnected();
    void notConnectedYetThrows();
    void connectFailureIsReportedAsynchronously();
    void connectToUnroutableAddressDoesNotBlock();
    void destroyWhileConnectingDoesNotBlock();
    void destroyWhileConnectedDoesNotBlock();
    void closeReportsClosedAndAllowsReconnecting();
    void connectTwiceThrows();
    void manyClients();
    void sendAsapAndWriteDeadline();
    // Restarts the server, so it runs last.
    void reconnectsAfterServerRestart();
};

void CoreTestCase::startServer() {
    natsServer.start("nats-server", QStringList());
    natsServer.waitForStarted();
    QTest::qWait(1000);
}

void CoreTestCase::initTestCase() {
    connect(&natsServer, &QProcess::stateChanged, [](QProcess::ProcessState newState) {
        cout << "nats-server: " << qPrintable(enumToString(newState)) << endl;
    });
    startServer();
}
void CoreTestCase::cleanupTestCase() {
    natsServer.close();
    natsServer.waitForFinished();
}

void CoreTestCase::deleteFromDirectSlot() {
    try {
        Client c;
        QVERIFY(connectAndWait(c));
        Subscription* sub = c.subscribe("test_delete");
        // Qt forbids deleting an object with a parent from another thread.
        sub->setParent(nullptr);
        std::atomic<int> received{0};
        connect(
            sub,
            &Subscription::received,
            sub,
            [sub, &received](const Message&) {
                if (received.fetch_add(1) == 0)
                    delete sub;
            },
            Qt::DirectConnection
        );
        c.ping(); // ensure the server received SUB

        for (int i = 0; i < 10; i++)
            c.publish(Message("test_delete", "x"));
        c.ping();
        QTRY_COMPARE(received.load(), 1);
        QTest::qWait(50);
        QCOMPARE(received.load(), 1);
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void CoreTestCase::subscribe() {
    try {
        Client c;

        connect(&c, &Client::statusChanged, [](ConnectionStatus s) {
            cout << "Connection status: " << qPrintable(enumToString(s)) << endl;
        });

        QVERIFY(connectAndWait(c));
        auto sub = c.subscribe("test_subject");

        QList<Message> msgList;
        connect(sub, &Subscription::received, [&msgList](Message message) { msgList += message; });

        c.ping(); // ensure the server received SUB

        QProcess natsCli;
        natsCli.start("nats", QStringList() << "publish" << "--count=100" << "test_subject" << "hello");
        natsCli.waitForFinished();

        QTest::qWait(1000);
        QCOMPARE(msgList.size(), 100);
        for (Message m : msgList) {
            QCOMPARE(m.subject, "test_subject");
            QCOMPARE(m.data, "hello");
        }
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void CoreTestCase::emptyPayload() {
    try {
        Client c;
        QVERIFY(connectAndWait(c));
        const std::unique_ptr<Subscription> sub(c.subscribe("test_empty"));
        QList<Message> received;
        connect(sub.get(), &Subscription::received, this, [&received](const Message& m) { received += m; });
        c.ping(); // ensure the server received SUB

        c.publish(Message("test_empty", QByteArray()));
        QTRY_COMPARE(received.size(), 1);
        QVERIFY(received[0].data.isEmpty());
        QVERIFY(!received[0].data.isNull());
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void CoreTestCase::request() {
    QProcess responder;
    try {
        Client c;
        QVERIFY(connectAndWait(c));

        responder.start("nats", QStringList() << "reply" << "service" << "bla");
        responder.waitForStarted();
        QTest::qWait(1000);

        for (int i = 0; i < 100; i++) {
            Message response = c.request(Message("service", "foo"), std::chrono::milliseconds{1000});
            QCOMPARE(response.data, "bla");
        }
    } catch (const QException& e) {
        QFAIL(e.what());
    }
    responder.close();
    responder.waitForFinished();
}

void CoreTestCase::asyncRequest() {
    QProcess responder;
    try {
        responder.start(
            "nats", QStringList() << "reply" << "service" << "bla"
        ); // can't use --count because sometimes NATS CLI exits before flushing its reply (?!)
        responder.waitForStarted();
        QTest::qWait(1000);

        Client c;
        QVERIFY(connectAndWait(c));
        QList<QFuture<Message>> futuresList;
        for (int i = 0; i < 100; i++) {
            futuresList += c.asyncRequest(Message("service", "bar"));
        }
        QTest::qWait(2000);

        c.close();
        QCOMPARE(futuresList.size(), 100);

        for (QFuture<Message> f : futuresList) {
            QCOMPARE(f.isFinished(), true);
            QCOMPARE(f.result().data, "bla");
        }
    } catch (const QException& e) {
        QFAIL(e.what());
    }
    responder.close();
    responder.waitForFinished();
}

void CoreTestCase::pendingRequestFailsOnClose() {
    try {
        Client c;
        QVERIFY(connectAndWait(c));
        // A responder that never replies keeps the request pending.
        const std::unique_ptr<Subscription> silent(c.subscribe("silent"));
        c.ping();
        QFuture<Message> f = c.asyncRequest(Message("silent", "x"), NatsTimeout{10000});

        c.close();
        QTRY_VERIFY(f.isFinished());
        try {
            f.result();
            QFAIL("The request succeeded after close()");
        } catch (const Exception& e) {
            QCOMPARE(e.errorCode, NATS_CONNECTION_CLOSED);
        }
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void CoreTestCase::connectReportsConnectingThenConnected() {
    Client c;
    QSignalSpy statuses(&c, &Client::statusChanged);
    c.connectToServer(localServer);
    QCOMPARE(c.status(), ConnectionStatus::Connecting);
    QTRY_COMPARE(statuses.count(), 2);
    QCOMPARE(statuses.at(0).at(0).value<ConnectionStatus>(), ConnectionStatus::Connecting);
    QCOMPARE(statuses.at(1).at(0).value<ConnectionStatus>(), ConnectionStatus::Connected);
    QCOMPARE(c.status(), ConnectionStatus::Connected);
    QVERIFY(c.ping());
}

void CoreTestCase::notConnectedYetThrows() {
    Client c;
    c.connectToServer(unroutable);
    try {
        c.publish(Message("anything", "x"));
        QFAIL("publish succeeded before the connection was up");
    } catch (const Exception& e) {
        QCOMPARE(e.errorCode, NATS_NOT_YET_CONNECTED);
    }
    QVERIFY(!c.ping());
}

void CoreTestCase::connectFailureIsReportedAsynchronously() {
    Client c;
    QSignalSpy errors(&c, &Client::errorOccurred);
    QElapsedTimer elapsed;
    elapsed.start();
    c.connectToServer(optionsFor(closedPort));
    QVERIFY(elapsed.elapsed() < nonBlockingMs);

    QTRY_COMPARE(c.status(), ConnectionStatus::Closed);
    QTRY_COMPARE(errors.count(), 1);
    QVERIFY(!c.errorString().isEmpty());
}

void CoreTestCase::connectToUnroutableAddressDoesNotBlock() {
    Client c;
    QElapsedTimer elapsed;
    elapsed.start();
    c.connectToServer(optionsFor(unroutable));
    QVERIFY(elapsed.elapsed() < nonBlockingMs);
    QTRY_COMPARE_WITH_TIMEOUT(c.status(), ConnectionStatus::Closed, 5000);
}

void CoreTestCase::destroyWhileConnectingDoesNotBlock() {
    auto c = std::make_unique<Client>();
    c->connectToServer(optionsFor(unroutable));
    QElapsedTimer elapsed;
    elapsed.start();
    c.reset();
    QVERIFY(elapsed.elapsed() < nonBlockingMs);
    // Let the background connect finish after its Client is gone.
    QTest::qWait(1500);
}

void CoreTestCase::destroyWhileConnectedDoesNotBlock() {
    auto c = std::make_unique<Client>();
    QVERIFY(connectAndWait(*c));
    QElapsedTimer elapsed;
    elapsed.start();
    c.reset();
    QVERIFY(elapsed.elapsed() < nonBlockingMs);
    QTest::qWait(500);
}

void CoreTestCase::closeReportsClosedAndAllowsReconnecting() {
    Client c;
    QVERIFY(connectAndWait(c));
    QSignalSpy statuses(&c, &Client::statusChanged);
    c.close();
    QCOMPARE(statuses.count(), 1);
    QCOMPARE(statuses.at(0).at(0).value<ConnectionStatus>(), ConnectionStatus::Closed);
    QCOMPARE(c.status(), ConnectionStatus::Closed);
    QVERIFY(c.getNatsConnection() == nullptr);

    QVERIFY(connectAndWait(c));
    QVERIFY(c.ping());
}

void CoreTestCase::connectTwiceThrows() {
    Client c;
    c.connectToServer(unroutable);
    try {
        c.connectToServer(localServer);
        QFAIL("A second connect succeeded while the first was in flight");
    } catch (const Exception& e) {
        QCOMPARE(e.errorCode, NATS_ILLEGAL_STATE);
    }
    c.close();

    QVERIFY(connectAndWait(c));
    try {
        c.connectToServer(localServer);
        QFAIL("A second connect succeeded while connected");
    } catch (const Exception& e) {
        QCOMPARE(e.errorCode, NATS_ILLEGAL_STATE);
    }
}

void CoreTestCase::manyClients() {
    constexpr int count = 200;
    std::vector<std::unique_ptr<Client>> clients;
    QElapsedTimer elapsed;
    elapsed.start();
    for (int i = 0; i < count; i++) {
        clients.push_back(std::make_unique<Client>());
        clients.back()->connectToServer(localServer);
    }
    const qint64 connectMs = elapsed.elapsed();
    QTRY_VERIFY_WITH_TIMEOUT(
        std::all_of(
            clients.begin(),
            clients.end(),
            [](const auto& c) { return c->status() == ConnectionStatus::Connected; }
        ),
        20000
    );

    elapsed.restart();
    clients.clear();
    const qint64 destroyMs = elapsed.elapsed();
    cout << count << " clients: connect " << connectMs << " ms, destroy " << destroyMs << " ms" << endl;
    QVERIFY(connectMs < 1000);
    QVERIFY(destroyMs < 1000);
    QTest::qWait(2000);
}

void CoreTestCase::sendAsapAndWriteDeadline() {
    try {
        Options options = optionsFor(localServer);
        options.sendAsap = true;
        options.writeDeadline = NatsTimeout{2000};
        Client c;
        c.connectToServer(options);
        QTRY_COMPARE(c.status(), ConnectionStatus::Connected);

        const std::unique_ptr<Subscription> sub(c.subscribe("test_asap"));
        int received = 0;
        connect(sub.get(), &Subscription::received, this, [&received](const Message&) { received++; });
        c.ping();
        c.publish(Message("test_asap", "x"));
        QTRY_COMPARE(received, 1);
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void CoreTestCase::reconnectsAfterServerRestart() {
    Options options = optionsFor(localServer);
    options.maxReconnect = -1;
    options.reconnectWait = 200;
    Client c;
    c.connectToServer(options);
    QTRY_COMPARE(c.status(), ConnectionStatus::Connected);

    natsServer.close();
    natsServer.waitForFinished();
    QTRY_VERIFY_WITH_TIMEOUT(c.status() != ConnectionStatus::Connected, 5000);

    startServer();
    QTRY_COMPARE_WITH_TIMEOUT(c.status(), ConnectionStatus::Connected, 10000);
    QVERIFY(c.ping());
}

QTEST_GUILESS_MAIN(CoreTestCase)
#include "test_core.moc"
