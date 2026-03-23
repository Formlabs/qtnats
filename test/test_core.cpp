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

#include <iostream>

#include <QCoreApplication>
#include <QMetaEnum>
#include <QProcess>

#include <QtTest>

using namespace std;
using namespace QtNats;

template <typename T>
QString enumToString(T value) {
    int castValue = static_cast<int>(value);
    return QMetaEnum::fromType<T>().valueToKey(castValue);
}

class CoreTestCase : public QObject {
    Q_OBJECT

    QProcess natsServer;

private Q_SLOTS:
    void initTestCase();
    void cleanupTestCase();

    void subscribe();
    void request();
    void asyncRequest();
};

void CoreTestCase::initTestCase() {
    connect(&natsServer, &QProcess::stateChanged, [](QProcess::ProcessState newState) {
        cout << "nats-server: " << qPrintable(enumToString(newState)) << endl;
    });

    natsServer.start("nats-server", QStringList());
    QVERIFY(natsServer.waitForStarted());
    QTest::qWait(1000);
}

void CoreTestCase::cleanupTestCase() {
    if (natsServer.state() != QProcess::NotRunning) {
        natsServer.close();
        if (!natsServer.waitForFinished(3000)) {
            natsServer.kill();
            natsServer.waitForFinished();
        }
    }
}

void CoreTestCase::subscribe() {
    try {
        Client subscriber;
        connect(&subscriber, &Client::statusChanged, [](ConnectionStatus s) {
            cout << "Connection status: " << qPrintable(enumToString(s)) << endl;
        });
        subscriber.connectToServer(QUrl("nats://localhost:4222"));

        auto sub = subscriber.subscribe("test_subject");

        QList<Message> msgList;
        connect(sub, &Subscription::received, [&msgList](Message message) { msgList += message; });

        subscriber.ping(); // ensure the server received SUB

        // Use a second QtNats client to publish instead of the nats CLI
        Client publisher;
        publisher.connectToServer(QUrl("nats://localhost:4222"));
        for (int i = 0; i < 100; i++) {
            publisher.publish(Message("test_subject", "hello"));
        }
        publisher.ping(); // flush

        QTest::qWait(1000);
        QCOMPARE(msgList.size(), 100);
        for (const Message& m : msgList) {
            QCOMPARE(m.subject, "test_subject");
            QCOMPARE(m.data, "hello");
        }
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void CoreTestCase::request() {
    try {
        // Set up a responder using a second QtNats client
        Client responder;
        responder.connectToServer(QUrl("nats://localhost:4222"));
        auto responderSub = responder.subscribe("service");
        connect(responderSub, &Subscription::received, [&responder](Message message) {
            responder.publish(Message(QString::fromUtf8(message.reply), "bla"));
        });
        responder.ping(); // ensure subscription is active

        Client requester;
        requester.connectToServer(QUrl("nats://localhost:4222"));

        for (int i = 0; i < 100; i++) {
            Message response = requester.request(Message("service", "foo"), 1000);
            QCOMPARE(response.data, "bla");
        }
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void CoreTestCase::asyncRequest() {
    try {
        // Set up a responder using a second QtNats client
        Client responder;
        responder.connectToServer(QUrl("nats://localhost:4222"));
        auto responderSub = responder.subscribe("service_async");
        connect(responderSub, &Subscription::received, [&responder](Message message) {
            responder.publish(Message(QString::fromUtf8(message.reply), "bla"));
        });
        responder.ping(); // ensure subscription is active

        Client requester;
        requester.connectToServer(QUrl("nats://localhost:4222"));
        QList<QFuture<Message>> futuresList;
        for (int i = 0; i < 100; i++) {
            futuresList += requester.asyncRequest(Message("service_async", "bar"));
        }
        QTest::qWait(2000);

        requester.close();
        QCOMPARE(futuresList.size(), 100);

        for (const QFuture<Message>& f : futuresList) {
            QCOMPARE(f.isFinished(), true);
            QCOMPARE(f.result().data, "bla");
        }
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

QTEST_GUILESS_MAIN(CoreTestCase)
#include "test_core.moc"
