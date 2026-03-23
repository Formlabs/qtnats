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

class JetStreamTestCase : public QObject {
    Q_OBJECT

    QProcess natsServer;
    std::unique_ptr<Client> client;
    JetStream* js = nullptr;

private Q_SLOTS:
    void initTestCase();
    void cleanupTestCase();

    void streamManagement();
    void publish();
    void pullSubscribe();
    void pushSubscribe();
};

void JetStreamTestCase::initTestCase() {
    connect(&natsServer, &QProcess::stateChanged, [](QProcess::ProcessState newState) {
        cout << "nats-server: " << qPrintable(enumToString(newState)) << endl;
    });

    natsServer.start("nats-server", QStringList() << "-js");
    QVERIFY(natsServer.waitForStarted());
    QTest::qWait(1000);

    try {
        client = std::make_unique<Client>();
        client->connectToServer(QUrl("nats://localhost:4222"));

        js = client->jetStream();

        // Create a shared stream for the publish/subscribe tests
        js->addStream(JsStreamConfig{
            .name = "TEST_STREAM",
            .subjects = {"test.>"},
            .storage = JsStorageType::Memory,
        });
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void JetStreamTestCase::cleanupTestCase() {
    try {
        if (js) {
            js->deleteStream("TEST_STREAM");
        }
    } catch (const std::exception& e) {
        cerr << "cleanupTestCase: " << e.what() << endl;
    }
    js = nullptr;
    client.reset();

    if (natsServer.state() != QProcess::NotRunning) {
        natsServer.close();
        if (!natsServer.waitForFinished(3000)) {
            natsServer.kill();
            natsServer.waitForFinished();
        }
    }
}

void JetStreamTestCase::streamManagement() {
    try {
        // Create
        auto info = js->addStream(JsStreamConfig{
            .name = "MGMT_STREAM",
            .subjects = {"mgmt.>"},
            .storage = JsStorageType::Memory,
        });
        QCOMPARE(info.config.name, "MGMT_STREAM");
        QCOMPARE(info.config.subjects.size(), 1);
        QCOMPARE(info.config.subjects[0], "mgmt.>");
        QCOMPARE(info.config.storage, JsStorageType::Memory);

        // Update — change max messages
        auto updatedInfo = js->updateStream(JsStreamConfig{
            .name = "MGMT_STREAM",
            .subjects = {"mgmt.>"},
            .storage = JsStorageType::Memory,
            .maxMsgs = 1000,
        });
        QCOMPARE(updatedInfo.config.maxMsgs, 1000);

        // Delete
        QVERIFY(js->deleteStream("MGMT_STREAM"));

        // Delete non-existent returns false
        QVERIFY(!js->deleteStream("MGMT_STREAM"));
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void JetStreamTestCase::publish() {
    try {
        connect(js, &JetStream::errorOccurred, [](natsStatus, jsErrCode, const QString& text, Message) {
            cout << "JS error: " << qPrintable(text) << endl;
        });

        // Sync publish
        auto ack = js->publish(Message("test.1", "HI"), {});
        QCOMPARE(ack.stream, "TEST_STREAM");
        QVERIFY(ack.sequence > 0);

        // Async publish
        for (int i = 0; i < 5; i++) {
            js->asyncPublish(Message("test.1", "HI"), {.timeout = 1000});
        }
        js->waitForPublishCompleted();
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void JetStreamTestCase::pullSubscribe() {
    try {
        // Create consumer programmatically
        js->addConsumer(
            "TEST_STREAM",
            JsConsumerConfig{
                .durable = "PULL_CONSUMER",
                .ackPolicy = JsAckPolicy::Explicit,
                .filterSubject = "test.pull",
            }
        );

        // Publish messages with headers
        for (int i = 0; i < 10; i++) {
            Message msg("test.pull", "hello JS");
            msg.headers.insert("hdr1", "val1");
            js->publish(msg, {});
        }

        // Pull subscribe and fetch
        auto sub = js->pullSubscribe("test.pull", "TEST_STREAM", "PULL_CONSUMER");
        auto msgList = sub->fetch(10);

        QCOMPARE(msgList.size(), 10);
        for (const Message& m : msgList) {
            QCOMPARE(m.data, "hello JS");
            QCOMPARE(m.subject, "test.pull");
            auto val = m.headers.values("hdr1");
            QCOMPARE(val.size(), 1);
            QCOMPARE(val[0], "val1");
            m.ack();
        }

        // Cleanup consumer
        QVERIFY(js->deleteConsumer("TEST_STREAM", "PULL_CONSUMER"));
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

void JetStreamTestCase::pushSubscribe() {
    try {
        // Create push consumer programmatically
        js->addConsumer(
            "TEST_STREAM",
            JsConsumerConfig{
                .durable = "PUSH_CONSUMER",
                .ackPolicy = JsAckPolicy::None,
                .filterSubject = "test.push",
                .deliverSubject = "_INBOX.push",
            }
        );

        auto sub = js->subscribe("test.push", "TEST_STREAM", "PUSH_CONSUMER");
        QList<Message> msgList;
        connect(sub, &Subscription::received, [&msgList](Message message) { msgList += message; });

        // Publish messages
        for (int i = 0; i < 10; i++) {
            js->publish(Message("test.push", "hello JS again"), {});
        }

        QTest::qWait(1000);

        QCOMPARE(msgList.size(), 10);
        for (const Message& m : msgList) {
            QCOMPARE(m.data, "hello JS again");
            QCOMPARE(m.subject, "test.push");
        }

        // Cleanup consumer
        QVERIFY(js->deleteConsumer("TEST_STREAM", "PUSH_CONSUMER"));
    } catch (const QException& e) {
        QFAIL(e.what());
    }
}

QTEST_GUILESS_MAIN(JetStreamTestCase)

#include "test_jetstream.moc"
