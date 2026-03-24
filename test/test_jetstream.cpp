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
#include <QDir>
#include <QProcess>

#include <QtTest>

using namespace QtNats;

template<typename T>
QString enumToString(T value) {
    int castValue = static_cast<int>(value);
    return QMetaEnum::fromType<T>().valueToKey(castValue);
}

class JetStreamTestCase : public QObject {
    Q_OBJECT

    QProcess natsServer;
    QProcess natsCli;

private Q_SLOTS:
    void initTestCase();
    void cleanupTestCase();

    void publish();
    void pullSubscribe();
    void pushSubscribe();
};

void JetStreamTestCase::initTestCase() {
    connect(&natsServer, &QProcess::stateChanged, [](QProcess::ProcessState newState) {
        std::cout << "nats-server: " << qPrintable(enumToString(newState)) << std::endl;
    });

    natsServer.start("nats-server", QStringList() << "-js");
    natsServer.waitForStarted();
    QTest::qWait(1000);

    Client c;
    c.connectToServer(QUrl("nats://localhost:4222"));
    const JetStream* js = c.jetStream();

    JsStreamConfig config;
    config.name = "MY_STREAM";
    config.subjects = {"test.*"};
    config.retention = JsRetentionPolicy::Limits;
    config.discard = JsDiscardPolicy::Old;
    config.storage = JsStorageType::Memory;
    config.maxConsumers = std::nullopt;
    config.maxMsgs = std::nullopt;
    config.maxBytes = std::nullopt;
    config.maxAge = std::nullopt;
    config.maxMsgsPerSubject = std::nullopt;
    config.maxMsgSize = std::nullopt;

    const JsStreamInfo jsStreamInfo = js->addStream(config);
    QVERIFY(jsStreamInfo.config.name == config.name);

    natsCli.setProcessChannelMode(QProcess::ForwardedChannels);
    natsCli.start("nats", QStringList() << "stream" << "info" << config.name);
    QVERIFY2(natsCli.waitForFinished(), qPrintable(natsCli.errorString()));
    QVERIFY2(natsCli.exitCode() == 0, "nats CLI failed (see output above)");
}

void JetStreamTestCase::cleanupTestCase() {
    natsServer.close();
    natsServer.waitForFinished();
}

// Verifies JetStream publish: a synchronous publish returns an ack with the correct stream name
// and a positive sequence number, and five async publishes complete without error.
void JetStreamTestCase::publish() {
    try {
        Client c;
        c.connectToServer(QUrl("nats://localhost:4222"));

        const auto js = c.jetStream();

        connect(js, &JetStream::errorOccurred, [](natsStatus error, jsErrCode jsErr, const QString &text, Message msg) {
            std::cout << "JS error: " << qPrintable(text) << std::endl;
        });

        auto ack = js->publish(Message("test.1", "HI"), {});

        QCOMPARE(ack.stream, "MY_STREAM");

        for (int i = 0; i < 5; i++) {
            js->asyncPublish(Message("test.1", "HI"), {.timeout = NatsTimeout{1000}});
        }
        js->waitForPublishCompleted();
    } catch (const QException &e) {
        QFAIL(e.what());
    }
}

void JetStreamTestCase::pullSubscribe() {
    try {
        Client c;
        c.connectToServer(QUrl("nats://localhost:4222"));

        const auto js = c.jetStream();

        JsConsumerConfig config;
        config.name = "PULL_CONSUMER";
        config.deliverPolicy = JsDeliverPolicy::All;
        config.ackPolicy = JsAckPolicy::Explicit;
        config.replayPolicy = JsReplayPolicy::Instant;
        config.maxDeliver = 5;
        config.filterSubject = "test.pull";

        constexpr auto streamName = "MY_STREAM";
        const auto consumerInfo = js->addConsumer(streamName, config);
        QVERIFY(consumerInfo.name == config.name);

        natsCli.setProcessChannelMode(QProcess::ForwardedChannels);
        natsCli.start("nats", QStringList() << "consumer" << "info" << streamName << config.name.value());
        QVERIFY2(natsCli.waitForFinished(), qPrintable(natsCli.errorString()));
        QVERIFY2(natsCli.exitCode() == 0, "nats CLI failed (see output above)");

        // Publish messages with headers
        const Message pubMessage{"test.pull", "hello JS", {{"hdr1", "val1"}}};
        for (auto i = 0; i < 10; i++) {
            c.publish(pubMessage);
        }

        // Pull subscribe and fetch
        const auto sub = js->pullSubscribe("test.pull", streamName, config.name.value());
        auto msgList = sub->fetch(10);

        QCOMPARE(msgList.size(), 10);
        for (const Message m: msgList) {
            QCOMPARE(m.data, "hello JS");
            QCOMPARE(m.subject, "test.pull");
            auto val = m.headers.values("hdr1");
            QCOMPARE(val.size(), 1);
            QCOMPARE(val[0], "val1");
            m.ack();
        }

        // Cleanup consumer
        js->deleteConsumer(streamName, config.name.value());
    } catch (const QException &e) {
        QFAIL(e.what());
    }
}

/// Verifies push-based consumption: programmatically creates a durable push consumer with a deliver
/// subject, subscribes via Qt signal, publishes 10 messages, and confirms all are delivered.
void JetStreamTestCase::pushSubscribe() {
    try {
        Client c;
        c.connectToServer(QUrl("nats://localhost:4222"));

        const auto js = c.jetStream();

        JsConsumerConfig config;
        config.name = "PUSH_CONSUMER";
        config.deliverPolicy = JsDeliverPolicy::Last;
        config.ackPolicy = JsAckPolicy::None;
        config.replayPolicy = JsReplayPolicy::Instant;
        config.maxDeliver = 5;
        config.filterSubject = "test.push";
        config.deliverSubject = "delivery";

        constexpr auto streamName = "MY_STREAM";
        const auto consumerInfo = js->addConsumer(streamName, config);
        QVERIFY(consumerInfo.name == config.name);

        natsCli.setProcessChannelMode(QProcess::ForwardedChannels);
        natsCli.start("nats", QStringList() << "consumer" << "info" << streamName << config.name.value());
        QVERIFY2(natsCli.waitForFinished(), qPrintable(natsCli.errorString()));
        QVERIFY2(natsCli.exitCode() == 0, "nats CLI failed (see output above)");

        const auto sub = js->subscribe("test.push", streamName, config.name.value());
        // can we miss a message if "connect" is not fast enough?
        // apparently, consumer's deliver_subject does not matter here
        QList<Message> msgList;
        connect(sub, &Subscription::received, [&msgList](Message message) {
            msgList += message;
        });

        // Publish messages
        const Message pubMessage{"test.push", "hello JS again"};
        for (auto i = 0; i < 10; i++) {
            c.publish(pubMessage);
        }

        QTest::qWait(1000);

        QCOMPARE(msgList.size(), 10);
        for (const Message m: msgList) {
            QCOMPARE(m.data, "hello JS again");
            QCOMPARE(m.subject, "test.push");
        }

        // Cleanup consumer
        js->deleteConsumer(streamName, config.name.value());
    } catch (const QException &e) {
        QFAIL(e.what());
    }
}

QTEST_GUILESS_MAIN(JetStreamTestCase)

#include "test_jetstream.moc"
