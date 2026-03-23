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

#include "qtnats/qtnats.h"
#include "qtnats/qtnats_p.h"

using namespace QtNats;

static void checkJsError(const natsStatus& s, const jsErrCode& js) {
    if (s == NATS_OK) {
        return;
    }
    throw JetStreamException(s, js);
}

static void jsPubErrHandler(jsCtx*, jsPubAckErr* pae, void* closure) {
    auto* const js = static_cast<JetStream*>(closure);
    const Message msg = fromC(NatsMsgPtr(pae->Msg));
    Q_EMIT js->errorOccurred(pae->Err, pae->ErrCode, QString(pae->ErrText), msg);
}

JetStream* Client::jetStream(const JsOptions& options) {
    auto* const js = new JetStream(this);
    convertAndHandle(options, [&](jsOptions& jsOpts) {
        jsOpts.PublishAsync.ErrHandler = &jsPubErrHandler;
        jsOpts.PublishAsync.ErrHandlerClosure = js;
        checkError(natsConnection_JetStream(&js->m_jsCtx, m_conn, &jsOpts));
    });
    return js;
}

JsStreamInfo Client::addStream(const JetStream* js, const JsStreamConfig& config) {
    return convertAndHandle(config, [&](jsStreamConfig& jsConfig) {
        jsStreamInfo* si;
        jsErrCode jsErr;
        // We'll just always use the options used to set up the JetStream
        const natsStatus s = js_AddStream(&si, js->getJsContext(), &jsConfig, nullptr, &jsErr);
        checkJsError(s, jsErr);
        return fromC(JsStreamInfoPtr(si));
    });
}

JsStreamInfo Client::updateStream(const JetStream* js, const JsStreamConfig& config) {
    return convertAndHandle(config, [&](jsStreamConfig& jsConfig) {
        jsStreamInfo* si;
        jsErrCode jsErr;
        // We'll just always use the options used to set up the JetStream
        const natsStatus s = js_UpdateStream(&si, js->getJsContext(), &jsConfig, nullptr, &jsErr);
        checkJsError(s, jsErr);
        return fromC(JsStreamInfoPtr(si));
    });
}

void Client::purgeStream(const JetStream* js, const QString& stream) {
    jsErrCode jsErr;
    const natsStatus s = js_PurgeStream(js->getJsContext(), stream.toUtf8().constData(), nullptr, &jsErr);
    checkJsError(s, jsErr);
}

void Client::deleteStream(const JetStream* js, const QString& stream) {
    jsErrCode jsErr;
    const natsStatus s = js_DeleteStream(js->getJsContext(), stream.toUtf8().constData(), nullptr, &jsErr);
    checkJsError(s, jsErr);
}

JsConsumerInfo Client::addConsumer(const JetStream* js, const QString& stream, const JsConsumerConfig& config) {
    return convertAndHandle(config, [&](jsConsumerConfig& jsConfig) {
        jsConsumerInfo* ci;
        jsErrCode jsErr;
        // We'll just always use the options used to set up the JetStream
        const natsStatus s =
            js_AddConsumer(&ci, js->getJsContext(), stream.toUtf8().constData(), &jsConfig, nullptr, &jsErr);
        checkJsError(s, jsErr);
        return fromC(JsConsumerInfoPtr(ci));
    });
}

JsConsumerInfo Client::updateConsumer(const JetStream* js, const QString& stream, const JsConsumerConfig& config) {
    return convertAndHandle(config, [&](jsConsumerConfig& jsConfig) {
        jsConsumerInfo* ci;
        jsErrCode jsErr;
        // We'll just always use the options used to set up the JetStream
        const natsStatus s =
            js_UpdateConsumer(&ci, js->getJsContext(), stream.toUtf8().constData(), &jsConfig, nullptr, &jsErr);
        checkJsError(s, jsErr);
        return fromC(JsConsumerInfoPtr(ci));
    });
}

JsConsumerInfo Client::getConsumerInfo(const JetStream* js, const QString& stream, const QString& consumer) {
    jsConsumerInfo* ci;
    jsErrCode jsErr;
    const natsStatus s = js_GetConsumerInfo(
        &ci, js->getJsContext(), stream.toUtf8().constData(), consumer.toUtf8().constData(), nullptr, &jsErr
    );
    checkJsError(s, jsErr);
    return fromC(JsConsumerInfoPtr(ci));
}

void Client::deleteConsumer(const JetStream* js, const QString& stream, const QString& consumer) {
    jsErrCode jsErr;
    const natsStatus s = js_DeleteConsumer(
        js->getJsContext(), stream.toUtf8().constData(), consumer.toUtf8().constData(), nullptr, &jsErr
    );
    checkJsError(s, jsErr);
}

JsConsumerPauseResponse Client::pauseConsumer(const JetStream* js, const QString& stream, const QString& consumer, NatsTimePoint pauseUntil) {
    jsConsumerPauseResponse* resp;
    jsErrCode jsErr;
    const natsStatus s = js_PauseConsumer(
        &resp,
        js->getJsContext(),
        stream.toUtf8().constData(),
        consumer.toUtf8().constData(),
        pauseUntil.time_since_epoch().count(),
        nullptr,
        &jsErr
    );
    checkJsError(s, jsErr);
    return fromC(JsConsumerPauseResponsePtr(resp));
}

void Message::ack() const {
    jsErrCode jsErr;
    const natsStatus s = natsMsg_AckSync(m_natsMsg.get(), nullptr, &jsErr);
    checkJsError(s, jsErr);
}

void Message::nack(std::optional<int64_t> delay) const {
    natsStatus s;
    if (delay.has_value()) {
        s = natsMsg_NakWithDelay(m_natsMsg.get(), delay.value(), nullptr);
    } else {
        s = natsMsg_Nak(m_natsMsg.get(), nullptr);
    }
    checkError(s);
}

void Message::inProgress() const { checkError(natsMsg_InProgress(m_natsMsg.get(), nullptr)); }

void Message::terminate() const { checkError(natsMsg_Term(m_natsMsg.get(), nullptr)); }

PullSubscription::~PullSubscription() noexcept { natsSubscription_Destroy(m_sub); }

QList<Message> PullSubscription::fetch(const int batch, const NatsTimeout timeout) const {
    // see also https://github.com/nats-io/nats.c/issues/545
    natsMsgList list{nullptr, 0};
    jsErrCode jsErr;
    const natsStatus s = natsSubscription_Fetch(&list, m_sub, batch, timeout.count(), &jsErr);
    checkJsError(s, jsErr);
    QList<Message> result;
    for (int i = 0; i < list.Count; i++) {
        result += fromC(NatsMsgPtr(list.Msgs[i]));
        list.Msgs[i] = nullptr; // natsMsgList_Destroy should destroy only the list, and keep the messages
    }
    natsMsgList_Destroy(&list);
    return result;
}

JetStream::~JetStream() noexcept { jsCtx_Destroy(m_jsCtx); }


JsPublishAck JetStream::publish(const Message& msg, const JsPublishOptions& opts) {
    return convertAndHandle(opts, [&](jsPubOptions& jsOpts) { return doPublish(msg, &jsOpts); });
}

void JetStream::asyncPublish(const Message& msg, const JsPublishOptions& opts) const {
    convertAndHandle(opts, [&](jsPubOptions& jsOpts) { doAsyncPublish(msg, &jsOpts); });
}

void JetStream::waitForPublishCompleted(const std::optional<NatsTimeout> timeout) const {
    natsStatus s = NATS_OK;

    if (timeout.has_value()) {
        convertAndHandle(JsPublishOptions{.timeout = timeout}, [&](jsPubOptions& jsOpts) {
            s = js_PublishAsyncComplete(m_jsCtx, &jsOpts);
        });
    } else {
        s = js_PublishAsyncComplete(m_jsCtx, nullptr);
    }

    if (s == NATS_TIMEOUT) {
        // optionally we can delete the messages, but they might be ACK'ed later
        // natsMsgList list;
        // js_PublishAsyncGetPendingList(&list, m_jsCtx);
        // natsMsgList_Destroy(&list);
    }
    checkError(s);
}

Subscription* JetStream::subscribe(const QString& subject, const QString& stream, const QString& consumer) {
    // manualAck=true: avoid _autoAckCB in cnats internals, because it takes over ownership of delivered messages
    auto sub = std::unique_ptr<Subscription>(new Subscription(nullptr));
    jsErrCode jsErr = {};
    convertAndHandle(JsSubOptions{stream, consumer}, /*manualAck=*/true, [&](jsSubOptions& subOpts) {
        const natsStatus s = js_Subscribe(
            &sub->m_sub,
            m_jsCtx,
            subject.toUtf8().constData(),
            &subscriptionCallback,
            sub.get(),
            nullptr,
            &subOpts,
            &jsErr
        );
        checkJsError(s, jsErr);
    });
    sub->setParent(this);
    return sub.release();
}

PullSubscription* JetStream::pullSubscribe(const QString& subject, const QString& stream, const QString& consumer) {
    auto sub = std::unique_ptr<PullSubscription>(new PullSubscription(nullptr));
    jsErrCode jsErr = {};
    convertAndHandle(JsSubOptions{stream, consumer}, /*manualAck=*/false, [&](jsSubOptions& subOpts) {
        const natsStatus s = js_PullSubscribe(
            &sub->m_sub, m_jsCtx, subject.toUtf8().constData(), consumer.toUtf8().constData(), nullptr, &subOpts, &jsErr
        );
        checkJsError(s, jsErr);
    });
    sub->setParent(this);
    return sub.release();
}

JsPublishAck JetStream::doPublish(const Message& msg, jsPubOptions* opts) {
    return convertAndHandle(msg, nullptr, [&](const auto& p) {
        auto jsErr = static_cast<jsErrCode>(0);
        jsPubAck* ack = nullptr;

        const natsStatus s = js_PublishMsg(&ack, m_jsCtx, p.get(), opts, &jsErr);
        checkJsError(s, jsErr);

        return fromC(JsPubAckPtr(ack));
    });
}

void JetStream::doAsyncPublish(const Message& msg, jsPubOptions* opts) const {
    // js_PublishMsgAsync is tricky to manage lifetime of natsMsg, so let's go the safe way
    // TODO headers will require js_PublishMsgAsync
    checkError(js_PublishAsync(m_jsCtx, msg.subject.toUtf8().constData(), msg.data.constData(), msg.data.size(), opts));
}
