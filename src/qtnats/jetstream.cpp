/* Copyright(c) 2022 Petro Kazmirchuk https://github.com/Kazmirchuk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License.You may obtain a copy of the License at http ://www.apache.org/licenses/LICENSE-2.0 Unless required by
applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the License for the specific language
governing permissions and  limitations under the License.
*/

#include "qtnats/qtnats.h"
#include "qtnats/qtnats_p.h"

using namespace QtNats;

static void checkJsError(natsStatus s, jsErrCode js) {
    if (s == NATS_OK)
        return;
    throw JetStreamException(s, js);
}

static void jsPubErrHandler(jsCtx*, jsPubAckErr* pae, void* closure) {
    auto* const js = static_cast<JetStream*>(closure);
    const Message msg = fromC(NatsMsgPtr(pae->Msg));
    Q_EMIT js->errorOccurred(pae->Err, pae->ErrCode, QString(pae->ErrText), msg);
}

JetStream* Client::jetStream(const JsOptions& options) {
    auto* const js = new JetStream(this);
    jsOptions jsOpts = toC(options);
    jsOpts.PublishAsync.ErrHandler = &jsPubErrHandler;
    jsOpts.PublishAsync.ErrHandlerClosure = js;

    checkError(natsConnection_JetStream(&js->m_jsCtx, m_conn, &jsOpts));
    return js;
}

void Message::ack() {
    jsErrCode jsErr;
    const natsStatus s = natsMsg_AckSync(m_natsMsg.get(), nullptr, &jsErr);
    checkJsError(s, jsErr);
}

void Message::nack(int64_t delay) {
    natsStatus s;
    if (delay == -1) {
        s = natsMsg_Nak(m_natsMsg.get(), nullptr);
    } else {
        s = natsMsg_NakWithDelay(m_natsMsg.get(), delay, nullptr);
    }
    checkError(s);
}

void Message::inProgress() { checkError(natsMsg_InProgress(m_natsMsg.get(), nullptr)); }

void Message::terminate() { checkError(natsMsg_Term(m_natsMsg.get(), nullptr)); }

PullSubscription::~PullSubscription() noexcept { natsSubscription_Destroy(m_sub); }

QList<Message> PullSubscription::fetch(int batch, int64_t timeout) {
    // see also https://github.com/nats-io/nats.c/issues/545
    natsMsgList list{nullptr, 0};
    jsErrCode jsErr;
    const natsStatus s = natsSubscription_Fetch(&list, m_sub, batch, timeout, &jsErr);
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
    jsPubOptions jsOpts = toC(opts);
    return doPublish(msg, &jsOpts);
}

JsPublishAck JetStream::publish(const Message& msg, int64_t timeout) {
    jsPubOptions jsOpts;
    jsPubOptions_Init(&jsOpts);
    if (timeout != -1) {
        jsOpts.MaxWait = timeout;
    }
    return doPublish(msg, &jsOpts);
}

void JetStream::asyncPublish(const Message& msg, const JsPublishOptions& opts) {
    jsPubOptions jsOpts = toC(opts);
    doAsyncPublish(msg, &jsOpts);
}

void JetStream::asyncPublish(const Message& msg, int64_t timeout) {
    jsPubOptions jsOpts;
    jsPubOptions_Init(&jsOpts);
    if (timeout != -1) {
        jsOpts.MaxWait = timeout;
    }
    doAsyncPublish(msg, &jsOpts);
}

void JetStream::waitForPublishCompleted(int64_t timeout) {
    // TODO use QtConcurrent::run and return QFuture?
    natsStatus s = NATS_OK;
    if (timeout != -1) {
        jsPubOptions jsOpts;
        jsPubOptions_Init(&jsOpts);
        jsOpts.MaxWait = timeout;
        s = js_PublishAsyncComplete(m_jsCtx, &jsOpts);
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

Subscription* JetStream::subscribe(const QByteArray& subject, const QByteArray& stream, const QByteArray& consumer) {
    // manualAck=true: avoid _autoAckCB in cnats internals, because it takes over ownership of delivered messages
    const JsSubOptions qtOpts{stream, consumer};
    jsSubOptions subOpts = toC(qtOpts, /*manualAck=*/true);
    auto sub = std::unique_ptr<Subscription>(new Subscription(nullptr));
    jsErrCode jsErr;
    const natsStatus s = js_Subscribe(
        &sub->m_sub, m_jsCtx, subject.constData(), &subscriptionCallback, sub.get(), nullptr, &subOpts, &jsErr
    );
    checkJsError(s, jsErr);
    sub->setParent(this);
    return sub.release();
}

PullSubscription* JetStream::pullSubscribe(
    const QByteArray& subject,
    const QByteArray& stream,
    const QByteArray& consumer
) {
    auto sub = std::unique_ptr<PullSubscription>(new PullSubscription(nullptr));
    jsErrCode jsErr;
    const JsSubOptions qtOpts{stream, consumer};
    jsSubOptions subOpts = toC(qtOpts);

    const natsStatus s =
        js_PullSubscribe(&sub->m_sub, m_jsCtx, subject.constData(), consumer.constData(), nullptr, &subOpts, &jsErr);
    checkJsError(s, jsErr);
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

void JetStream::doAsyncPublish(const Message& msg, jsPubOptions* opts) {
    // js_PublishMsgAsync is tricky to manage lifetime of natsMsg, so let's go the safe way
    // TODO headers will require js_PublishMsgAsync
    checkError(js_PublishAsync(m_jsCtx, msg.subject.constData(), msg.data.constData(), msg.data.size(), opts));
}
