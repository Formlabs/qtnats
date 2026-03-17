/* Copyright(c) 2021-2022 Petro Kazmirchuk https://github.com/Kazmirchuk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License.You may obtain a copy of the License at http ://www.apache.org/licenses/LICENSE-2.0 Unless required by
applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the License for the specific language
governing permissions and  limitations under the License.
*/

#include "qtnats/qtnats.h"
#include "qtnats/qtnats_p.h"

namespace QtNats {

void checkError(natsStatus s) {
    if (s == NATS_OK)
        return;
    throw Exception(s);
}

void subscriptionCallback(natsConnection* /*nc*/, natsSubscription* /*sub*/, natsMsg* msg, void* closure) {
    auto* const sub = reinterpret_cast<Subscription*>(closure);

    const Message m = fromC(NatsMsgPtr(msg));
    Q_EMIT sub->received(m);
}

NatsMsgPtr asC(const Message& msg, const char* reply) {
    natsMsg* cnatsMsg;

    const char* realReply = nullptr; // in asyncRequest I need to provide my own reply
    if (reply) {
        realReply = reply;
    } else if (msg.reply.size()) {
        realReply = msg.reply.constData();
    }

    checkError(natsMsg_Create(&cnatsMsg, msg.subject.constData(), realReply, msg.data.constData(), msg.data.size()));

    NatsMsgPtr msgPtr(cnatsMsg);

    auto i = msg.headers.constBegin();
    while (i != msg.headers.constEnd()) {
        checkError(natsMsgHeader_Add(cnatsMsg, i.key().constData(), i.value().constData()));
        ++i;
    }
    return msgPtr;
}

NatsOptsPtr asC(const Options& opts) {
    natsOptions* o;
    natsOptions_Create(&o);
    NatsOptsPtr ptr(o);

    if (!opts.servers.empty()) {
        QList<QByteArray> l;
        QVector<const char*> ptrs;
        for (const auto& url : opts.servers) {
            // TODO check for invalid URL
            l.append(url.toEncoded());
            ptrs.append(l.last().constData());
        }
        checkError(natsOptions_SetServers(o, ptrs.data(), static_cast<int>(ptrs.size())));
    }
    checkError(natsOptions_SetUserInfo(o, opts.user.constData(), opts.password.constData()));
    checkError(natsOptions_SetToken(o, opts.token.constData()));
    checkError(natsOptions_SetNoRandomize(o, !opts.randomize)); // NB! reverted flag
    checkError(natsOptions_SetTimeout(o, opts.timeout));
    checkError(natsOptions_SetName(o, opts.name.constData()));

    // TLS/mTLS configuration
    if (opts.secure) {
        checkError(natsOptions_SetSecure(o, true));
    }
    if (!opts.caFile.isEmpty()) {
        checkError(natsOptions_SetCATrustedCertificates(o, opts.caFile.toUtf8().constData()));
    }
    if (!opts.certFile.isEmpty() && !opts.keyFile.isEmpty()) {
        checkError(
            natsOptions_LoadCertificatesChain(o, opts.certFile.toUtf8().constData(), opts.keyFile.toUtf8().constData())
        );
    }

    checkError(natsOptions_SetVerbose(o, opts.verbose));
    checkError(natsOptions_SetPedantic(o, opts.pedantic));
    checkError(natsOptions_SetPingInterval(o, opts.pingInterval));
    checkError(natsOptions_SetMaxPingsOut(o, opts.maxPingsOut));
    checkError(natsOptions_SetAllowReconnect(o, opts.allowReconnect));
    checkError(natsOptions_SetMaxReconnect(o, opts.maxReconnect));
    checkError(natsOptions_SetReconnectWait(o, opts.reconnectWait));
    checkError(natsOptions_SetReconnectBufSize(o, opts.reconnectBufferSize));
    checkError(natsOptions_SetMaxPendingMsgs(o, opts.maxPendingMessages));
    checkError(natsOptions_SetNoEcho(o, !opts.echo)); // NB! reverted flag

    return ptr;
}

Message fromC(NatsMsgPtr msg) {
    return Message(msg.release());
}

JsPublishAck fromC(JsPubAckPtr ack) {
    JsPublishAck result;
    result.stream = QByteArray(ack->Stream);
    result.domain = QByteArray(ack->Domain);
    result.sequence = ack->Sequence;
    result.duplicate = ack->Duplicate;
    return result;
}

jsOptions toC(const JsOptions& opts) {
    jsOptions o;
    jsOptions_Init(&o);
    o.Domain = opts.domain.constData();
    o.Wait = opts.timeout;
    return o;
}

jsPubOptions toC(const JsPublishOptions& opts) {
    jsPubOptions o;
    jsPubOptions_Init(&o);
    o.MaxWait = opts.timeout;
    if (opts.msgID.size())
        o.MsgId = opts.msgID.constData();
    if (opts.expectStream.size())
        o.ExpectStream = opts.expectStream.constData();
    if (opts.expectLastMessageID.size())
        o.ExpectLastMsgId = opts.expectLastMessageID.constData();
    o.ExpectLastSeq = opts.expectLastSequence;
    o.ExpectLastSubjectSeq = opts.expectLastSubjectSequence;
    o.ExpectNoMessage = opts.expectNoMessage;
    return o;
}

jsSubOptions toC(const QByteArray& stream, const QByteArray& consumer, bool manualAck) {
    jsSubOptions o;
    jsSubOptions_Init(&o);
    o.Stream = stream.constData();
    o.Consumer = consumer.constData();
    o.ManualAck = manualAck;
    return o;
}

} // namespace QtNats
