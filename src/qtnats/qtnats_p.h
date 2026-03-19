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

#pragma once

#include <type_traits>

#include "qtnats.h"

namespace QtNats {

void checkError(natsStatus s);
void subscriptionCallback(natsConnection* nc, natsSubscription* sub, natsMsg* msg, void* closure);

// Conversions between nats.c types and QtNats types.
// nats.c -> QtNats is generally straightforward: Any pointers get dereferenced, and copied into the QtNats types.
// QtNats -> nats.c is more complicated: We can't just populate the C structs with pointers to the data in the QtNats
// types, because the C structs may outlive the QtNats types (plus it makes transient conversions like UTF-16 to UTF-8,
// which are needed for QStrings, more difficult.) Instead, we pass the entire C-based handler code in as a function,
// which gets called after the conversion is complete.
// In addition, nats.c uses both opaque types and concrete structs, the former for persistent objects and the latter
// for immediately consumed objects. The former are always handled as pointers; the latter are handled by value when
// appropriate.

// Holds UTF-8 QByteArray conversions so that const char* pointers into them remain valid
// for the duration of a convertAndHandle call.
struct StringArena {
    QList<QByteArray> data;
    const char* add(const QString& s) {
        data.append(s.toUtf8());
        return data.last().constData();
    }
};

// We wrap raw pointers in unique_ptr with struct deleters to ensure proper cleanup
// and allow construction without passing the deleter explicitly.
struct JsPubAckDeleter {
    void operator()(jsPubAck* p) const { jsPubAck_Destroy(p); }
};
struct NatsMsgDeleter {
    void operator()(natsMsg* p) const { natsMsg_Destroy(p); }
};
struct NatsOptsDeleter {
    void operator()(natsOptions* p) const { natsOptions_Destroy(p); }
};
using JsPubAckPtr = std::unique_ptr<jsPubAck, JsPubAckDeleter>;
using NatsMsgPtr = std::unique_ptr<natsMsg, NatsMsgDeleter>;
using NatsOptsPtr = std::unique_ptr<natsOptions, NatsOptsDeleter>;

JsPublishAck fromC(const JsPubAckPtr& ack);
Message fromC(NatsMsgPtr msg);

template <typename F>
auto convertAndHandle(const Message& msg, const char* reply, F&& handler) -> std::invoke_result_t<F, NatsMsgPtr&> {
    StringArena a;
    natsMsg* cnatsMsg;

    const char* realReply = nullptr; // in asyncRequest I need to provide my own reply
    if (reply) {
        realReply = reply;
    } else if (msg.reply.size()) {
        realReply = msg.reply.constData();
    }

    checkError(
        natsMsg_Create(&cnatsMsg, a.add(msg.subject), realReply, msg.data.constData(), msg.data.size())
    );

    NatsMsgPtr msgPtr(cnatsMsg);

    auto i = msg.headers.constBegin();
    while (i != msg.headers.constEnd()) {
        checkError(natsMsgHeader_Add(cnatsMsg, a.add(i.key()), i.value().constData()));
        ++i;
    }

    return handler(msgPtr);
}

template <typename F>
auto convertAndHandle(const Options& opts, F&& handler) -> std::invoke_result_t<F, NatsOptsPtr&> {
    StringArena a;
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
    checkError(natsOptions_SetUserInfo(o, a.add(opts.user), a.add(opts.password)));
    checkError(natsOptions_SetToken(o, a.add(opts.token)));
    checkError(natsOptions_SetNoRandomize(o, !opts.randomize)); // NB! reverted flag
    checkError(natsOptions_SetTimeout(o, opts.timeout));
    checkError(natsOptions_SetName(o, a.add(opts.name)));

    // TLS/mTLS configuration
    if (opts.secure) {
        checkError(natsOptions_SetSecure(o, true));
    }
    if (!opts.caFile.isEmpty()) {
        checkError(natsOptions_SetCATrustedCertificates(o, a.add(opts.caFile)));
    }
    if (!opts.certFile.isEmpty() && !opts.keyFile.isEmpty()) {
        checkError(
            natsOptions_LoadCertificatesChain(o, a.add(opts.certFile), a.add(opts.keyFile))
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

    return handler(ptr);
}

template <typename F>
auto convertAndHandle(const JsConsumerConfig& c, F&& handler) -> std::invoke_result_t<F, jsConsumerConfig&> {
    // No jsConsumerConfig_Init() — default values are already set in JsConsumerConfig
    StringArena a;
    jsConsumerConfig o = {};
    o.Name = a.add(c.name);
    o.Durable = a.add(c.durable);
    o.Description = a.add(c.description);
    o.DeliverPolicy =
        c.deliverPolicy ? static_cast<jsDeliverPolicy>(*c.deliverPolicy) : static_cast<jsDeliverPolicy>(-1);
    o.OptStartSeq = c.optStartSeq;
    o.OptStartTime = c.optStartTime;
    o.AckPolicy = c.ackPolicy ? static_cast<jsAckPolicy>(*c.ackPolicy) : static_cast<jsAckPolicy>(-1);
    o.AckWait = c.ackWait;
    o.MaxDeliver = c.maxDeliver;
    o.BackOff = c.backOff.isEmpty() ? nullptr : const_cast<int64_t*>(c.backOff.constData());
    o.BackOffLen = static_cast<int>(c.backOff.size());
    o.FilterSubject = a.add(c.filterSubject);
    o.ReplayPolicy = c.replayPolicy ? static_cast<jsReplayPolicy>(*c.replayPolicy) : static_cast<jsReplayPolicy>(-1);
    o.RateLimit = c.rateLimit;
    o.SampleFrequency = a.add(c.sampleFrequency);
    o.MaxWaiting = c.maxWaiting;
    o.MaxAckPending = c.maxAckPending;
    o.FlowControl = c.flowControl;
    o.Heartbeat = c.heartbeat;
    o.HeadersOnly = c.headersOnly;
    o.MaxRequestBatch = c.maxRequestBatch;
    o.MaxRequestExpires = c.maxRequestExpires;
    o.MaxRequestMaxBytes = c.maxRequestMaxBytes;
    o.DeliverSubject = a.add(c.deliverSubject);
    o.DeliverGroup = a.add(c.deliverGroup);
    o.InactiveThreshold = c.inactiveThreshold;
    o.Replicas = c.replicas;
    o.MemoryStorage = c.memoryStorage;
    // FilterSubjects and Metadata require intermediate pointer arrays — TODO
    o.FilterSubjects = nullptr;
    o.FilterSubjectsLen = 0;
    o.Metadata = {nullptr, 0};
    o.PauseUntil = c.pauseUntil;
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsOptionsPublishAsync& opts, F&& handler)
    -> std::invoke_result_t<F, jsOptionsPublishAsync&> {
    // No jsOptionsPublishAsync_Init() — default values are already set in JsOptionsPublishAsync
    jsOptionsPublishAsync o = {};
    o.MaxPending = opts.maxPending;
    o.AckHandler = nullptr;
    o.AckHandlerClosure = nullptr;
    o.ErrHandler = nullptr;
    o.ErrHandlerClosure = nullptr;
    o.StallWait = opts.stallWait;
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsOptionsPullSubscribeAsync& opts, F&& handler)
    -> std::invoke_result_t<F, jsOptionsPullSubscribeAsync&> {
    // No jsOptionsPullSubscribeAsync_Init() — default values are already set in JsOptionsPullSubscribeAsync
    jsOptionsPullSubscribeAsync o = {};
    o.Timeout = opts.timeout;
    o.MaxMessages = opts.maxMessages;
    o.MaxBytes = opts.maxBytes;
    o.NoWait = opts.noWait;
    o.CompleteHandler = nullptr;
    o.CompleteHandlerClosure = nullptr;
    o.Heartbeat = opts.heartbeat;
    o.FetchSize = opts.fetchSize;
    o.KeepAhead = opts.keepAhead;
    o.NextHandler = nullptr;
    o.NextHandlerClosure = nullptr;
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsOptionsStreamInfo& opts, F&& handler) -> std::invoke_result_t<F, jsOptionsStreamInfo&> {
    // No jsOptionsStreamInfo_Init() — default values are already set in JsOptionsStreamInfo
    StringArena a;
    jsOptionsStreamInfo o = {};
    o.DeletedDetails = opts.deletedDetails;
    o.SubjectsFilter = a.add(opts.subjectsFilter);
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsOptionsStreamPurge& opts, F&& handler) -> std::invoke_result_t<F, jsOptionsStreamPurge&> {
    // No jsOptionsStreamPurge_Init() — default values are already set in JsOptionsStreamPurge
    StringArena a;
    jsOptionsStreamPurge o = {};
    o.Subject = a.add(opts.subject);
    o.Sequence = opts.sequence;
    o.Keep = opts.keep;
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsPublishOptions& opts, F&& handler) -> std::invoke_result_t<F, jsPubOptions&> {
    // No jsPubOptions_Init() — default values are already set in JsPublishOptions
    StringArena a;
    jsPubOptions o = {};
    o.MaxWait = opts.timeout;
    o.MsgId = a.add(opts.msgID);
    o.ExpectStream = a.add(opts.expectStream);
    o.ExpectLastMsgId = a.add(opts.expectLastMessageID);
    o.ExpectLastSeq = opts.expectLastSequence;
    o.ExpectLastSubjectSeq = opts.expectLastSubjectSequence;
    o.ExpectNoMessage = opts.expectNoMessage;
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsOptionsStream& opts, F&& handler) -> std::invoke_result_t<F, jsOptionsStream&> {
    // No jsOptionsStream_Init() — default values are already set in JsOptionsStream
    return convertAndHandle(opts.purge, [&](const jsOptionsStreamPurge& purge) {
        return convertAndHandle(opts.info, [&](const jsOptionsStreamInfo& info) {
            jsOptionsStream o = {};
            o.Purge = purge;
            o.Info = info;
            return handler(o);
        });
    });
}

template <typename F>
auto convertAndHandle(const JsOptions& opts, F&& handler) -> std::invoke_result_t<F, jsOptions&> {
    // No jsOptions_Init() — default values are already set in JsOptions
    return convertAndHandle(opts.publishAsync, [&](const jsOptionsPublishAsync& pa) {
        return convertAndHandle(opts.pullSubscribeAsync, [&](const jsOptionsPullSubscribeAsync& psa) {
            return convertAndHandle(opts.stream, [&](const jsOptionsStream& stream) {
                StringArena a;
                jsOptions o = {};
                o.Prefix = a.add(opts.prefix);
                o.Domain = a.add(opts.domain);
                o.Wait = opts.timeout;
                o.PublishAsync = pa;
                o.PullSubscribeAsync = psa;
                o.Stream = stream;
                return handler(o);
            });
        });
    });
}

template <typename F>
auto convertAndHandle(const JsSubOptions& opts, bool manualAck, F&& handler) -> std::invoke_result_t<F, jsSubOptions&> {
    // No jsSubOptions_Init() — default values are already set in JsSubOptions
    StringArena a;
    return convertAndHandle(opts.config, [&](const jsConsumerConfig& config) {
        jsSubOptions o = {};
        o.Stream = a.add(opts.stream);
        o.Consumer = a.add(opts.consumer);
        o.Queue = opts.queue.isEmpty() ? nullptr : a.add(opts.queue);
        o.ManualAck = manualAck;
        o.Config = config;
        o.Ordered = opts.ordered;
        return handler(o);
    });
}

} // namespace QtNats
