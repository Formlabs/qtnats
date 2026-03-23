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

#include <list>
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

// Owns heap-allocated objects of type T with stable addresses.
// Use instead of QList when elements are added incrementally and pointers into the container must remain valid.
// std::list is used (rather than a Qt container) because QLinkedList was removed in Qt6 and QList can reallocate.
// emplace() zero-initializes a new T in place; add() moves an existing value in.
template <typename T>
struct Arena {
    std::list<T> objects;

    T* emplace() {
        objects.push_back({});
        return &objects.back();
    }
    T* add(T&& val) {
        objects.push_back(std::move(val));
        return &objects.back();
    }
};

// Holds UTF-8 QByteArray conversions so that const char* pointers into them remain valid
// for the duration of a convertAndHandle call. Also owns const char* pointer arrays so that
// const char** pointers into them are equally stable.
struct StringArena {
    Arena<QByteArray> utf8;
    Arena<QList<const char*>> utf8Arrays;

    const char* add(const QString& s) { return utf8.add(s.toUtf8())->constData(); }
    const char* add(const QByteArray& b) { return utf8.add(QByteArray(b))->constData(); }
    const char* add(const std::optional<QString>& s) {
        if (!s.has_value())
            return nullptr;
        return add(*s);
    }
    const char** add(const QList<QString>& strings) {
        QList<const char*> ptrs;
        ptrs.reserve(strings.size());
        for (const auto& s : strings)
            ptrs.append(add(s));
        auto* stored = utf8Arrays.add(std::move(ptrs));
        return stored->isEmpty() ? nullptr : stored->data();
    }
};

// Continuation-passing helper for optional pointer fields.
// If opt is empty, calls cont() immediately. If opt has a value, converts it via convertAndHandle, calls setter() to
// attach the pointer to the parent struct, then calls cont() — all while the converted object is still on the stack.
template <typename T, typename Setter, typename Cont>
auto withOptional(const std::optional<T>& opt, Setter&& setter, Cont&& cont) {
    if (!opt.has_value())
        return cont();
    return convertAndHandle(*opt, [&](auto& converted) {
        setter(converted);
        return cont();
    });
}

// General helpers for conversions
inline std::optional<QString> toOptionalQString(const char* s) {
    return s ? std::optional(QString::fromUtf8(s)) : std::nullopt;
}
inline NatsTimePoint toTimePoint(const int64_t ns) { return NatsTimePoint{NatsDuration{ns}}; }
inline std::optional<NatsTimePoint> toOptionalTimePoint(const int64_t ns) {
    return ns > 0 ? std::optional(NatsTimePoint{NatsDuration{ns}}) : std::nullopt;
};

// We wrap raw pointers in unique_ptr with struct deleters to ensure proper cleanup
// and allow construction without passing the deleter explicitly.
struct JsPubAckDeleter {
    void operator()(jsPubAck* p) const { jsPubAck_Destroy(p); }
};
struct JsStreamInfoDeleter {
    void operator()(jsStreamInfo* p) const { jsStreamInfo_Destroy(p); }
};
struct JsConsumerInfoDeleter {
    void operator()(jsConsumerInfo* p) const { jsConsumerInfo_Destroy(p); }
};
struct JsConsumerPauseResponseDeleter {
    void operator()(jsConsumerPauseResponse* p) const { jsConsumerPauseResponse_Destroy(p); }
};
struct NatsMsgDeleter {
    void operator()(natsMsg* p) const { natsMsg_Destroy(p); }
};
struct NatsOptsDeleter {
    void operator()(natsOptions* p) const { natsOptions_Destroy(p); }
};
using JsPubAckPtr = std::unique_ptr<jsPubAck, JsPubAckDeleter>;
using JsStreamInfoPtr = std::unique_ptr<jsStreamInfo, JsStreamInfoDeleter>;
using JsConsumerInfoPtr = std::unique_ptr<jsConsumerInfo, JsConsumerInfoDeleter>;
using JsConsumerPauseResponsePtr = std::unique_ptr<jsConsumerPauseResponse, JsConsumerPauseResponseDeleter>;
using NatsMsgPtr = std::unique_ptr<natsMsg, NatsMsgDeleter>;
using NatsOptsPtr = std::unique_ptr<natsOptions, NatsOptsDeleter>;

JsPublishAck fromC(const JsPubAckPtr& ack);
Message fromC(NatsMsgPtr msg);

NatsMetadata fromC(const natsMetadata& meta);
JsExternalStream fromC(const jsExternalStream& ext);
JsSubjectTransformConfig fromC(const jsSubjectTransformConfig& st);
JsStreamConsumerLimits fromC(const jsStreamConsumerLimits& lim);
JsStreamSource fromC(const jsStreamSource& src);
JsPlacement fromC(const jsPlacement& p);
JsRePublish fromC(const jsRePublish& rp);
JsStreamConfig fromC(const jsStreamConfig& cfg);
JsLostStreamData fromC(const jsLostStreamData& lost);
JsStreamStateSubject fromC(const jsStreamStateSubject& subj);
QList<JsStreamStateSubject> fromC(const jsStreamStateSubjects& subjs);
JsStreamState fromC(const jsStreamState& state);
JsPeerInfo fromC(const jsPeerInfo& peer);
JsClusterInfo fromC(const jsClusterInfo& cluster);
JsStreamSourceInfo fromC(const jsStreamSourceInfo& src);
JsStreamAlternate fromC(const jsStreamAlternate& alt);
JsStreamInfo fromC(const JsStreamInfoPtr& info);
JsConsumerPauseResponse fromC(const JsConsumerPauseResponsePtr& resp);
JsConsumerConfig fromC(const jsConsumerConfig& cfg);
JsSequencePair fromC(const jsSequencePair& seq);
JsSequenceInfo fromC(const jsSequenceInfo& seq);
JsConsumerInfo fromC(const JsConsumerInfoPtr& info);

// Probe functor used by convertAndHandleAll to deduce CType without a runtime call.
// A lambda would be simpler ([](auto& c) { return &c; }) but lambdas in unevaluated
// contexts (decltype) require C++20; this named functor works in C++17.
struct ConvertProbe {
    template <typename C>
    C* operator()(C& c) const { return &c; }
};

// Converts a QList of T types to a QList of C pointers and passes it to handler.
//
// Internally, this uses a Y-combinator (self(self, i)) to enable a lambda to recurse without needing a named free
// function. Each recursive call nests inside the previous convertAndHandle callback, keeping every converted
// C object on the stack simultaneously until handler() is finally invoked at the base case.
template <typename T, typename F>
auto convertAndHandleAll(const QList<T>& items, F&& handler) {
    // The C type is deduced by probing convertAndHandle's return type via decltype/std::declval.
    // This handles the empty-list case without requiring an explicit template argument.
    // Admittedly this is hard to read, but it does work even on C++17.
    using CType =
        std::remove_pointer_t<decltype(convertAndHandle(std::declval<const T&>(), std::declval<ConvertProbe>()))>;
    QList<CType*> ptrs;
    ptrs.reserve(items.size());
    // recurse captures itself as a parameter so it can call itself, sidestepping the usual restriction that a
    // lambda cannot refer to its own name inside its body.
    auto recurse = [&](auto& self, int i) -> decltype(handler(ptrs)) {
        if (i == items.size()) {
            return handler(ptrs);
        }
        return convertAndHandle(items[i], [&](CType& c) {
            ptrs.append(&c);
            return self(self, i + 1);
        });
    };
    return recurse(recurse, 0);
}

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
    checkError(natsOptions_Create(&o));
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
    checkError(natsOptions_SetTimeout(o, opts.timeout.count()));
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
auto convertAndHandle(const NatsMetadata& metadata, F&& handler) -> std::invoke_result_t<F, natsMetadata&> {
    // natsMetadata.List is [k, v, k, v, ...]; Count is the number of k/v pairs (not the list length)
    // I blame nats.c for this.
    StringArena a;
    QList<const char*> ptrs;
    ptrs.reserve(metadata.size() * 2);
    for (auto it = metadata.constBegin(); it != metadata.constEnd(); ++it) {
        ptrs.append(a.add(it.key()));
        ptrs.append(a.add(it.value()));
    }
    natsMetadata o = {ptrs.isEmpty() ? nullptr : ptrs.data(), static_cast<int>(metadata.size())};
    return handler(o);
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
    o.OptStartTime = c.optStartTime ? c.optStartTime->time_since_epoch().count() : 0;
    o.AckPolicy = c.ackPolicy ? static_cast<jsAckPolicy>(*c.ackPolicy) : static_cast<jsAckPolicy>(-1);
    o.AckWait = c.ackWait.count();
    o.MaxDeliver = c.maxDeliver;
    QList<int64_t> backOffNs;
    backOffNs.reserve(c.backOff.size());
    for (const auto& d : c.backOff)
        backOffNs.append(d.count());
    o.BackOff = backOffNs.isEmpty() ? nullptr : backOffNs.data();
    o.BackOffLen = static_cast<int>(backOffNs.size());
    o.FilterSubject = a.add(c.filterSubject);
    o.ReplayPolicy = c.replayPolicy ? static_cast<jsReplayPolicy>(*c.replayPolicy) : static_cast<jsReplayPolicy>(-1);
    o.RateLimit = c.rateLimit.value_or(0);
    o.SampleFrequency = a.add(c.sampleFrequency);
    o.MaxWaiting = c.maxWaiting;
    o.MaxAckPending = c.maxAckPending;
    o.FlowControl = c.flowControl;
    o.Heartbeat = c.heartbeat.count();
    o.HeadersOnly = c.headersOnly;
    o.MaxRequestBatch = c.maxRequestBatch;
    o.MaxRequestExpires = c.maxRequestExpires.count();
    o.MaxRequestMaxBytes = c.maxRequestMaxBytes;
    o.DeliverSubject = a.add(c.deliverSubject);
    o.DeliverGroup = a.add(c.deliverGroup);
    o.InactiveThreshold = c.inactiveThreshold.count();
    o.Replicas = c.replicas;
    o.MemoryStorage = c.memoryStorage;
    o.FilterSubjects = a.add(c.filterSubjects);
    o.FilterSubjectsLen = static_cast<int>(c.filterSubjects.size());
    o.PauseUntil = c.pauseUntil ? c.pauseUntil->time_since_epoch().count() : 0;
    return convertAndHandle(c.metadata, [&](const natsMetadata& meta) {
        o.Metadata = meta;
        return handler(o);
    });
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
    o.StallWait = opts.stallWait.count();
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsOptionsPullSubscribeAsync& opts, F&& handler)
    -> std::invoke_result_t<F, jsOptionsPullSubscribeAsync&> {
    // No jsOptionsPullSubscribeAsync_Init() — default values are already set in JsOptionsPullSubscribeAsync
    jsOptionsPullSubscribeAsync o = {};
    o.Timeout = opts.timeout.count();
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
    o.MaxWait = opts.timeout.has_value() ? opts.timeout->count() : 0;
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
                o.Wait = opts.timeout.count();
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
        o.Queue = a.add(opts.queue);
        o.ManualAck = manualAck;
        o.Config = config;
        o.Ordered = opts.ordered;
        return handler(o);
    });
}

template <typename F>
auto convertAndHandle(const JsExternalStream& ext, F&& handler) -> std::invoke_result_t<F, jsExternalStream&> {
    StringArena a;
    jsExternalStream o = {};
    o.APIPrefix = a.add(ext.apiPrefix);
    o.DeliverPrefix = a.add(ext.deliverPrefix);
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsStreamSource& src, F&& handler) -> std::invoke_result_t<F, jsStreamSource&> {
    StringArena a;
    jsStreamSource o = {};
    o.Name = a.add(src.name);
    o.OptStartSeq = src.optStartSeq;
    o.OptStartTime = src.optStartTime ? src.optStartTime->time_since_epoch().count() : 0;
    o.FilterSubject = a.add(src.filterSubject);
    o.Domain = a.add(src.domain);
    return withOptional(src.external, [&](jsExternalStream& ext) { o.External = &ext; }, [&] { return handler(o); });
}

template <typename F>
auto convertAndHandle(const JsPlacement& p, F&& handler) -> std::invoke_result_t<F, jsPlacement&> {
    StringArena a;
    jsPlacement o = {};
    o.Cluster = a.add(p.cluster);
    o.Tags = a.add(p.tags);
    o.TagsLen = static_cast<int>(p.tags.size());
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsRePublish& rp, F&& handler) -> std::invoke_result_t<F, jsRePublish&> {
    StringArena a;
    jsRePublish o = {};
    o.Source = a.add(rp.source);
    o.Destination = a.add(rp.destination);
    o.HeadersOnly = rp.headersOnly;
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsSubjectTransformConfig& cfg, F&& handler)
    -> std::invoke_result_t<F, jsSubjectTransformConfig&> {
    StringArena a;
    jsSubjectTransformConfig o = {};
    o.Source = a.add(cfg.source);
    o.Destination = a.add(cfg.destination);
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsStreamConsumerLimits& lim, F&& handler)
    -> std::invoke_result_t<F, jsStreamConsumerLimits&> {
    jsStreamConsumerLimits o = {};
    o.InactiveThreshold = lim.inactiveThreshold.count();
    o.MaxAckPending = lim.maxAckPending;
    return handler(o);
}

template <typename F>
auto convertAndHandle(const JsStreamConfig& cfg, F&& handler) -> std::invoke_result_t<F, jsStreamConfig&> {
    // No jsStreamConfig_Init() — defaults below replicate what Init() sets, with nullopt mapping to the Init() value.
    StringArena a;
    jsStreamConfig o = {};

    o.Name = a.add(cfg.name);
    o.Description = a.add(cfg.description);
    o.Subjects = a.add(cfg.subjects);
    o.SubjectsLen = static_cast<int>(cfg.subjects.size());
    o.Retention = static_cast<jsRetentionPolicy>(cfg.retention);

    // nullopt → Init() default (-1 = unlimited); has_value → explicit limit
    o.MaxConsumers = cfg.maxConsumers.has_value() ? static_cast<int64_t>(*cfg.maxConsumers) : -1;
    o.MaxMsgs = cfg.maxMsgs.has_value() ? static_cast<int64_t>(*cfg.maxMsgs) : -1;
    o.MaxBytes = cfg.maxBytes.has_value() ? static_cast<int64_t>(*cfg.maxBytes) : -1;
    o.MaxAge = cfg.maxAge.has_value() ? cfg.maxAge->count() : 0;
    o.MaxMsgsPerSubject = cfg.maxMsgsPerSubject.has_value() ? static_cast<int64_t>(*cfg.maxMsgsPerSubject) : 0;
    o.MaxMsgSize = cfg.maxMsgSize.has_value() ? static_cast<int32_t>(*cfg.maxMsgSize) : -1;

    o.Discard = static_cast<jsDiscardPolicy>(cfg.discard);
    o.Storage = static_cast<jsStorageType>(cfg.storage);
    o.Replicas = cfg.replicas;
    o.NoAck = cfg.noAck;
    o.Template = a.add(cfg.templateOwner);
    o.Duplicates = cfg.duplicates.count();
    o.Sealed = cfg.sealed;
    o.DenyDelete = cfg.denyDelete;
    o.DenyPurge = cfg.denyPurge;
    o.AllowRollup = cfg.allowRollup;
    o.AllowDirect = cfg.allowDirect;
    o.MirrorDirect = cfg.mirrorDirect;
    o.DiscardNewPerSubject = cfg.discardNewPerSubject;
    o.Compression = static_cast<jsStorageCompression>(cfg.compression);
    o.FirstSeq = cfg.firstSeq;

    // The remaining fields require nested conversions, so we build up a chain of continuations that each convert one
    // field and then call the next, with the final continuation calling handler(o). The exact order is arbitrary.
    auto withConsumerLimits = [&] {
        return convertAndHandle(cfg.consumerLimits, [&](const jsStreamConsumerLimits& cl) {
            o.ConsumerLimits = cl;
            return handler(o);
        });
    };
    auto withSubjectTransform = [&] {
        return convertAndHandle(cfg.subjectTransform, [&](const jsSubjectTransformConfig& st) {
            o.SubjectTransform = st;
            return withConsumerLimits();
        });
    };
    auto withRePublish = [&] {
        return withOptional(cfg.rePublish, [&](jsRePublish& rp) { o.RePublish = &rp; }, withSubjectTransform);
    };
    auto withMirror = [&] {
        return withOptional(cfg.mirror, [&](jsStreamSource& m) { o.Mirror = &m; }, withRePublish);
    };
    auto withPlacement = [&] {
        return withOptional(cfg.placement, [&](jsPlacement& p) { o.Placement = &p; }, withMirror);
    };
    auto withMetadata = [&] {
        return convertAndHandle(cfg.metadata, [&](const natsMetadata& meta) {
            o.Metadata = meta;
            return withPlacement();
        });
    };
    auto withSources = [&] {
        return convertAndHandleAll(cfg.sources, [&](QList<jsStreamSource*>& sources) {
            o.Sources = sources.isEmpty() ? nullptr : sources.data();
            o.SourcesLen = static_cast<int>(sources.size());
            return withMetadata();
        });
    };

    return withSources();
}

} // namespace QtNats
