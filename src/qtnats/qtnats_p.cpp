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

#include "qtnats/qtnats_p.h"
#include "qtnats/qtnats.h"

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

JsClusterInfo fromC(const jsClusterInfo& cluster) {
    JsClusterInfo result;
    result.name = toOptionalQString(cluster.Name);
    result.leader = toOptionalQString(cluster.Leader);
    result.replicas.reserve(cluster.ReplicasLen);
    for (int i = 0; i < cluster.ReplicasLen; i++)
        result.replicas.append(fromC(*cluster.Replicas[i]));
    return result;
}

JsConsumerConfig fromC(const jsConsumerConfig& cfg) {
    JsConsumerConfig result;
    result.name = toOptionalQString(cfg.Name);
    result.durable = toOptionalQString(cfg.Durable);
    result.description = toOptionalQString(cfg.Description);
    // Enums are set to -1 when not specified (see convertAndHandle(JsConsumerConfig))
    result.deliverPolicy = static_cast<int>(cfg.DeliverPolicy) == -1
        ? std::nullopt
        : std::optional(static_cast<JsDeliverPolicy>(cfg.DeliverPolicy));
    result.ackPolicy =
        static_cast<int>(cfg.AckPolicy) == -1 ? std::nullopt : std::optional(static_cast<JsAckPolicy>(cfg.AckPolicy));
    result.replayPolicy = static_cast<int>(cfg.ReplayPolicy) == -1
        ? std::nullopt
        : std::optional(static_cast<JsReplayPolicy>(cfg.ReplayPolicy));
    result.optStartSeq = cfg.OptStartSeq;
    result.optStartTime = toOptionalTimePoint(cfg.OptStartTime);
    result.ackWait = NatsDuration{cfg.AckWait};
    result.maxDeliver = cfg.MaxDeliver;
    result.backOff.reserve(cfg.BackOffLen);
    for (int i = 0; i < cfg.BackOffLen; ++i)
        result.backOff.append(NatsDuration{cfg.BackOff[i]});
    result.filterSubject = toOptionalQString(cfg.FilterSubject);
    result.rateLimit = cfg.RateLimit > 0 ? std::optional(cfg.RateLimit) : std::nullopt;
    result.sampleFrequency = toOptionalQString(cfg.SampleFrequency);
    result.maxWaiting = cfg.MaxWaiting;
    result.maxAckPending = cfg.MaxAckPending;
    result.flowControl = cfg.FlowControl;
    result.heartbeat = NatsDuration{cfg.Heartbeat};
    result.headersOnly = cfg.HeadersOnly;
    result.maxRequestBatch = cfg.MaxRequestBatch;
    result.maxRequestExpires = NatsDuration{cfg.MaxRequestExpires};
    result.maxRequestMaxBytes = cfg.MaxRequestMaxBytes;
    result.deliverSubject = toOptionalQString(cfg.DeliverSubject);
    result.deliverGroup = toOptionalQString(cfg.DeliverGroup);
    result.inactiveThreshold = NatsDuration{cfg.InactiveThreshold};
    result.replicas = cfg.Replicas;
    result.memoryStorage = cfg.MemoryStorage;
    result.filterSubjects.reserve(cfg.FilterSubjectsLen);
    for (int i = 0; i < cfg.FilterSubjectsLen; ++i)
        result.filterSubjects.append(QString::fromUtf8(cfg.FilterSubjects[i]));
    result.metadata = fromC(cfg.Metadata);
    result.pauseUntil = toOptionalTimePoint(cfg.PauseUntil);
    return result;
}

JsConsumerInfo fromC(const JsConsumerInfoPtr& info) {
    JsConsumerInfo result;
    result.stream = QString::fromUtf8(info->Stream);
    result.name = QString::fromUtf8(info->Name);
    result.created = toTimePoint(info->Created);
    result.config = fromC(*info->Config);
    result.delivered = fromC(info->Delivered);
    result.ackFloor = fromC(info->AckFloor);
    result.numAckPending = info->NumAckPending;
    result.numRedelivered = info->NumRedelivered;
    result.numWaiting = info->NumWaiting;
    result.numPending = info->NumPending;
    result.cluster = info->Cluster ? std::optional(fromC(*info->Cluster)) : std::nullopt;
    result.pushBound = info->PushBound;
    result.paused = info->Paused;
    result.pauseRemaining = NatsDuration{info->PauseRemaining};
    return result;
}

JsConsumerPauseResponse fromC(const JsConsumerPauseResponsePtr& resp) {
    JsConsumerPauseResponse result;
    result.paused = resp->Paused;
    result.pauseUntil = toOptionalTimePoint(resp->PauseUntil);
    result.pauseRemaining = NatsDuration{resp->PauseRemaining};
    return result;
}

JsExternalStream fromC(const jsExternalStream& ext) {
    JsExternalStream result;
    result.apiPrefix = QString::fromUtf8(ext.APIPrefix);
    result.deliverPrefix = toOptionalQString(ext.DeliverPrefix);
    return result;
}

JsLostStreamData fromC(const jsLostStreamData& lost) {
    JsLostStreamData result;
    result.msgs.reserve(lost.MsgsLen);
    for (int i = 0; i < lost.MsgsLen; i++)
        result.msgs.append(lost.Msgs[i]);
    result.bytes = lost.Bytes;
    return result;
}

JsPeerInfo fromC(const jsPeerInfo& peer) {
    JsPeerInfo result;
    result.name = QString::fromUtf8(peer.Name);
    result.current = peer.Current;
    result.offline = peer.Offline;
    result.active = NatsDuration{peer.Active};
    result.lag = peer.Lag;
    return result;
}

JsPlacement fromC(const jsPlacement& p) {
    JsPlacement result;
    result.cluster = QString::fromUtf8(p.Cluster);
    result.tags.reserve(p.TagsLen);
    for (int i = 0; i < p.TagsLen; i++)
        result.tags.append(QString::fromUtf8(p.Tags[i]));
    return result;
}

JsPublishAck fromC(const JsPubAckPtr& ack) {
    JsPublishAck result;
    result.stream = QString::fromUtf8(ack->Stream);
    result.sequence = ack->Sequence;
    result.domain = QString::fromUtf8(ack->Domain);
    result.duplicate = ack->Duplicate;
    return result;
}

JsRePublish fromC(const jsRePublish& rp) {
    JsRePublish result;
    result.source = QString::fromUtf8(rp.Source);
    result.destination = QString::fromUtf8(rp.Destination);
    result.headersOnly = rp.HeadersOnly;
    return result;
}

JsSequenceInfo fromC(const jsSequenceInfo& seq) {
    JsSequenceInfo result;
    result.consumer = seq.Consumer;
    result.stream = seq.Stream;
    result.last = toTimePoint(seq.Last);
    return result;
}

JsSequencePair fromC(const jsSequencePair& seq) {
    JsSequencePair result;
    result.consumer = seq.Consumer;
    result.stream = seq.Stream;
    return result;
}

JsStreamAlternate fromC(const jsStreamAlternate& alt) {
    JsStreamAlternate result;
    result.name = QString::fromUtf8(alt.Name);
    result.domain = QString::fromUtf8(alt.Domain);
    result.cluster = QString::fromUtf8(alt.Cluster);
    return result;
}

JsStreamConfig fromC(const jsStreamConfig& cfg) {
    JsStreamConfig result;
    result.name = QString::fromUtf8(cfg.Name);
    result.description = toOptionalQString(cfg.Description);
    result.subjects.reserve(cfg.SubjectsLen);
    for (int i = 0; i < cfg.SubjectsLen; i++)
        result.subjects.append(QString::fromUtf8(cfg.Subjects[i]));
    result.retention = static_cast<JsRetentionPolicy>(cfg.Retention);
    result.discard = static_cast<JsDiscardPolicy>(cfg.Discard);
    result.storage = static_cast<JsStorageType>(cfg.Storage);
    result.compression = static_cast<JsStorageCompression>(cfg.Compression);

    // -1 means unlimited for consumers/msgs/bytes/msgSize; 0 means unlimited for age/msgsPerSubject
    // We use <= for robustness, in case there's an unexpected out-of-range value.
    result.maxConsumers =
        cfg.MaxConsumers <= -1 ? std::nullopt : std::optional(static_cast<uint64_t>(cfg.MaxConsumers));
    result.maxMsgs = cfg.MaxMsgs <= -1 ? std::nullopt : std::optional(static_cast<uint64_t>(cfg.MaxMsgs));
    result.maxBytes = cfg.MaxBytes <= -1 ? std::nullopt : std::optional(static_cast<uint64_t>(cfg.MaxBytes));
    result.maxAge = cfg.MaxAge <= 0 ? std::nullopt : std::optional(NatsDuration{cfg.MaxAge});
    result.maxMsgsPerSubject =
        cfg.MaxMsgsPerSubject <= 0 ? std::nullopt : std::optional(static_cast<uint64_t>(cfg.MaxMsgsPerSubject));
    result.maxMsgSize = cfg.MaxMsgSize <= -1 ? std::nullopt : std::optional(static_cast<uint32_t>(cfg.MaxMsgSize));

    result.replicas = cfg.Replicas;
    result.noAck = cfg.NoAck;
    result.duplicates = NatsDuration{cfg.Duplicates};
    result.templateOwner = toOptionalQString(cfg.Template);
    result.placement = cfg.Placement ? std::optional(fromC(*cfg.Placement)) : std::nullopt;
    result.mirror = cfg.Mirror ? std::optional(fromC(*cfg.Mirror)) : std::nullopt;
    result.sources.reserve(cfg.SourcesLen);
    for (int i = 0; i < cfg.SourcesLen; i++)
        result.sources.append(fromC(*cfg.Sources[i]));
    result.sealed = cfg.Sealed;
    result.denyDelete = cfg.DenyDelete;
    result.denyPurge = cfg.DenyPurge;
    result.allowRollup = cfg.AllowRollup;
    result.allowDirect = cfg.AllowDirect;
    result.mirrorDirect = cfg.MirrorDirect;
    result.discardNewPerSubject = cfg.DiscardNewPerSubject;
    result.rePublish = cfg.RePublish ? std::optional(fromC(*cfg.RePublish)) : std::nullopt;
    result.subjectTransform = fromC(cfg.SubjectTransform);
    result.consumerLimits = fromC(cfg.ConsumerLimits);
    result.firstSeq = cfg.FirstSeq;
    result.metadata = fromC(cfg.Metadata);
    return result;
}

JsStreamConsumerLimits fromC(const jsStreamConsumerLimits& lim) {
    JsStreamConsumerLimits result;
    result.inactiveThreshold = NatsDuration{lim.InactiveThreshold};
    result.maxAckPending = lim.MaxAckPending;
    return result;
}

JsStreamInfo fromC(const JsStreamInfoPtr& info) {
    JsStreamInfo result;
    result.config = fromC(*info->Config);
    result.created = toTimePoint(info->Created);
    result.state = fromC(info->State);
    result.cluster = info->Cluster ? std::optional(fromC(*info->Cluster)) : std::nullopt;
    result.mirror = info->Mirror ? std::optional(fromC(*info->Mirror)) : std::nullopt;
    result.sources.reserve(info->SourcesLen);
    for (int i = 0; i < info->SourcesLen; i++)
        result.sources.append(fromC(*info->Sources[i]));
    result.alternates.reserve(info->AlternatesLen);
    for (int i = 0; i < info->AlternatesLen; i++)
        result.alternates.append(fromC(*info->Alternates[i]));
    return result;
}

JsStreamSource fromC(const jsStreamSource& src) {
    JsStreamSource result;
    result.name = QString::fromUtf8(src.Name);
    result.optStartSeq = src.OptStartSeq;
    result.optStartTime = toOptionalTimePoint(src.OptStartTime);
    result.filterSubject = toOptionalQString(src.FilterSubject);
    result.external = src.External ? std::optional(fromC(*src.External)) : std::nullopt;
    result.domain = toOptionalQString(src.Domain);
    return result;
}

JsStreamSourceInfo fromC(const jsStreamSourceInfo& src) {
    JsStreamSourceInfo result;
    result.name = QString::fromUtf8(src.Name);
    result.external = src.External ? std::optional(fromC(*src.External)) : std::nullopt;
    result.lag = src.Lag;
    result.active = NatsDuration{src.Active};
    result.filterSubject = toOptionalQString(src.FilterSubject);
    result.subjectTransforms.reserve(src.SubjectTransformsLen);
    for (int i = 0; i < src.SubjectTransformsLen; i++)
        result.subjectTransforms.append(fromC(src.SubjectTransforms[i]));
    return result;
}

JsStreamState fromC(const jsStreamState& state) {
    JsStreamState result;
    result.msgs = state.Msgs;
    result.bytes = state.Bytes;
    result.firstSeq = state.FirstSeq;
    result.firstTime = toTimePoint(state.FirstTime);
    result.lastSeq = state.LastSeq;
    result.lastTime = toTimePoint(state.LastTime);
    result.numSubjects = state.NumSubjects;
    result.subjects = state.Subjects ? std::optional(fromC(*state.Subjects)) : std::nullopt;
    result.numDeleted = state.NumDeleted;
    result.deleted.reserve(state.DeletedLen);
    for (int i = 0; i < state.DeletedLen; i++)
        result.deleted.append(state.Deleted[i]);
    result.lost = state.Lost ? std::optional(fromC(*state.Lost)) : std::nullopt;
    result.consumers = state.Consumers;
    return result;
}

JsStreamStateSubject fromC(const jsStreamStateSubject& subj) {
    JsStreamStateSubject result;
    result.subject = QString::fromUtf8(subj.Subject);
    result.msgs = subj.Msgs;
    return result;
}

QList<JsStreamStateSubject> fromC(const jsStreamStateSubjects& subjs) {
    QList<JsStreamStateSubject> result;
    result.reserve(subjs.Count);
    for (int i = 0; i < subjs.Count; i++)
        result.append(fromC(subjs.List[i]));
    return result;
}

JsSubjectTransformConfig fromC(const jsSubjectTransformConfig& st) {
    JsSubjectTransformConfig result;
    result.source = toOptionalQString(st.Source);
    result.destination = QString::fromUtf8(st.Destination);
    return result;
}

Message fromC(NatsMsgPtr msg) { return Message(msg.release()); }

NatsMetadata fromC(const natsMetadata& meta) {
    NatsMetadata result;
    // natsMetadata.List is [k, v, k, v, ...]; Count is the number of k/v pairs
    for (int i = 0; i < meta.Count; i++)
        result.insert(QString::fromUtf8(meta.List[i * 2]), QByteArray(meta.List[i * 2 + 1]));
    return result;
}

} // namespace QtNats