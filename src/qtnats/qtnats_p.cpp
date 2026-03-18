/* Copyright(c) 2021-2022 Petro Kazmirchuk https://github.com/Kazmirchuk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License.You may obtain a copy of the License at http ://www.apache.org/licenses/LICENSE-2.0 Unless required by
applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the License for the specific language
governing permissions and  limitations under the License.
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

Message fromC(NatsMsgPtr msg) { return Message(msg.release()); }

JsPublishAck fromC(const JsPubAckPtr& ack) {
    JsPublishAck result;
    result.stream = QByteArray(ack->Stream);
    result.sequence = ack->Sequence;
    result.domain = QByteArray(ack->Domain);
    result.duplicate = ack->Duplicate;
    return result;
}

jsConsumerConfig toC(const JsConsumerConfig& c) {
    // No jsConsumerConfig_Init() — default values are already set in JsConsumerConfig
    jsConsumerConfig o = {};
    o.Name = c.name.isEmpty() ? nullptr : c.name.constData();
    o.Durable = c.durable.isEmpty() ? nullptr : c.durable.constData();
    o.Description = c.description.isEmpty() ? nullptr : c.description.constData();
    o.DeliverPolicy =
        c.deliverPolicy ? static_cast<jsDeliverPolicy>(*c.deliverPolicy) : static_cast<jsDeliverPolicy>(-1);
    o.OptStartSeq = c.optStartSeq;
    o.OptStartTime = c.optStartTime;
    o.AckPolicy = c.ackPolicy ? static_cast<jsAckPolicy>(*c.ackPolicy) : static_cast<jsAckPolicy>(-1);
    o.AckWait = c.ackWait;
    o.MaxDeliver = c.maxDeliver;
    o.BackOff = c.backOff.isEmpty() ? nullptr : const_cast<int64_t*>(c.backOff.constData());
    o.BackOffLen = static_cast<int>(c.backOff.size());
    o.FilterSubject = c.filterSubject.isEmpty() ? nullptr : c.filterSubject.constData();
    o.ReplayPolicy = c.replayPolicy ? static_cast<jsReplayPolicy>(*c.replayPolicy) : static_cast<jsReplayPolicy>(-1);
    o.RateLimit = c.rateLimit;
    o.SampleFrequency = c.sampleFrequency.isEmpty() ? nullptr : c.sampleFrequency.constData();
    o.MaxWaiting = c.maxWaiting;
    o.MaxAckPending = c.maxAckPending;
    o.FlowControl = c.flowControl;
    o.Heartbeat = c.heartbeat;
    o.HeadersOnly = c.headersOnly;
    o.MaxRequestBatch = c.maxRequestBatch;
    o.MaxRequestExpires = c.maxRequestExpires;
    o.MaxRequestMaxBytes = c.maxRequestMaxBytes;
    o.DeliverSubject = c.deliverSubject.isEmpty() ? nullptr : c.deliverSubject.constData();
    o.DeliverGroup = c.deliverGroup.isEmpty() ? nullptr : c.deliverGroup.constData();
    o.InactiveThreshold = c.inactiveThreshold;
    o.Replicas = c.replicas;
    o.MemoryStorage = c.memoryStorage;
    // FilterSubjects and Metadata require intermediate pointer arrays — TODO
    o.FilterSubjects = nullptr;
    o.FilterSubjectsLen = 0;
    o.Metadata = {nullptr, 0};
    o.PauseUntil = c.pauseUntil;
    return o;
}

jsOptions toC(const JsOptions& opts) {
    // No jsOptions_Init() — default values are already set in JsOptions
    jsOptions o = {};
    o.Prefix = opts.prefix.constData();
    o.Domain = opts.domain.constData();
    o.Wait = opts.timeout;
    o.PublishAsync = toC(opts.publishAsync);
    o.PullSubscribeAsync = toC(opts.pullSubscribeAsync);
    o.Stream = toC(opts.stream);
    return o;
}

jsOptionsPublishAsync toC(const JsOptionsPublishAsync& opts) {
    // No jsOptionsPublishAsync_Init() — default values are already set in JsOptionsPublishAsync
    jsOptionsPublishAsync o = {};
    o.MaxPending = opts.maxPending;
    o.AckHandler = nullptr;
    o.AckHandlerClosure = nullptr;
    o.ErrHandler = nullptr;
    o.ErrHandlerClosure = nullptr;
    o.StallWait = opts.stallWait;
    return o;
}

jsOptionsPullSubscribeAsync toC(const JsOptionsPullSubscribeAsync& opts) {
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
    return o;
}

jsOptionsStream toC(const JsOptionsStream& opts) {
    // No jsOptionsStream_Init() — default values are already set in JsOptionsStream
    jsOptionsStream o = {};
    o.Purge = toC(opts.purge);
    o.Info = toC(opts.info);
    return o;
}

jsOptionsStreamInfo toC(const JsOptionsStreamInfo& opts) {
    // No jsOptionsStreamInfo_Init() — default values are already set in JsOptionsStreamInfo
    jsOptionsStreamInfo o = {};
    o.DeletedDetails = opts.deletedDetails;
    o.SubjectsFilter = opts.subjectsFilter.constData();
    return o;
}

jsOptionsStreamPurge toC(const JsOptionsStreamPurge& opts) {
    // No jsOptionsStreamPurge_Init() — default values are already set in JsOptionsStreamPurge
    jsOptionsStreamPurge o = {};
    o.Subject = opts.subject.constData();
    o.Sequence = opts.sequence;
    o.Keep = opts.keep;
    return o;
}

jsPubOptions toC(const JsPublishOptions& opts) {
    // No jsPubOptions_Init() — default values are already set in JsPublishOptions
    jsPubOptions o = {};
    o.MaxWait = opts.timeout;
    o.MsgId = opts.msgID.isEmpty() ? nullptr : opts.msgID.constData();
    o.ExpectStream = opts.expectStream.isEmpty() ? nullptr : opts.expectStream.constData();
    o.ExpectLastMsgId = opts.expectLastMessageID.isEmpty() ? nullptr : opts.expectLastMessageID.constData();
    o.ExpectLastSeq = opts.expectLastSequence;
    o.ExpectLastSubjectSeq = opts.expectLastSubjectSequence;
    o.ExpectNoMessage = opts.expectNoMessage;
    return o;
}

jsSubOptions toC(const JsSubOptions& opts, bool manualAck) {
    // No jsSubOptions_Init() — default values are already set in JsSubOptions
    jsSubOptions o = {};
    o.Stream = opts.stream.constData();
    o.Consumer = opts.consumer.constData();
    o.Queue = opts.queue.isEmpty() ? nullptr : opts.queue.constData();
    o.ManualAck = manualAck;
    o.Config = toC(opts.config);
    o.Ordered = opts.ordered;
    return o;
}

} // namespace QtNats
