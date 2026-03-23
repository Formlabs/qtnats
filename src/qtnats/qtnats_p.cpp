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

Message fromC(NatsMsgPtr msg) { return Message(msg.release()); }

JsPublishAck fromC(const JsPubAckPtr& ack) {
    JsPublishAck result;
    result.stream = QString::fromUtf8(ack->Stream);
    result.sequence = ack->Sequence;
    result.domain = QString::fromUtf8(ack->Domain);
    result.duplicate = ack->Duplicate;
    return result;
}

static JsStreamConfig streamConfigFromC(const jsStreamConfig* cfg) {
    JsStreamConfig result;
    result.name = QString::fromUtf8(cfg->Name);
    if (cfg->Description) {
        result.description = QString::fromUtf8(cfg->Description);
    }
    for (int i = 0; i < cfg->SubjectsLen; i++) {
        result.subjects.append(QString::fromUtf8(cfg->Subjects[i]));
    }
    result.retention = static_cast<JsRetentionPolicy>(cfg->Retention);
    result.storage = static_cast<JsStorageType>(cfg->Storage);
    result.discard = static_cast<JsDiscardPolicy>(cfg->Discard);
    result.maxConsumers = cfg->MaxConsumers;
    result.maxMsgs = cfg->MaxMsgs;
    result.maxBytes = cfg->MaxBytes;
    result.maxAge = cfg->MaxAge;
    result.maxMsgSize = cfg->MaxMsgSize;
    result.numReplicas = cfg->Replicas;
    result.duplicateWindow = cfg->Duplicates;
    result.maxMsgsPerSubject = cfg->MaxMsgsPerSubject;
    result.noAck = cfg->NoAck;
    return result;
}

JsStreamInfo fromC(const JsStreamInfoPtr& info) {
    JsStreamInfo result;
    result.config = streamConfigFromC(info->Config);
    result.state.messages = info->State.Msgs;
    result.state.bytes = info->State.Bytes;
    result.state.firstSeq = info->State.FirstSeq;
    result.state.lastSeq = info->State.LastSeq;
    result.state.numSubjects = info->State.NumSubjects;
    result.state.numDeleted = info->State.NumDeleted;
    return result;
}

} // namespace QtNats
