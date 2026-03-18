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

} // namespace QtNats
