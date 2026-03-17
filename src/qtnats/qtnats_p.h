/* Copyright(c) 2022 Petro Kazmirchuk https://github.com/Kazmirchuk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at http ://www.apache.org/licenses/LICENSE-2.0 Unless required by
applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the License for the specific language
governing permissions and  limitations under the License.
*/

#pragma once

#include "qtnats.h"

namespace QtNats {

void checkError(natsStatus s);
void subscriptionCallback(natsConnection* nc, natsSubscription* sub, natsMsg* msg, void* closure);

// Conversions between nats.c types and QtNats types.
// nats.c uses both opaque types and concrete structs, the former for persistent objects and the latter for immediately
// consumed objects. The former are always handled as pointers; the latter are handled by value when appropriate.

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

NatsMsgPtr asC(const Message& msg, const char* reply = nullptr);
NatsOptsPtr asC(const Options& opts);

JsPublishAck fromC(JsPubAckPtr ack);
Message fromC(NatsMsgPtr msg);

// Note that these are NON-OWNING: Because the C types contain pointers to the data, the QtNats types must outlive the
// C types. Otherwise, the C types will have dangling pointers.
jsOptions toC(const JsOptions& opts);
jsPubOptions toC(const JsPublishOptions& opts);
jsSubOptions toC(const QByteArray& stream, const QByteArray& consumer, bool manualAck = false);

} // namespace QtNats
