/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "consumer_ack.h"

#include <cstdlib>
#include <exception>

#include <napi.h>

#include "addon_data.h"
#include "common_utils.h"

namespace __node_rocketmq__ {

Napi::Object ConsumerAck::Init(Napi::Env env, Napi::Object exports, AddonData* addon_data) {
  Napi::Function func = DefineClass(
      env, "ConsumerAck", {InstanceMethod<&ConsumerAck::Done>("done")});

  addon_data->consumer_ack_constructor = Napi::Persistent(func);

  exports.Set("ConsumerAck", func);
  return exports;
}

Napi::Object ConsumerAck::NewInstance(Napi::Env env) {
  AddonData* addon_data = GetAddonData(env);
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  if (addon_data == nullptr || IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ACK_NULL_ADDON_DATA")) {
#else
  if (addon_data == nullptr) {
#endif
    Napi::Error::New(env, "ConsumerAck constructor not initialized").ThrowAsJavaScriptException();
    return Napi::Object();
  }
  return addon_data->consumer_ack_constructor.New({});
}

ConsumerAck::ConsumerAck(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ConsumerAck>(info) {}

void ConsumerAck::SetPromise(std::promise<bool>&& promise) {
  promise_ = std::move(promise);
}

void ConsumerAck::Done(std::exception_ptr exception) {
  if (!done_called_.exchange(true)) {
    try {
      promise_.set_exception(exception);
    } catch (const std::future_error& e) {
      // 记录错误但不抛出异常，因为这可能在析构函数中调用
      fprintf(stderr, "[RocketMQ] Warning: Failed to set exception in ConsumerAck::Done: %s\n", e.what());
    } catch (const std::exception& e) {
      fprintf(stderr, "[RocketMQ] Warning: Unexpected error in ConsumerAck::Done: %s\n", e.what());
    }
  }
}

Napi::Value ConsumerAck::Done(const Napi::CallbackInfo& info) {
  if (done_called_.exchange(true)) {
    return info.Env().Undefined();
  }

  bool ack = true;
  if (info.Length() >= 1) {
    Napi::Value ack_value = info[0];
    if (ack_value.IsBoolean() && !ack_value.ToBoolean()) {
      ack = false;
    }
  }

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR")) {
    try {
      promise_.set_value(true);
    } catch (const std::future_error&) {
    }
  }
#endif
  try {
    promise_.set_value(ack);
  } catch (const std::future_error& e) {
    fprintf(stderr, "[RocketMQ] Warning: Failed to set value in ConsumerAck::Done: %s\n", e.what());
  } catch (const std::exception& e) {
    fprintf(stderr, "[RocketMQ] Warning: Unexpected error in ConsumerAck::Done: %s\n", e.what());
  }
  return info.Env().Undefined();
}

}  // namespace __node_rocketmq__
