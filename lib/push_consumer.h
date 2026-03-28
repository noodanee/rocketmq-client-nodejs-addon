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
#ifndef __ROCKETMQ_PUSH_CONSUMER_H__
#define __ROCKETMQ_PUSH_CONSUMER_H__

#include <string>
#include <atomic>
#include <memory>
#include <mutex>

#include <napi.h>

#include <DefaultMQPushConsumer.h>

namespace __node_rocketmq__ {

struct AddonData;
class ConsumerStartWorker;
class ConsumerShutdownWorker;
class ConsumerMessageListener;

class RocketMQPushConsumer : public Napi::ObjectWrap<RocketMQPushConsumer> {
  friend class ConsumerStartWorker;
  friend class ConsumerShutdownWorker;
  friend class ConsumerMessageListener;
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports, AddonData* addon_data);

  RocketMQPushConsumer(const Napi::CallbackInfo& info);
  ~RocketMQPushConsumer();

  enum class LifecycleState : uint8_t {
    kIdle = 0,
    kStarting,
    kStarted,
    kShuttingDown,
    kShutdown,
    kDestroyed
  };

 private:
  Napi::Value SetSessionCredentials(const Napi::CallbackInfo& info);

  Napi::Value Start(const Napi::CallbackInfo& info);
  Napi::Value Shutdown(const Napi::CallbackInfo& info);
  Napi::Value IsListenerIdle(const Napi::CallbackInfo& info);

  Napi::Value Subscribe(const Napi::CallbackInfo& info);
  Napi::Value SetListener(const Napi::CallbackInfo& info);
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  Napi::Value ForceDestroyForTest(const Napi::CallbackInfo& info);
#endif

 private:
  void SetOptions(const Napi::Object& options, Napi::Env env);
  void SafeShutdown();

  bool TryTransitionState(LifecycleState expected, LifecycleState desired);
  LifecycleState GetState() const { return lifecycle_state_.load(); }

 private:
  std::shared_ptr<ConsumerMessageListener> listener_;
  std::atomic<LifecycleState> lifecycle_state_{LifecycleState::kIdle};
  mutable std::mutex state_mutex_;
  mutable std::mutex native_access_mutex_;
  bool sdk_shutdown_called_{false};  // Protected by state_mutex_; ensures consumer_.shutdown() once
  bool shutdown_worker_active_{false};  // Protected by state_mutex_; prevents concurrent shutdown workers
  rocketmq::DefaultMQPushConsumer consumer_;
};

}  // namespace __node_rocketmq__

#endif
