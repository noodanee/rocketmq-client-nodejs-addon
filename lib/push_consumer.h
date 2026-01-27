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
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports, AddonData* addon_data);

  RocketMQPushConsumer(const Napi::CallbackInfo& info);
  ~RocketMQPushConsumer();

 private:
  Napi::Value SetSessionCredentials(const Napi::CallbackInfo& info);

  Napi::Value Start(const Napi::CallbackInfo& info);
  Napi::Value Shutdown(const Napi::CallbackInfo& info);

  Napi::Value Subscribe(const Napi::CallbackInfo& info);
  Napi::Value SetListener(const Napi::CallbackInfo& info);

 private:
  void SetOptions(const Napi::Object& options);
  void SafeShutdown();

 private:
  rocketmq::DefaultMQPushConsumer consumer_;
  std::unique_ptr<ConsumerMessageListener> listener_;
  std::atomic<bool> is_started_{false};
  std::atomic<bool> is_shutting_down_{false};
  std::atomic<bool> is_destroyed_{false};
  // state_mutex_ 用于保护状态转换的原子性
  // 注意：start()/shutdown() 期间会持有锁，调用方应避免在回调中访问状态
  mutable std::mutex state_mutex_;
};

}  // namespace __node_rocketmq__

#endif
