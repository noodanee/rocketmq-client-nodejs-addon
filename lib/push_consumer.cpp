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
#include "push_consumer.h"

#include <atomic>
#include <cstdlib>
#include <exception>
#include <future>
#include <stdexcept>

#include <napi.h>

#include <ClientRPCHook.h>
#include <LoggerConfig.h>
#include <MQMessageListener.h>

#include "addon_data.h"
#include "consumer_ack.h"
#include "common_utils.h"

namespace __node_rocketmq__ {

Napi::Object RocketMQPushConsumer::Init(Napi::Env env, Napi::Object exports, AddonData* addon_data) {
  Napi::Function func = DefineClass(
      env,
      "RocketMQPushConsumer",
      {
          InstanceMethod<&RocketMQPushConsumer::Start>("start"),
          InstanceMethod<&RocketMQPushConsumer::Shutdown>("shutdown"),
          InstanceMethod<&RocketMQPushConsumer::Subscribe>("subscribe"),
          InstanceMethod<&RocketMQPushConsumer::SetListener>("setListener"),
          InstanceMethod<&RocketMQPushConsumer::SetSessionCredentials>(
              "setSessionCredentials"),
      });

  addon_data->push_consumer_constructor = Napi::Persistent(func);

  exports.Set("PushConsumer", func);
  return exports;
}

RocketMQPushConsumer::RocketMQPushConsumer(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<RocketMQPushConsumer>(info), consumer_("") {
  const Napi::Value group_name = info[0];
  if (group_name.IsString()) {
    consumer_.set_group_name(group_name.ToString());
  }

  const Napi::Value instance_name = info[1];
  if (instance_name.IsString()) {
    consumer_.set_instance_name(instance_name.ToString());
  }

  const Napi::Value options = info[2];
  if (options.IsObject()) {
    // try to set options
    SetOptions(options.ToObject());
  }
}

RocketMQPushConsumer::~RocketMQPushConsumer() {
  SafeShutdown();
}

void RocketMQPushConsumer::SafeShutdown() {
  std::lock_guard<std::mutex> lock(state_mutex_);
  
  if (is_destroyed_.exchange(true)) {
    return; // Already destroyed
  }
  
  // First reset the listener to prevent new messages
  if (listener_) {
    listener_.reset();
  }
  
  if (is_started_.load() && !is_shutting_down_.exchange(true)) {
    try {
      consumer_.shutdown();
    } catch (const std::exception& e) {
      // Log error but don't throw in destructor
      fprintf(stderr, "[RocketMQ] Warning: Consumer shutdown failed in destructor: %s\n", e.what());
    } catch (...) {
      fprintf(stderr, "[RocketMQ] Warning: Unknown error during consumer shutdown in destructor\n");
    }
  }
  
  is_started_.store(false);
}

void RocketMQPushConsumer::SetOptions(const Napi::Object& options) {
  // set name server
  Napi::Value name_server = options.Get("nameServer");
  if (name_server.IsString()) {
    consumer_.set_namesrv_addr(name_server.ToString());
  }

  // set group name
  Napi::Value group_name = options.Get("groupName");
  if (group_name.IsString()) {
    consumer_.set_group_name(group_name.ToString());
  }

  // set thread count
  Napi::Value thread_count = options.Get("threadCount");
  if (thread_count.IsNumber()) {
    consumer_.set_consume_thread_nums(thread_count.ToNumber());
  }

  // set message batch max size
  Napi::Value max_batch_size = options.Get("maxBatchSize");
  if (max_batch_size.IsNumber()) {
    consumer_.set_consume_message_batch_max_size(max_batch_size.ToNumber());
  }

  Napi::Value max_reconsume_times = options.Get("maxReconsumeTimes");
  if (max_reconsume_times.IsNumber()) {
    consumer_.set_max_reconsume_times(max_reconsume_times.ToNumber());
  }

  // 使用通用的日志配置函数
  utils::SetLoggerOptions(options);
}

Napi::Value RocketMQPushConsumer::SetSessionCredentials(
    const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // 使用通用的参数验证函数
  if (!utils::ValidateStringArguments(info, 3, "All arguments must be strings")) {
    return env.Undefined();
  }

  Napi::String access_key = info[0].As<Napi::String>();
  Napi::String secret_key = info[1].As<Napi::String>();
  Napi::String ons_channel = info[2].As<Napi::String>();

  auto rpc_hook = std::make_shared<rocketmq::ClientRPCHook>(
      rocketmq::SessionCredentials(access_key, secret_key, ons_channel));
  consumer_.setRPCHook(rpc_hook);

  return env.Undefined();
}

class ConsumerStartWorker : public Napi::AsyncWorker {
 public:
  ConsumerStartWorker(const Napi::Function& callback,
                      RocketMQPushConsumer* wrapper)
      : Napi::AsyncWorker(callback),
        wrapper_ref_(Napi::Persistent(wrapper->Value())),
        consumer_(&wrapper->consumer_),
        wrapper_(wrapper) {}

  void Execute() override {
    // 在整个操作期间持有锁以避免竞态条件
    std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
    
    if (wrapper_->is_destroyed_.load()) {
      SetError("Consumer has been destroyed");
      return;
    }
    
    if (wrapper_->is_started_.load()) {
      SetError("Consumer is already started");
      return;
    }
    
    if (wrapper_->is_shutting_down_.load()) {
      SetError("Consumer is shutting down");
      return;
    }
    
    try {
      consumer_->start();
      wrapper_->is_started_.store(true);
    } catch (const std::exception& e) {
      SetError(e.what());
    }
  }

 private:
  Napi::ObjectReference wrapper_ref_;
  rocketmq::DefaultMQPushConsumer* consumer_;
  RocketMQPushConsumer* wrapper_;
};

Napi::Value RocketMQPushConsumer::Start(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // 使用通用的回调验证函数
  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  Napi::Function callback = info[0].As<Napi::Function>();

  auto* worker = new ConsumerStartWorker(callback, this);
  worker->Queue();
  return env.Undefined();
}

class ConsumerShutdownWorker : public Napi::AsyncWorker {
 public:
  ConsumerShutdownWorker(const Napi::Function& callback,
                         RocketMQPushConsumer* wrapper)
      : Napi::AsyncWorker(callback),
        wrapper_ref_(Napi::Persistent(wrapper->Value())),
        consumer_(&wrapper->consumer_),
        wrapper_(wrapper) {}

  void Execute() override {
    // 在整个操作期间持有锁以避免竞态条件
    std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
    
    if (wrapper_->is_destroyed_.load()) {
      SetError("Consumer has been destroyed");
      return;
    }
    
    if (!wrapper_->is_started_.load()) {
      SetError("Consumer is not started");
      return;
    }
    
    if (wrapper_->is_shutting_down_.exchange(true)) {
      SetError("Consumer is already shutting down");
      return;
    }
    
    // First reset the listener to prevent new messages
    if (wrapper_->listener_) {
      wrapper_->listener_.reset();
    }
    
    try {
      consumer_->shutdown();
      wrapper_->is_started_.store(false);
      wrapper_->is_shutting_down_.store(false); // Reset shutdown flag after successful shutdown
    } catch (const std::exception& e) {
      wrapper_->is_shutting_down_.store(false); // Reset on error
      SetError(e.what());
    }
  }

 private:
  Napi::ObjectReference wrapper_ref_;
  rocketmq::DefaultMQPushConsumer* consumer_;
  RocketMQPushConsumer* wrapper_;
};

Napi::Value RocketMQPushConsumer::Shutdown(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // 使用通用的回调验证函数
  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  Napi::Function callback = info[0].As<Napi::Function>();

  auto* worker = new ConsumerShutdownWorker(callback, this);
  worker->Queue();
  return env.Undefined();
}

Napi::Value RocketMQPushConsumer::Subscribe(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // Check if required parameters are provided FIRST (before state checks)
  if (info.Length() < 2) {
    Napi::TypeError::New(env, "Wrong number of arguments").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (!info[0].IsString() || !info[1].IsString()) {
    Napi::TypeError::New(env, "Topic and expression must be strings").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Check if consumer is in valid state AFTER parameter validation
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (is_destroyed_.load()) {
      Napi::Error::New(env, "Consumer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    
    if (is_shutting_down_.load()) {
      Napi::Error::New(env, "Consumer is shutting down").ThrowAsJavaScriptException();
      return env.Undefined();
    }
  }

  Napi::String topic = info[0].As<Napi::String>();
  Napi::String expression = info[1].As<Napi::String>();

  try {
    consumer_.subscribe(topic, expression);
  } catch (const std::exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return env.Undefined();
}

struct MessageAndPromise {
  rocketmq::MQMessageExt message;
  std::promise<bool> promise;
};

void CallConsumerMessageJsListener(Napi::Env env,
                                   Napi::Function listener,
                                   std::nullptr_t*,
                                   MessageAndPromise* data) {
  std::unique_ptr<MessageAndPromise> data_guard(data);
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  if (data == nullptr || IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_NULL_DATA")) {
#else
  if (data == nullptr) {
#endif
    if (data != nullptr) {
      try {
        data->promise.set_value(false);
      } catch (const std::future_error&) {
      }
    }
    return;
  }

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  if (env == nullptr || listener == nullptr || IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_NULL_ENV")) {
#else
  if (env == nullptr || listener == nullptr) {
#endif
    try {
      data->promise.set_value(false);
    } catch (const std::future_error&) {
    }
    return;
  }

  Napi::HandleScope scope(env);
  try {
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_THROW")) {
      throw std::runtime_error("consumer stub throw");
    }
#endif

    Napi::Object message = Napi::Object::New(env);
    message.Set("topic", data->message.topic());
    message.Set("tags", data->message.tags());
    message.Set("keys", data->message.keys());
    message.Set("body", data->message.body());
    message.Set("msgId", data->message.msg_id());

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    Napi::Object ack = IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ACK_EMPTY") ? Napi::Object()
                                                                        : ConsumerAck::NewInstance(env);
#else
    Napi::Object ack = ConsumerAck::NewInstance(env);
#endif
    if (ack.IsEmpty()) {
      data->promise.set_value(false);
      return;
    }

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    ConsumerAck* consumer_ack = IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ACK_NULL")
                                    ? nullptr
                                    : Napi::ObjectWrap<ConsumerAck>::Unwrap(ack);
#else
    ConsumerAck* consumer_ack = Napi::ObjectWrap<ConsumerAck>::Unwrap(ack);
#endif
    if (consumer_ack == nullptr) {
      data->promise.set_value(false);
      return;
    }

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_PROMISE_SET")) {
      try {
        data->promise.set_value(true);
      } catch (const std::future_error&) {
      }
    }
#endif

    consumer_ack->SetPromise(std::move(data->promise));

    try {
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
      if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_LISTENER_ERROR")) {
        throw Napi::Error::New(env, "consumer listener error");
      }
#endif
      listener.Call(Napi::Object::New(env), {message, ack});
    } catch (const Napi::Error& e) {
      consumer_ack->Done(std::current_exception());
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
      if (!IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_LISTENER_ERROR")) {
        e.ThrowAsJavaScriptException();
      }
#else
      e.ThrowAsJavaScriptException();
#endif
    }
  } catch (const std::exception&) {
    try {
      data->promise.set_value(false);
    } catch (const std::future_error&) {
    }
  }
}

class ConsumerMessageListener : public rocketmq::MessageListenerConcurrently {
 public:
  ConsumerMessageListener(Napi::Env& env, Napi::Function&& callback)
      : listener_(
            Listener::New(env, callback, "RocketMQ Message Listener", 0, 1)),
        aborted_(false),
        shutdown_requested_(false) {}

  ~ConsumerMessageListener() {
    Shutdown();
  }
  
  void Shutdown() {
    shutdown_requested_.store(true);
    
    if (!aborted_.exchange(true)) {
      try {
        listener_.Release();
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
        if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_RELEASE_THROW")) {
          throw std::runtime_error("consumer release throw");
        }
#endif
      } catch (const std::exception& e) {
        fprintf(stderr, "[RocketMQ] Warning: Error releasing consumer listener: %s\n", e.what());
      } catch (...) {
        fprintf(stderr, "[RocketMQ] Warning: Unknown error releasing consumer listener\n");
      }
    }
  }

  rocketmq::ConsumeStatus consumeMessage(
      std::vector<rocketmq::MQMessageExt>& msgs) override {
    
    // Check if shutdown was requested
    if (shutdown_requested_.load()) {
      return rocketmq::ConsumeStatus::RECONSUME_LATER;
    }
    
    for (auto& msg : msgs) {
      // Double check shutdown status for each message
      if (shutdown_requested_.load()) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }
      
      // 使用智能指针管理内存，确保异常安全
      std::unique_ptr<MessageAndPromise> data_ptr(new MessageAndPromise{msg, std::promise<bool>()});
      auto* data = data_ptr.get();
      auto future = data->promise.get_future();

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
      if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET")) {
        bool preset = !IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET_FALSE");
        try {
          data->promise.set_value(preset);
          data->promise.set_value(preset);
        } catch (const std::future_error&) {
        }
      }

      if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_TIMEOUT_SKIP_CALL")) {
        auto wait_time = IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_TIMEOUT")
                             ? std::chrono::milliseconds(0)
                             : config::DEFAULT_MESSAGE_TIMEOUT;
        if (future.wait_for(wait_time) == std::future_status::timeout) {
          return rocketmq::ConsumeStatus::RECONSUME_LATER;
        }
        if (!future.get()) {
          return rocketmq::ConsumeStatus::RECONSUME_LATER;
        }
        continue;
      }
#endif

      napi_status status = napi_ok;
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
      if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ABORT_TSFN")) {
        listener_.Abort();
        aborted_.store(true);
        status = napi_generic_failure;
      } else if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_BLOCKING_FAIL")) {
        status = napi_generic_failure;
      } else {
        status = listener_.BlockingCall(data);
      }
#else
      // Check if we're already aborted before making the call
      if (aborted_.load() || shutdown_requested_.load()) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }
      
      status = listener_.BlockingCall(data);
#endif
      if (status != napi_ok) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }

      // 成功调用后释放智能指针的所有权，让 CallConsumerMessageJsListener 管理内存
      data_ptr.release();

      try {
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
        if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_FORCE_FUTURE_ERROR")) {
          std::promise<bool> probe;
          auto probe_future = probe.get_future();
          (void)probe_future;
          probe.get_future();
        }

        auto wait_time = IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_TIMEOUT")
                             ? std::chrono::milliseconds(0)
                             : config::DEFAULT_MESSAGE_TIMEOUT;
#else
        auto wait_time = config::DEFAULT_MESSAGE_TIMEOUT;
#endif
        if (future.wait_for(wait_time) == std::future_status::timeout) {
          return rocketmq::ConsumeStatus::RECONSUME_LATER;
        }
        if (!future.get()) {
          return rocketmq::ConsumeStatus::RECONSUME_LATER;
        }
      } catch (const std::future_error&) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      } catch (const std::exception&) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }
    }
    return rocketmq::ConsumeStatus::CONSUME_SUCCESS;
  };

 private:
  using Listener =
      Napi::TypedThreadSafeFunction<std::nullptr_t,
                                    MessageAndPromise,
                                    &CallConsumerMessageJsListener>;

  Listener listener_;
  std::atomic<bool> aborted_;
  std::atomic<bool> shutdown_requested_;
};

Napi::Value RocketMQPushConsumer::SetListener(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // 使用通用的回调验证函数
  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  // Check if consumer is in valid state AFTER parameter validation
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (is_destroyed_.load()) {
      Napi::Error::New(env, "Consumer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    
    if (is_shutting_down_.load()) {
      Napi::Error::New(env, "Consumer is shutting down").ThrowAsJavaScriptException();
      return env.Undefined();
    }
  }

  // Safely replace the listener
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    
    // Reset old listener if exists (destructor will handle cleanup)
    if (listener_) {
      listener_.reset();
    }
    
    listener_.reset(
        new ConsumerMessageListener(env, info[0].As<Napi::Function>()));
    consumer_.registerMessageListener(listener_.get());
  }
  
  return env.Undefined();
}

}  // namespace __node_rocketmq__
