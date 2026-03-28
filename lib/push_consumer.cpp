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
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <exception>
#include <future>
#include <limits>
#include <stdexcept>
#include <thread>

#include <napi.h>

#include <ClientRPCHook.h>
#include <LoggerConfig.h>
#include <MQMessageListener.h>

#include "addon_data.h"
#include "consumer_ack.h"
#include "common_utils.h"

namespace __node_rocketmq__ {

using LifecycleState = RocketMQPushConsumer::LifecycleState;

void RequestListenerShutdown(const std::shared_ptr<ConsumerMessageListener>& listener);
void FinalizeListenerShutdown(const std::shared_ptr<ConsumerMessageListener>& listener);
void ResumeListener(const std::shared_ptr<ConsumerMessageListener>& listener);
bool CheckListenerIdle(const std::shared_ptr<ConsumerMessageListener>& listener);
bool WaitForListenerIdle(const std::shared_ptr<ConsumerMessageListener>& listener,
                         std::chrono::milliseconds timeout);

Napi::Object RocketMQPushConsumer::Init(Napi::Env env, Napi::Object exports, AddonData* addon_data) {
  Napi::Function func = DefineClass(
      env,
      "RocketMQPushConsumer",
      {
          InstanceMethod<&RocketMQPushConsumer::Start>("start"),
          InstanceMethod<&RocketMQPushConsumer::Shutdown>("shutdown"),
          InstanceMethod<&RocketMQPushConsumer::IsListenerIdle>("isListenerIdle"),
          InstanceMethod<&RocketMQPushConsumer::Subscribe>("subscribe"),
          InstanceMethod<&RocketMQPushConsumer::SetListener>("setListener"),
          InstanceMethod<&RocketMQPushConsumer::SetSessionCredentials>(
              "setSessionCredentials"),
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
          InstanceMethod<&RocketMQPushConsumer::ForceDestroyForTest>("__testForceDestroy"),
#endif
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
    SetOptions(options.ToObject(), info.Env());
  }
}

RocketMQPushConsumer::~RocketMQPushConsumer() {
  // SDK destructor now calls shutdown() with try-catch, which waits for all
  // inflight consumeMessage calls to complete. The SDK holds a shared_ptr to
  // the listener, preventing UAF. No leaked-listener system needed.
  std::shared_ptr<ConsumerMessageListener> listener;
  bool was_started = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState state = lifecycle_state_.load();
    if (state == LifecycleState::kDestroyed) {
      return;
    }
    listener = listener_;
    was_started = (state == LifecycleState::kStarted ||
                   state == LifecycleState::kShuttingDown);
    lifecycle_state_.store(LifecycleState::kDestroyed);
  }

  RequestListenerShutdown(listener);

  // Wait for all inflight consumeMessage calls to exit before calling
  // consumer_.shutdown(). The SDK's shutdown joins consume threads;
  // if any thread is still in the poll loop waiting for an ack future,
  // shutdown would block. RequestListenerShutdown sets shutdown_requested_
  // which breaks poll loops, then we wait for threads to actually exit.
  bool listener_idle = WaitForListenerIdle(listener, std::chrono::seconds(5));
  if (!listener_idle) {
    fprintf(stderr, "[RocketMQ] Warning: Listener did not become idle within timeout in destructor\n");
  }

  // Always call explicit shutdown when consumer was started.
  // set_shutdown_on_destroy(false) is ineffective: DefaultMQPushConsumerImpl
  // destructor unconditionally calls shutdown. Calling explicitly ensures
  // proper ordering (before TSFN cleanup) and makes the SDK destructor a no-op.
  if (was_started) {
    bool should_shutdown = false;
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      if (!sdk_shutdown_called_) {
        sdk_shutdown_called_ = true;
        should_shutdown = true;
      }
    }
    if (should_shutdown) {
      try {
        consumer_.shutdown();
      } catch (const std::exception& e) {
        fprintf(stderr, "[RocketMQ] Warning: Consumer shutdown failed in destructor: %s\n", e.what());
      } catch (...) {
        fprintf(stderr, "[RocketMQ] Warning: Unknown error during consumer shutdown in destructor\n");
      }
    }
  }

  FinalizeListenerShutdown(listener);

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    listener_.reset();
  }
}

bool RocketMQPushConsumer::TryTransitionState(LifecycleState expected, LifecycleState desired) {
  return lifecycle_state_.compare_exchange_strong(expected, desired);
}

void RocketMQPushConsumer::SafeShutdown() {
  std::shared_ptr<ConsumerMessageListener> listener;
  bool was_started = false;
  bool was_starting = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState state = lifecycle_state_.load();
    if (state == LifecycleState::kDestroyed) {
      return;
    }
    listener = listener_;
    was_started = (state == LifecycleState::kStarted ||
                   state == LifecycleState::kShuttingDown);
    was_starting = (state == LifecycleState::kStarting);
    lifecycle_state_.store(LifecycleState::kDestroyed);
  }

  RequestListenerShutdown(listener);

  // Wait for inflight consumeMessage calls to drain before SDK shutdown.
  bool listener_idle = WaitForListenerIdle(listener, std::chrono::seconds(5));
  if (!listener_idle) {
    fprintf(stderr, "[RocketMQ] Warning: Listener did not become idle within timeout in SafeShutdown\n");
  }

  if (was_started) {
    bool should_shutdown = false;
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      if (!sdk_shutdown_called_) {
        sdk_shutdown_called_ = true;
        should_shutdown = true;
      }
    }
    if (should_shutdown) {
      try {
        std::lock_guard<std::mutex> native_lock(native_access_mutex_);
        consumer_.shutdown();
      } catch (const std::exception& e) {
        fprintf(stderr, "[RocketMQ] Warning: Consumer shutdown failed: %s\n", e.what());
      } catch (...) {
        fprintf(stderr, "[RocketMQ] Warning: Unknown error during consumer shutdown\n");
      }
    }
  }

  if (listener && was_started) {
    FinalizeListenerShutdown(listener);
  }

  if (!was_starting) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    listener_.reset();
  }
}

void RocketMQPushConsumer::SetOptions(const Napi::Object& options, Napi::Env env) {
  Napi::Value name_server = options.Get("nameServer");
  if (name_server.IsString()) {
    consumer_.set_namesrv_addr(name_server.ToString());
  }

  Napi::Value group_name = options.Get("groupName");
  if (group_name.IsString()) {
    consumer_.set_group_name(group_name.ToString());
  }

  Napi::Value thread_count = options.Get("threadCount");
  if (!thread_count.IsUndefined() && !thread_count.IsNull()) {
    if (!thread_count.IsNumber()) {
      Napi::TypeError::New(env, "threadCount must be a number")
          .ThrowAsJavaScriptException();
      return;
    }
    double double_val = thread_count.ToNumber().DoubleValue();
    if (double_val != std::floor(double_val) || double_val < 1 ||
        double_val > static_cast<double>(std::numeric_limits<int>::max())) {
      Napi::TypeError::New(env, "threadCount must be a positive integer")
          .ThrowAsJavaScriptException();
      return;
    }
    int count = static_cast<int>(double_val);
    consumer_.set_consume_thread_nums(count);
  }

  Napi::Value max_batch_size = options.Get("maxBatchSize");
  if (max_batch_size.IsNumber()) {
    consumer_.set_consume_message_batch_max_size(max_batch_size.ToNumber());
  }

  Napi::Value max_reconsume_times = options.Get("maxReconsumeTimes");
  if (max_reconsume_times.IsNumber()) {
    consumer_.set_max_reconsume_times(max_reconsume_times.ToNumber());
  }

  utils::SetLoggerOptions(options);
}

Napi::Value RocketMQPushConsumer::SetSessionCredentials(
    const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState state = lifecycle_state_.load();
    if (state != LifecycleState::kIdle) {
      Napi::Error::New(env, "Cannot set session credentials after consumer has been started or destroyed")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }
  }

  if (!utils::ValidateStringArguments(info, 3, "All arguments must be strings")) {
    return env.Undefined();
  }

  Napi::String access_key = info[0].As<Napi::String>();
  Napi::String secret_key = info[1].As<Napi::String>();
  Napi::String ons_channel = info[2].As<Napi::String>();

  auto rpc_hook = std::make_shared<rocketmq::ClientRPCHook>(
      rocketmq::SessionCredentials(access_key, secret_key, ons_channel));

  {
    std::lock(native_access_mutex_, state_mutex_);
    std::lock_guard<std::mutex> native_lock(native_access_mutex_, std::adopt_lock);
    std::lock_guard<std::mutex> lock(state_mutex_, std::adopt_lock);
    LifecycleState state = lifecycle_state_.load();
    if (state != LifecycleState::kIdle) {
      Napi::Error::New(env, "Cannot set session credentials after consumer has been started or destroyed")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }
    consumer_.setRPCHook(rpc_hook);
  }

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
    {
      std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
      LifecycleState state = wrapper_->lifecycle_state_.load();
      if (state == LifecycleState::kDestroyed) {
        SetError("Consumer has been destroyed");
        return;
      }
    }

    bool started = false;
    std::string error;
    try {
      std::lock_guard<std::mutex> native_lock(wrapper_->native_access_mutex_);
      consumer_->start();
      started = true;
    } catch (const std::exception& e) {
      error = e.what();
    } catch (...) {
      error = "Unknown error during consumer start";
    }

    bool destroyed = false;
    std::shared_ptr<ConsumerMessageListener> listener;
    {
      std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
      if (started) {
        LifecycleState state = wrapper_->lifecycle_state_.load();
        if (state == LifecycleState::kStarting) {
          wrapper_->lifecycle_state_.store(LifecycleState::kStarted);
        } else if (state == LifecycleState::kDestroyed) {
          destroyed = true;
          listener = wrapper_->listener_;
        }
      } else {
        if (wrapper_->lifecycle_state_.load() == LifecycleState::kStarting) {
          wrapper_->lifecycle_state_.store(LifecycleState::kShutdown);
        }
        listener = wrapper_->listener_;
      }
    }

    if (!started) {
      if (listener) {
        RequestListenerShutdown(listener);
        bool idle = WaitForListenerIdle(listener, std::chrono::seconds(5));
        if (!idle) {
          // Start failed and listener threads didn't drain. Call shutdown
          // explicitly — set_shutdown_on_destroy(false) is ineffective since
          // DefaultMQPushConsumerImpl destructor calls shutdown unconditionally.
          bool should_shutdown = false;
          {
            std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
            if (!wrapper_->sdk_shutdown_called_) {
              wrapper_->sdk_shutdown_called_ = true;
              should_shutdown = true;
            }
          }
          if (should_shutdown) {
            try {
              std::lock_guard<std::mutex> native_lock(wrapper_->native_access_mutex_);
              consumer_->shutdown();
            } catch (...) {
            }
          }
        }
        FinalizeListenerShutdown(listener);
        {
          std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
          wrapper_->listener_.reset();
        }
        deferred_listener_ = std::move(listener);
      }
      SetError(error.empty() ? "Consumer start failed" : error.c_str());
      return;
    }

    if (destroyed) {
      RequestListenerShutdown(listener);
      bool idle = WaitForListenerIdle(listener, std::chrono::seconds(5));

      if (idle) {
        bool should_shutdown = false;
        {
          std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
          if (!wrapper_->sdk_shutdown_called_) {
            wrapper_->sdk_shutdown_called_ = true;
            should_shutdown = true;
          }
        }
        if (should_shutdown) {
          try {
            std::lock_guard<std::mutex> native_lock(wrapper_->native_access_mutex_);
            consumer_->shutdown();
          } catch (...) {
          }
        }
      } else {
        // Listener threads didn't drain. Call shutdown explicitly —
        // set_shutdown_on_destroy(false) is ineffective since
        // DefaultMQPushConsumerImpl destructor calls shutdown unconditionally.
        bool should_shutdown = false;
        {
          std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
          if (!wrapper_->sdk_shutdown_called_) {
            wrapper_->sdk_shutdown_called_ = true;
            should_shutdown = true;
          }
        }
        if (should_shutdown) {
          try {
            std::lock_guard<std::mutex> native_lock(wrapper_->native_access_mutex_);
            consumer_->shutdown();
          } catch (...) {
          }
        }
      }

      FinalizeListenerShutdown(listener);
      {
        std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
        wrapper_->listener_.reset();
      }
      // Prevent ConsumerMessageListener destructor on worker thread:
      // move to member so it releases on main thread with AsyncWorker.
      deferred_listener_ = std::move(listener);

      SetError("Consumer has been destroyed");
    }
  }

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  void OnOK() override {
    Napi::AsyncWorker::OnOK();
    if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_START_CALLBACK_THEN_THROW")) {
      throw Napi::Error::New(Env(), "callback then throw test error");
    }
  }
#endif

 private:
  Napi::ObjectReference wrapper_ref_;
  rocketmq::DefaultMQPushConsumer* consumer_;
  RocketMQPushConsumer* wrapper_;
  std::shared_ptr<ConsumerMessageListener> deferred_listener_;
};

Napi::Value RocketMQPushConsumer::Start(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  Napi::Function callback = info[0].As<Napi::Function>();

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState current_state = lifecycle_state_.load();
    if (current_state == LifecycleState::kDestroyed) {
      Napi::Error::New(env, "Consumer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShutdown) {
      Napi::Error::New(env, "Consumer cannot be restarted after shutdown").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kStarted) {
      Napi::Error::New(env, "Consumer is already started").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShuttingDown) {
      Napi::Error::New(env, "Consumer is stopping, please wait for shutdown to complete").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kStarting) {
      Napi::Error::New(env, "Consumer is already starting").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    lifecycle_state_.store(LifecycleState::kStarting);
  }

  auto* worker = new ConsumerStartWorker(callback, this);
  try {
    worker->Queue();
  } catch (...) {
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      lifecycle_state_.store(LifecycleState::kIdle);
    }
    delete worker;
    Napi::Error::New(env, "Failed to queue consumer start operation").ThrowAsJavaScriptException();
    return env.Undefined();
  }
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
    std::shared_ptr<ConsumerMessageListener> listener;
    {
      std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
      LifecycleState state = wrapper_->lifecycle_state_.load();
      if (state == LifecycleState::kDestroyed) {
        wrapper_->shutdown_worker_active_ = false;
        SetError("Consumer has been destroyed");
        return;
      }
      listener = wrapper_->listener_;
    }

    RequestListenerShutdown(listener);

    // Wait for inflight consumeMessage calls to drain before SDK shutdown.
    // The SDK's shutdown() joins consume threads; if any thread is still
    // in the ack-future poll loop, shutdown would deadlock. After
    // RequestListenerShutdown sets shutdown_requested_, poll loops break,
    // then we wait for threads to actually exit consumeMessage().
    bool listener_idle = WaitForListenerIdle(listener, std::chrono::seconds(5));
    if (!listener_idle) {
      fprintf(stderr, "[RocketMQ] Warning: Listener did not become idle within timeout during shutdown\n");
    }

    bool shutdown_ok = false;
    std::string error;
    {
      bool should_shutdown = false;
      {
        std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
        if (!wrapper_->sdk_shutdown_called_) {
          wrapper_->sdk_shutdown_called_ = true;
          should_shutdown = true;
        }
      }
      if (should_shutdown) {
        try {
          std::lock_guard<std::mutex> native_lock(wrapper_->native_access_mutex_);
          consumer_->shutdown();
          shutdown_ok = true;
        } catch (const std::exception& e) {
          error = e.what();
        } catch (...) {
          error = "Unknown error during consumer shutdown";
        }
      } else {
        shutdown_ok = true;
      }
    }

    if (!shutdown_ok) {
      // Shutdown failed — keep kShuttingDown to prevent further operations.
      // Don't finalize/reset listener here; it was already RequestShutdown'd
      // and the consumer is in an indeterminate state. Finalizing would make
      // a rollback to kStarted impossible and create inconsistency.
      {
        std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
        wrapper_->sdk_shutdown_called_ = false;
        wrapper_->shutdown_worker_active_ = false;
      }
      // Prevent ConsumerMessageListener destructor on worker thread:
      // move to member so it releases on main thread with AsyncWorker.
      deferred_listener_ = std::move(listener);
      SetError(error.empty() ? "Consumer shutdown failed" : error.c_str());
      return;
    }

    // SDK shutdown complete — all inflight consumeMessage calls done.
    // Safe to finalize TSFN immediately (no UAF risk with shared_ptr).
    FinalizeListenerShutdown(listener);
    {
      std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
      wrapper_->listener_.reset();
      if (wrapper_->lifecycle_state_.load() == LifecycleState::kShuttingDown) {
        wrapper_->lifecycle_state_.store(LifecycleState::kShutdown);
      }
      wrapper_->shutdown_worker_active_ = false;
    }
    // Prevent ConsumerMessageListener destructor on worker thread:
    // move to member so it releases on main thread with AsyncWorker.
    deferred_listener_ = std::move(listener);
  }

 private:
  Napi::ObjectReference wrapper_ref_;
  rocketmq::DefaultMQPushConsumer* consumer_;
  RocketMQPushConsumer* wrapper_;
  std::shared_ptr<ConsumerMessageListener> deferred_listener_;
};

Napi::Value RocketMQPushConsumer::Shutdown(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  Napi::Function callback = info[0].As<Napi::Function>();

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState current_state = lifecycle_state_.load();
    if (current_state == LifecycleState::kDestroyed) {
      Napi::Error::New(env, "Consumer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShutdown) {
      Napi::Error::New(env, "Consumer is already stopped").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShuttingDown) {
      if (shutdown_worker_active_) {
        Napi::Error::New(env, "Consumer is already shutting down").ThrowAsJavaScriptException();
        return env.Undefined();
      }
      // Allow retry — previous shutdown attempt failed but kept kShuttingDown
    } else if (current_state != LifecycleState::kStarted) {
      Napi::Error::New(env, "Consumer is not started").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    lifecycle_state_.store(LifecycleState::kShuttingDown);
    shutdown_worker_active_ = true;
  }

  auto* worker = new ConsumerShutdownWorker(callback, this);
  try {
    worker->Queue();
  } catch (...) {
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      lifecycle_state_.store(LifecycleState::kStarted);
      shutdown_worker_active_ = false;
    }
    delete worker;
    Napi::Error::New(env, "Failed to queue consumer shutdown operation").ThrowAsJavaScriptException();
    return env.Undefined();
  }
  return env.Undefined();
}

Napi::Value RocketMQPushConsumer::Subscribe(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (info.Length() < 2) {
    Napi::TypeError::New(env, "Wrong number of arguments").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (!info[0].IsString() || !info[1].IsString()) {
    Napi::TypeError::New(env, "Topic and expression must be strings").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState state = lifecycle_state_.load();
    if (state == LifecycleState::kDestroyed) {
      Napi::Error::New(env, "Consumer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    if (state == LifecycleState::kShuttingDown) {
      Napi::Error::New(env, "Consumer is shutting down").ThrowAsJavaScriptException();
      return env.Undefined();
    }
  }

  Napi::String topic = info[0].As<Napi::String>();
  Napi::String expression = info[1].As<Napi::String>();

  try {
    std::lock(native_access_mutex_, state_mutex_);
    std::lock_guard<std::mutex> native_lock(native_access_mutex_, std::adopt_lock);
    std::lock_guard<std::mutex> lock(state_mutex_, std::adopt_lock);
    LifecycleState state = lifecycle_state_.load();
    if (state == LifecycleState::kDestroyed) {
      Napi::Error::New(env, "Consumer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (state == LifecycleState::kShuttingDown) {
      Napi::Error::New(env, "Consumer is shutting down").ThrowAsJavaScriptException();
      return env.Undefined();
    }
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
  std::shared_ptr<std::atomic<bool>> shutdown_requested;
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

  if (data->shutdown_requested && data->shutdown_requested->load()) {
    try {
      data->promise.set_value(false);
    } catch (const std::future_error&) {
    }
    return;
  }

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
    Napi::Value ack_value = IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ACK_EMPTY") ? Napi::Object()
                                                                             : ConsumerAck::NewInstance(env);
#else
    Napi::Value ack_value = ConsumerAck::NewInstance(env);
#endif
    if (ack_value.IsNull() || !ack_value.IsObject()) {
      try {
        data->promise.set_exception(
            std::make_exception_ptr(std::runtime_error("ConsumerAck construction failed: addon lifecycle error")));
      } catch (const std::future_error&) {
      }
      return;
    }
    Napi::Object ack = ack_value.ToObject();

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    ConsumerAck* consumer_ack = IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ACK_NULL")
                                    ? nullptr
                                    : Napi::ObjectWrap<ConsumerAck>::Unwrap(ack);
#else
    ConsumerAck* consumer_ack = Napi::ObjectWrap<ConsumerAck>::Unwrap(ack);
#endif
    if (consumer_ack == nullptr) {
      try {
        data->promise.set_exception(
            std::make_exception_ptr(std::runtime_error("ConsumerAck unwrap failed: null consumer_ack")));
      } catch (const std::future_error&) {
      }
      return;
    }

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_PROMISE_SET")) {
      try {
        data->promise.set_value(true);
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
      utils::ThrowViaMicrotask(env, e.Value());
    }
  } catch (const std::exception& e) {
    fprintf(stderr, "[RocketMQ] Error in message listener setup: %s\n", e.what());
    try {
      data->promise.set_exception(std::current_exception());
    } catch (const std::future_error&) {
    }
  }
}

class ConsumerMessageListener : public rocketmq::MessageListenerConcurrently,
                                public std::enable_shared_from_this<ConsumerMessageListener> {
  struct Private {};

 public:
  static std::shared_ptr<ConsumerMessageListener> Create(Napi::Env env, Napi::Function callback) {
    return std::shared_ptr<ConsumerMessageListener>(
        new ConsumerMessageListener(Private{}, env, std::move(callback)));
  }

  ~ConsumerMessageListener() {
    if (!env_cleanup_done_) {
      napi_remove_env_cleanup_hook(raw_env_, EnvCleanupHook, this);
    }
    try {
      Shutdown();
    } catch (...) {
    }
  }

  void RequestShutdown() {
    shutdown_requested_->store(true);
  }

  void Resume() {
    shutdown_requested_->store(false);
  }

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  void TestHoldInflight() {
    inflight_.fetch_add(1);
  }

  void TestReleaseInflight() {
    const int previous = inflight_.fetch_sub(1);
    (void)previous;
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      inflight_cv_.notify_all();
    }
  }
#endif

  void Shutdown() {
    RequestShutdown();

    if (!released_.exchange(true)) {
      napi_status release_status = listener_.Release();
      if (release_status != napi_ok) {
        fprintf(stderr, "[RocketMQ] Warning: TSFN Release failed: %d\n",
                static_cast<int>(release_status));
      }
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
      if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_RELEASE_THROW")) {
        throw std::runtime_error("consumer release throw");
      }
#endif
    }
  }

  rocketmq::ConsumeStatus consumeMessage(
      std::vector<rocketmq::MQMessageExt>& msgs) override {
    std::shared_ptr<ConsumerMessageListener> self = shared_from_this();

    InflightGuard inflight_guard(self);

    if (self->shutdown_requested_->load()) {
      return rocketmq::ConsumeStatus::RECONSUME_LATER;
    }

    for (auto& msg : msgs) {
      if (self->shutdown_requested_->load()) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }

      std::unique_ptr<MessageAndPromise> data_ptr(new MessageAndPromise{msg, std::promise<bool>(), self->shutdown_requested_});
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
        {
          const auto poll_interval = std::chrono::milliseconds(100);
          auto deadline = std::chrono::steady_clock::now() + wait_time;
          bool resolved = false;
          while (true) {
            auto remaining = deadline - std::chrono::steady_clock::now();
            if (remaining <= std::chrono::milliseconds(0)) break;
            if (self->shutdown_requested_->load()) break;
            auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
            if (future.wait_for(std::min(wait, poll_interval)) != std::future_status::timeout) {
              resolved = true;
              break;
            }
          }
          if (!resolved) {
            return rocketmq::ConsumeStatus::RECONSUME_LATER;
          }
        }
        if (!future.get()) {
          return rocketmq::ConsumeStatus::RECONSUME_LATER;
        }
        continue;
      }
#endif

      if (self->shutdown_requested_->load()) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }

      auto tsfn_guard = TSFNGuard::Create(self->listener_);
      if (!tsfn_guard) {
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }

      napi_status status = napi_ok;
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
      if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_ABORT_TSFN")) {
        self->listener_.Abort();
        tsfn_guard->Disarm();
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      } else if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_BLOCKING_FAIL")) {
        status = napi_generic_failure;
      } else {
        status = self->listener_.BlockingCall(data);
      }
#else
      status = self->listener_.BlockingCall(data);
#endif

      if (status != napi_ok) {
        fprintf(stderr, "[RocketMQ] Error: failed to schedule message to JS listener: %d\n", static_cast<int>(status));
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }

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
        {
          const auto poll_interval = std::chrono::milliseconds(100);
          auto deadline = std::chrono::steady_clock::now() + wait_time;
          bool resolved = false;
          while (true) {
            auto remaining = deadline - std::chrono::steady_clock::now();
            if (remaining <= std::chrono::milliseconds(0)) {
              break;
            }
            if (self->shutdown_requested_->load()) {
              break;
            }
            auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
            if (future.wait_for(std::min(wait, poll_interval)) != std::future_status::timeout) {
              resolved = true;
              break;
            }
          }
          if (!resolved) {
            return rocketmq::ConsumeStatus::RECONSUME_LATER;
          }
        }
        if (!future.get()) {
          return rocketmq::ConsumeStatus::RECONSUME_LATER;
        }
      } catch (const std::future_error& e) {
        fprintf(stderr, "[RocketMQ] Error: future error in consumeMessage: %s\n", e.what());
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      } catch (const std::exception& e) {
        fprintf(stderr, "[RocketMQ] Error: exception in consumeMessage: %s\n", e.what());
        return rocketmq::ConsumeStatus::RECONSUME_LATER;
      }
    }
    return rocketmq::ConsumeStatus::CONSUME_SUCCESS;
  };

  bool IsIdle() const {
    return inflight_.load() == 0;
  }

  bool WaitForIdle(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(inflight_mutex_);
    return inflight_cv_.wait_for(lock, timeout, [this] {
      return inflight_.load() == 0;
    });
  }

 private:
  using Listener =
      Napi::TypedThreadSafeFunction<std::nullptr_t,
                                    MessageAndPromise,
                                    &CallConsumerMessageJsListener>;

  static void EnvCleanupHook(void* arg) {
    auto* self = static_cast<ConsumerMessageListener*>(arg);
    self->env_cleanup_done_ = true;
    // Release the TSFN while the env is still alive.
    // This prevents the TSFN from lingering until UV handle closure
    // when the V8 isolate is already dead.
    self->RequestShutdown();
    if (!self->released_.exchange(true)) {
      self->listener_.Release();
    }
  }

  ConsumerMessageListener(Private, Napi::Env env, Napi::Function callback)
      : listener_(
            Listener::New(env,
                          callback,
                          "RocketMQ Message Listener",
                          0,
                          1,
                          nullptr,
                          [](Napi::Env, void*, std::nullptr_t*) {
                          })),
        released_(false),
        shutdown_requested_(std::make_shared<std::atomic<bool>>(false)),
        inflight_(0),
        raw_env_(static_cast<napi_env>(env)),
        env_cleanup_done_(false) {
    napi_add_env_cleanup_hook(raw_env_, EnvCleanupHook, this);
  }

  class InflightGuard {
   public:
    explicit InflightGuard(std::shared_ptr<ConsumerMessageListener> owner)
        : owner_(std::move(owner)) {
      owner_->inflight_.fetch_add(1);
    }
    ~InflightGuard() {
      owner_->inflight_.fetch_sub(1);
      {
        std::lock_guard<std::mutex> lock(owner_->inflight_mutex_);
        owner_->inflight_cv_.notify_all();
      }
    }
    InflightGuard(const InflightGuard&) = delete;
    InflightGuard& operator=(const InflightGuard&) = delete;

   private:
    std::shared_ptr<ConsumerMessageListener> owner_;
  };

  class TSFNGuard {
   public:
    static std::unique_ptr<TSFNGuard> Create(Listener& tsfn) {
      if (tsfn.Acquire() != napi_ok) {
        return nullptr;
      }
      return std::unique_ptr<TSFNGuard>(new TSFNGuard(tsfn));
    }

    ~TSFNGuard() {
      if (!released_) {
        tsfn_.Release();
      }
    }
    TSFNGuard(const TSFNGuard&) = delete;
    TSFNGuard& operator=(const TSFNGuard&) = delete;
    void Disarm() {
      released_ = true;
    }

   private:
    explicit TSFNGuard(Listener& tsfn) : tsfn_(tsfn), released_(false) {}
    Listener& tsfn_;
    bool released_;
  };

  Listener listener_;
  std::atomic<bool> released_;
  std::shared_ptr<std::atomic<bool>> shutdown_requested_;
  std::atomic<int> inflight_;
  std::mutex inflight_mutex_;
  std::condition_variable inflight_cv_;
  napi_env raw_env_;
  std::atomic<bool> env_cleanup_done_;
};

void RequestListenerShutdown(const std::shared_ptr<ConsumerMessageListener>& listener) {
  if (listener) {
    listener->RequestShutdown();
  }
}

void FinalizeListenerShutdown(const std::shared_ptr<ConsumerMessageListener>& listener) {
  if (listener) {
    try {
      listener->Shutdown();
    } catch (const std::exception& e) {
      fprintf(stderr, "[RocketMQ] Warning: Listener shutdown failed: %s\n", e.what());
    } catch (...) {
      fprintf(stderr, "[RocketMQ] Warning: Unknown error during listener shutdown\n");
    }
  }
}

void ResumeListener(const std::shared_ptr<ConsumerMessageListener>& listener) {
  if (listener) {
    listener->Resume();
  }
}

bool CheckListenerIdle(const std::shared_ptr<ConsumerMessageListener>& listener) {
  return listener && listener->IsIdle();
}

bool WaitForListenerIdle(const std::shared_ptr<ConsumerMessageListener>& listener,
                         std::chrono::milliseconds timeout) {
  if (!listener) return true;
  return listener->WaitForIdle(timeout);
}

Napi::Value RocketMQPushConsumer::IsListenerIdle(const Napi::CallbackInfo& info) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  if (listener_) {
    return Napi::Boolean::New(info.Env(), listener_->IsIdle());
  }
  return Napi::Boolean::New(info.Env(), true);
}

Napi::Value RocketMQPushConsumer::SetListener(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState state = lifecycle_state_.load();
    if (state != LifecycleState::kIdle) {
      Napi::Error::New(env, "Cannot change listener after consumer has been started or destroyed")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }
  }

  std::shared_ptr<ConsumerMessageListener> next =
      ConsumerMessageListener::Create(env, info[0].As<Napi::Function>());
  std::shared_ptr<ConsumerMessageListener> previous;

  {
    std::lock(native_access_mutex_, state_mutex_);
    std::lock_guard<std::mutex> native_lock(native_access_mutex_, std::adopt_lock);
    std::lock_guard<std::mutex> lock(state_mutex_, std::adopt_lock);
    LifecycleState state = lifecycle_state_.load();
    if (state != LifecycleState::kIdle) {
      Napi::Error::New(env, "Cannot change listener after consumer has been started or destroyed")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }

    previous = listener_;
    try {
      // Pass shared_ptr to SDK — SDK now co-owns the listener, preventing UAF.
      consumer_.registerMessageListener(
          std::shared_ptr<rocketmq::MessageListenerConcurrently>(next, static_cast<rocketmq::MessageListenerConcurrently*>(next.get())));
    } catch (const std::exception& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
      return env.Undefined();
    } catch (...) {
      Napi::Error::New(env, "Failed to register consumer listener").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    listener_ = next;
  }

  FinalizeListenerShutdown(previous);

  return env.Undefined();
}

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
Napi::Value RocketMQPushConsumer::ForceDestroyForTest(const Napi::CallbackInfo& info) {
  SafeShutdown();
  return info.Env().Undefined();
}
#endif

}  // namespace __node_rocketmq__
