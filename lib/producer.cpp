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
#include "producer.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <string>
#include <system_error>

#include <napi.h>

#include <ClientRPCHook.h>
#include <LoggerConfig.h>
#include <MQException.h>
#include <MQMessage.h>
#include <SendCallback.h>

#include "addon_data.h"
#include "common_utils.h"

namespace __node_rocketmq__ {

namespace {

std::chrono::milliseconds PendingSendWaitTimeout() {
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  const char* value = std::getenv("ROCKETMQ_STUB_PRODUCER_SHUTDOWN_WAIT_MS");
  if (value != nullptr && value[0] != '\0') {
    char* end = nullptr;
    long long parsed = std::strtoll(value, &end, 10);
    if (end != value && parsed >= 0) {
      return std::chrono::milliseconds(parsed);
    }
  }
#endif
  return std::chrono::seconds(30);
}

}  // namespace

Napi::Object RocketMQProducer::Init(Napi::Env env, Napi::Object exports, AddonData* addon_data) {
  Napi::Function func =
      DefineClass(env,
                  "RocketMQProducer",
                  {
                      InstanceMethod<&RocketMQProducer::Start>("start"),
                      InstanceMethod<&RocketMQProducer::Shutdown>("shutdown"),
                      InstanceMethod<&RocketMQProducer::Send>("send"),
                      InstanceMethod<&RocketMQProducer::SetSessionCredentials>(
                          "setSessionCredentials"),
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
                      InstanceMethod<&RocketMQProducer::ForceDestroyForTest>("__testForceDestroy"),
                      InstanceMethod<&RocketMQProducer::SimulateShutdownTimeoutForTest>("__testSimulateShutdownTimeout"),
#endif
                  });

  addon_data->producer_constructor = Napi::Persistent(func);

  exports.Set("Producer", func);
  return exports;
}

RocketMQProducer::RocketMQProducer(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<RocketMQProducer>(info),
      pending_send_state_(std::make_shared<PendingSendState>()),
      producer_("") {
  const Napi::Value group_name = info[0];
  if (group_name.IsString()) {
    producer_.set_group_name(group_name.ToString());
  }

  const Napi::Value instance_name = info[1];
  if (instance_name.IsString()) {
    producer_.set_instance_name(instance_name.ToString());
  }

  const Napi::Value options = info[2];
  if (options.IsObject()) {
    // try to set options
    SetOptions(options.ToObject());
  }
}

RocketMQProducer::~RocketMQProducer() {
  // Safety invariant: This destructor only runs when ALL prevent_gc strong
  // references (one per in-flight send) have been released via Finalize.
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState state = lifecycle_state_.load();
    if (state == LifecycleState::kDestroyed) {
      return;
    }
    lifecycle_state_.store(LifecycleState::kDestroyed);
  }

  // Explicitly shutdown the native producer FIRST, before cleaning up
  // callbacks. This ensures late SDK callbacks complete or are rejected
  // by the SDK itself before we orphan-clean them.
  {
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
        std::lock_guard<std::mutex> lock(native_access_mutex_);
        producer_.shutdown();
      } catch (...) {
        // Best-effort shutdown; SDK destructor will retry anyway.
      }
    }
  }

  CancelPendingSends(pending_send_state_);
  JoinCancelTimerThread();

  // Always delete orphaned callbacks: even if explicit shutdown failed,
  // the producer_ member destructor will call SDK shutdown (with try-catch),
  // guaranteeing no further callbacks fire after this object is destroyed.
  DeleteOrphanedCallbacks(pending_send_state_);
}

bool RocketMQProducer::TryTransitionState(LifecycleState expected, LifecycleState desired) {
  return lifecycle_state_.compare_exchange_strong(expected, desired);
}

void RocketMQProducer::SafeShutdown(bool wait_for_pending_sends) {
  bool was_transitioning = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState state = lifecycle_state_.load();
    if (state == LifecycleState::kDestroyed) {
      return;
    }
    was_transitioning = (state == LifecycleState::kShuttingDown ||
                         state == LifecycleState::kStarting);
    lifecycle_state_.store(LifecycleState::kDestroyed);
  }

  if (was_transitioning) {
    CancelPendingSends(pending_send_state_);
  }

  JoinCancelTimerThread();

  if (wait_for_pending_sends && !was_transitioning) {
    bool wait_success = WaitForPendingSends(
        pending_send_state_, PendingSendWaitTimeout());
    if (!wait_success) {
      CancelPendingSends(pending_send_state_);
    }
  }

  // SDK destructor will call shutdown() automatically with try-catch.
  // Explicit shutdown here for immediate resource release.
  // Idempotent: sdk_shutdown_called_ ensures at most one call.
  {
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
        std::lock_guard<std::mutex> lock(native_access_mutex_);
        producer_.shutdown();
      } catch (const std::exception& e) {
        fprintf(stderr, "[RocketMQ] Warning: Producer shutdown failed: %s\n", e.what());
      } catch (...) {
        fprintf(stderr, "[RocketMQ] Warning: Unknown error during producer shutdown\n");
      }
    }
  }

  // Always delete orphaned callbacks: even if explicit shutdown failed,
  // the producer_ member destructor will call SDK shutdown (with try-catch),
  // guaranteeing no further callbacks fire after this object is destroyed.
  DeleteOrphanedCallbacks(pending_send_state_);
}

bool RocketMQProducer::WaitForPendingSends(
    const std::shared_ptr<PendingSendState>& pending_send_state,
    std::chrono::milliseconds timeout) {
  if (pending_send_state == nullptr) {
    return true;
  }
  std::unique_lock<std::mutex> lock(pending_send_state->mutex);
  return pending_send_state->cv.wait_for(lock, timeout, [&pending_send_state] {
    return pending_send_state->pending_send_callbacks == 0;
  });
}

void RocketMQProducer::ReleasePendingSend(
    const std::shared_ptr<PendingSendState>& pending_send_state) {
  if (pending_send_state == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(pending_send_state->mutex);
  if (pending_send_state->pending_send_callbacks == 0) {
    return;
  }
  pending_send_state->pending_send_callbacks--;
  if (pending_send_state->pending_send_callbacks == 0) {
    pending_send_state->cv.notify_all();
  }
}

void RocketMQProducer::CancelPendingSends(
    const std::shared_ptr<PendingSendState>& pending_send_state) {
  if (pending_send_state == nullptr) {
    return;
  }
  // Increment generation to invalidate all pending callbacks from previous lifecycle
  pending_send_state->generation.fetch_add(1);

  std::vector<PendingSendState::PendingTSFNHandle> handles_to_abort;
  {
    std::lock_guard<std::mutex> lock(pending_send_state->mutex);
    pending_send_state->pending_send_callbacks = 0;
    pending_send_state->cv.notify_all();
    handles_to_abort = std::move(pending_send_state->pending_tsfn_handles);
  }

  // Force-abort all tracked TSFNs outside the lock.
  // This triggers Finalize on the main thread, which releases pinned JS objects
  // (prevent_gc, callback_ref) even if the SDK never calls back.
  for (auto& handle : handles_to_abort) {
    if (!handle.released->exchange(true)) {
      napi_release_threadsafe_function(handle.tsfn, napi_tsfn_abort);
    }
  }
}

uint64_t RocketMQProducer::GetCurrentGeneration(
    const std::shared_ptr<PendingSendState>& pending_send_state) {
  if (pending_send_state == nullptr) {
    return 0;
  }
  return pending_send_state->generation.load();
}

void RocketMQProducer::JoinCancelTimerThread() {
  std::unique_ptr<std::thread> t;
  {
    std::lock_guard<std::mutex> lock(cancel_timer_mutex_);
    t = std::move(cancel_timer_thread_);
  }
  if (t && t->joinable()) {
    t->join();
  }
}

void RocketMQProducer::SetOptions(const Napi::Object& options) {
  // set name server
  Napi::Value name_server = options.Get("nameServer");
  if (name_server.IsString()) {
    producer_.set_namesrv_addr(name_server.ToString());
  }

  // set group name
  Napi::Value group_name = options.Get("groupName");
  if (group_name.IsString()) {
    producer_.set_group_name(group_name.ToString());
  }

  // set max message size
  Napi::Value max_message_size = options.Get("maxMessageSize");
  if (max_message_size.IsNumber()) {
    producer_.set_max_message_size(max_message_size.ToNumber());
  }

  // set compress level
  Napi::Value compress_level = options.Get("compressLevel");
  if (compress_level.IsNumber()) {
    producer_.set_compress_level(compress_level.ToNumber());
  }

  // set send message timeout
  Napi::Value send_message_timeout = options.Get("sendMessageTimeout");
  if (send_message_timeout.IsNumber()) {
    producer_.set_send_msg_timeout(send_message_timeout.ToNumber());
  }

  // 使用通用的日志配置函数
  utils::SetLoggerOptions(options);
}

Napi::Value RocketMQProducer::SetSessionCredentials(
    const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (lifecycle_state_.load() != RocketMQProducer::LifecycleState::kIdle) {
      Napi::Error::New(env, "Cannot set session credentials after producer has been started or destroyed")
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
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (lifecycle_state_.load() != RocketMQProducer::LifecycleState::kIdle) {
      Napi::Error::New(env, "Cannot set session credentials after producer has been started or destroyed")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }
    producer_.setRPCHook(rpc_hook);
  }

  return env.Undefined();
}

class ProducerStartWorker : public Napi::AsyncWorker {
 public:
  ProducerStartWorker(const Napi::Function& callback,
                      RocketMQProducer* wrapper)
      : Napi::AsyncWorker(callback),
        wrapper_ref_(Napi::Persistent(wrapper->Value())),
        producer_(&wrapper->producer_),
        wrapper_(wrapper) {}

  void Execute() override {
    // State was already checked and transitioned to kStarting in Start() API.
    // We only need to verify the producer hasn't been destroyed while we were queued.
    {
      std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
      RocketMQProducer::LifecycleState current_state = wrapper_->GetState();
      if (current_state == RocketMQProducer::LifecycleState::kDestroyed) {
        SetError("Producer has been destroyed");
        return;
      }
    }

    bool started = false;
    bool destroyed_after_start = false;
    std::string error;
    try {
      std::lock_guard<std::mutex> lock(wrapper_->native_access_mutex_);
      producer_->start();
      started = true;
    } catch (const std::exception& e) {
      error = e.what();
    } catch (...) {
      error = "Unknown error during producer start";
    }

    {
      std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
      if (wrapper_->lifecycle_state_.load() == RocketMQProducer::LifecycleState::kDestroyed) {
        destroyed_after_start = started;
      } else {
        if (started) {
          wrapper_->lifecycle_state_.store(RocketMQProducer::LifecycleState::kStarted);
          // Generation already incremented in CancelPendingSends during shutdown timeout.
          // New callbacks will capture the new generation and be isolated from old ones.
        } else {
          wrapper_->lifecycle_state_.store(RocketMQProducer::LifecycleState::kShutdown);
        }
      }
    }

    if (destroyed_after_start) {
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
            std::lock_guard<std::mutex> lock(wrapper_->native_access_mutex_);
            producer_->shutdown();
          } catch (const std::exception& e) {
            fprintf(stderr, "[RocketMQ] Warning: Producer shutdown failed after destroy during start: %s\n", e.what());
          } catch (...) {
            fprintf(stderr, "[RocketMQ] Warning: Unknown error during producer shutdown after destroy during start\n");
          }
        }
      }
      SetError("Producer has been destroyed");
      return;
    }

    if (!started) {
      SetError(error.empty() ? "Producer start failed" : error.c_str());
    }
  }

 private:
  Napi::ObjectReference wrapper_ref_;
  rocketmq::DefaultMQProducer* producer_;
  RocketMQProducer* wrapper_;
};

Napi::Value RocketMQProducer::Start(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // 使用通用的回调验证函数
  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  Napi::Function callback = info[0].As<Napi::Function>();

  // 在 JS API 边界进行状态检查和转换，避免 TOCTOU 竞态
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState current_state = lifecycle_state_.load();
    if (current_state == LifecycleState::kDestroyed) {
      Napi::Error::New(env, "Producer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShutdown) {
      Napi::Error::New(env, "Producer cannot be restarted after shutdown").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kStarted) {
      Napi::Error::New(env, "Producer is already started").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShuttingDown) {
      Napi::Error::New(env, "Producer is stopping, please wait for shutdown to complete").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kStarting) {
      Napi::Error::New(env, "Producer is already starting").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    // 原子地转换状态：Idle -> Starting
    lifecycle_state_.store(LifecycleState::kStarting);
  }

  auto* worker = new ProducerStartWorker(callback, this);
  try {
    worker->Queue();
  } catch (...) {
    // Queue failed - rollback state and clean up worker
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      lifecycle_state_.store(LifecycleState::kIdle);
    }
    delete worker;  // Clean up to prevent memory leak
    Napi::Error::New(env, "Failed to queue producer start operation").ThrowAsJavaScriptException();
    return env.Undefined();
  }
  return env.Undefined();
}

class ProducerShutdownWorker : public Napi::AsyncWorker {
 public:
  ProducerShutdownWorker(const Napi::Function& callback,
                         RocketMQProducer* wrapper)
      : Napi::AsyncWorker(callback),
        wrapper_ref_(Napi::Persistent(wrapper->Value())),
        producer_(&wrapper->producer_),
        wrapper_(wrapper) {}

  void Execute() override {
    // State was already checked and transitioned to kShuttingDown in Shutdown() API.
    // We only need to verify the producer hasn't been destroyed while we were queued.
    {
      std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
      RocketMQProducer::LifecycleState current_state = wrapper_->GetState();
      if (current_state == RocketMQProducer::LifecycleState::kDestroyed) {
        SetError("Producer has been destroyed");
        return;
      }
    }

    // Join the cancel timer thread started in Shutdown(). This thread handles
    // waiting for pending sends and cancelling on timeout, avoiding the libuv
    // thread pool delay race condition.
    wrapper_->JoinCancelTimerThread();

    bool shutdown_ok = false;
    std::string error;
    {
      bool should_shutdown = false;
      {
        std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
        if (!wrapper_->sdk_shutdown_called_) {
          should_shutdown = true;
        }
      }
      if (should_shutdown) {
        try {
          std::lock_guard<std::mutex> lock(wrapper_->native_access_mutex_);
          producer_->shutdown();
          shutdown_ok = true;
          {
            std::lock_guard<std::mutex> lock2(wrapper_->state_mutex_);
            wrapper_->sdk_shutdown_called_ = true;
          }
        } catch (const std::exception& e) {
          error = e.what();
        } catch (...) {
          error = "Unknown error during producer shutdown";
        }
      } else {
        // sdk_shutdown_called_ is true — a previous attempt already
        // succeeded at calling producer_->shutdown().
        shutdown_ok = true;
      }
    }

    // Delete orphaned callbacks unconditionally. After shutdown (success or
    // failure), the producer is in a terminal state and no new sends will
    // occur. ~ProducerSendCallback self-unregisters from the tracking list,
    // so any callback already auto-deleted by the SDK won't be in the list.
    RocketMQProducer::DeleteOrphanedCallbacks(wrapper_->pending_send_state_);

    if (shutdown_ok) {
      {
        std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
        if (wrapper_->lifecycle_state_.load() != RocketMQProducer::LifecycleState::kDestroyed) {
          wrapper_->lifecycle_state_.store(RocketMQProducer::LifecycleState::kShutdown);
        }
        wrapper_->shutdown_worker_active_ = false;
      }
    } else {
      fprintf(stderr, "[RocketMQ] Warning: Producer shutdown failed, orphan callbacks deleted\n");
      // Don't rollback to kStarted. CancelPendingSends may have already
      // poisoned the generation counter, making it unsafe to resume sending.
      // Keep kShuttingDown to prevent further operations on this producer.
      {
        std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
        wrapper_->shutdown_worker_active_ = false;
      }
      SetError(error.empty() ? "Producer shutdown failed" : error.c_str());
    }
  }

 private:
  Napi::ObjectReference wrapper_ref_;
  rocketmq::DefaultMQProducer* producer_;
  RocketMQProducer* wrapper_;
};

Napi::Value RocketMQProducer::Shutdown(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // 使用通用的回调验证函数
  if (!utils::ValidateCallback(info, 0, "Function expected as first argument")) {
    return env.Undefined();
  }

  Napi::Function callback = info[0].As<Napi::Function>();

  // 在 JS API 边界进行状态检查和转换，避免 TOCTOU 竞态
  bool is_retry = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    LifecycleState current_state = lifecycle_state_.load();
    if (current_state == LifecycleState::kDestroyed) {
      Napi::Error::New(env, "Producer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShutdown) {
      Napi::Error::New(env, "Producer is already stopped").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_state == LifecycleState::kShuttingDown) {
      if (shutdown_worker_active_) {
        Napi::Error::New(env, "Producer is already shutting down").ThrowAsJavaScriptException();
        return env.Undefined();
      }
      // Allow retry — previous shutdown failed, native kept kShuttingDown
      is_retry = true;
    } else if (current_state != LifecycleState::kStarted) {
      Napi::Error::New(env, "Producer is not started").ThrowAsJavaScriptException();
      return env.Undefined();
    } else {
      // 原子地转换状态：Started -> ShuttingDown
      lifecycle_state_.store(LifecycleState::kShuttingDown);
    }
  }

  if (is_retry) {
    // Retry path: join leftover cancel timer from previous attempt.
    // Pending sends were already cancelled by the first attempt.
    JoinCancelTimerThread();
  } else {
    // First shutdown: start cancel timer thread immediately to avoid libuv
    // thread pool delay. This ensures the shutdown timeout starts counting
    // from now, not from when the AsyncWorker gets a thread from the pool.
    auto pending_send_state = pending_send_state_;
    auto timeout = PendingSendWaitTimeout();
    std::lock_guard<std::mutex> lock(cancel_timer_mutex_);
    try {
      cancel_timer_thread_ = std::make_unique<std::thread>(
          [pending_send_state, timeout]() {
            bool wait_success = WaitForPendingSends(pending_send_state, timeout);
            if (!wait_success) {
              fprintf(stderr,
                      "[RocketMQ] Warning: Timed out waiting for pending send "
                      "callbacks during shutdown\n");
              CancelPendingSends(pending_send_state);
            }
          });
    } catch (const std::system_error&) {
      lifecycle_state_.store(LifecycleState::kStarted);
      Napi::Error::New(env, "Failed to create shutdown timer thread")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }
  }

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    shutdown_worker_active_ = true;
  }

  auto* worker = new ProducerShutdownWorker(callback, this);
  try {
    worker->Queue();
  } catch (...) {
    // Queue failed - don't rollback state since CancelPendingSends
    // poisons the generation counter. Keep kShuttingDown.
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      shutdown_worker_active_ = false;
    }
    delete worker;  // Clean up to prevent memory leak
    CancelPendingSends(pending_send_state_);
    JoinCancelTimerThread();
    Napi::Error::New(env, "Failed to queue producer shutdown operation").ThrowAsJavaScriptException();
    return env.Undefined();
  }
  return env.Undefined();
}

class ProducerSendCallback : public rocketmq::SendCallback {
 public:
  struct CallbackData {
    std::unique_ptr<rocketmq::SendResult> result;
    std::exception_ptr exception;
  };

 private:
  struct CleanupContext {
    explicit CleanupContext(
        std::shared_ptr<RocketMQProducer::PendingSendState> pending_send_state_in,
        uint64_t generation_in,
        std::unique_ptr<Napi::ObjectReference> prevent_gc_in)
        : pending_send_state(std::move(pending_send_state_in)),
          generation(generation_in),
          prevent_gc(std::move(prevent_gc_in)) {}
    std::shared_ptr<RocketMQProducer::PendingSendState> pending_send_state;
    uint64_t generation;  // Captured at send time for lifecycle isolation
    std::unique_ptr<CallbackData> pending;
    std::string delivery_error;
    Napi::FunctionReference callback_ref;
    std::atomic<bool> pending_send_released{false};
    // Mutex protects mutable fields (prevent_gc, pending, delivery_error, callback_ref)
    // against concurrent access from ScheduleCallback (SDK thread) and
    // Finalize (main thread) when CancelPendingSends aborts the TSFN.
    std::mutex ctx_mutex;
    std::unique_ptr<Napi::ObjectReference> prevent_gc;  // Pins Producer JS object
  };

  // Prevent-GC reference holder passed as TSFN finalize data.
  // Ensures CleanupContext outlives the TSFN even if ProducerSendCallback
  // is deleted by the SDK before Finalize fires.
  struct ContextRef {
    std::shared_ptr<CleanupContext> ctx;
  };

  static void ReleasePendingSend(CleanupContext* ctx) {
    if (ctx != nullptr && !ctx->pending_send_released.exchange(true)) {
      RocketMQProducer::ReleasePendingSend(ctx->pending_send_state);
    }
  }

  static void ResetCallbackReference(CleanupContext* ctx) {
    if (ctx != nullptr && !ctx->callback_ref.IsEmpty()) {
      ctx->callback_ref.Reset();
    }
  }

 public:
  ProducerSendCallback(RocketMQProducer* owner,
                       Napi::Env env,
                       Napi::ObjectReference&& producer_ref,
                       Napi::Function&& callback,
                       uint64_t generation)
      : cleanup_ctx_(),
        callback_(),
        tsfn_released_(std::make_shared<std::atomic<bool>>(false)),
        callback_scheduled_(false) {
    auto prevent_gc = std::make_unique<Napi::ObjectReference>(std::move(producer_ref));
    cleanup_ctx_ = std::make_shared<CleanupContext>(
        owner->pending_send_state_, generation, std::move(prevent_gc));
    cleanup_ctx_->callback_ref = Napi::Persistent(callback);

    auto ctx_ref = std::make_unique<ContextRef>(ContextRef{cleanup_ctx_});
    callback_ = Callback::New(env,
                              callback,
                              "RocketMQ Send Callback",
                              0,
                              1,
                              cleanup_ctx_.get(),
                              &Finalize,
                              ctx_ref.get());
    // TSFN created successfully — it owns ctx_ref now
    ctx_ref.release();

    // Register TSFN handle for fallback abort in CancelPendingSends
    {
      std::lock_guard<std::mutex> lock(owner->pending_send_state_->mutex);
      auto& handles = owner->pending_send_state_->pending_tsfn_handles;
      handles.erase(
          std::remove_if(handles.begin(), handles.end(),
                         [](const RocketMQProducer::PendingSendState::PendingTSFNHandle& h) {
                           return h.released->load();
                         }),
          handles.end());
      handles.push_back(
          {static_cast<napi_threadsafe_function>(callback_), tsfn_released_});
      owner->pending_send_state_->pending_callbacks.push_back(this);
    }
  }

  ~ProducerSendCallback() {
    // Unregister from orphan tracking
    {
      auto& ps = cleanup_ctx_->pending_send_state;
      if (ps) {
        std::lock_guard<std::mutex> lock(ps->mutex);
        auto& cbs = ps->pending_callbacks;
        cbs.erase(std::remove(cbs.begin(), cbs.end(), this), cbs.end());
      }
    }
    if (!tsfn_released_->exchange(true)) {
      napi_status status = callback_.Abort();
      if (status != napi_ok) {
        fprintf(stderr, "[RocketMQ] Warning: TSFN Abort failed: %d\n", static_cast<int>(status));
      }
    }
  }

  void onSuccess(rocketmq::SendResult& send_result) override {
    ScheduleCallback(std::unique_ptr<rocketmq::SendResult>(
                         new rocketmq::SendResult(send_result)),
                     nullptr);
  }

  void onException(rocketmq::MQException& exception) noexcept override {
    ScheduleCallback(nullptr, std::make_exception_ptr(exception));
  }

  void ReleasePendingSendOnce() {
    ReleasePendingSend(cleanup_ctx_.get());
  }

 private:
  void ScheduleCallback(std::unique_ptr<rocketmq::SendResult> result,
                        std::exception_ptr exception) {
    // Prevent multiple callback scheduling
    if (callback_scheduled_.exchange(true)) {
      fprintf(stderr, "[RocketMQ] Warning: Callback already scheduled, ignoring duplicate\n");
      return;
    }

    // Atomically claim TSFN ownership. If CancelPendingSends or the destructor
    // already claimed it (exchange returns true), bail — the handle may be freed.
    if (tsfn_released_->exchange(true)) {
      return;
    }

    auto* data = new CallbackData{
        std::move(result),
        std::move(exception)
    };

    napi_status status = napi_ok;
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    if (IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_BLOCKING_FAIL")) {
      status = napi_generic_failure;
    } else {
      status = callback_.BlockingCall(data);
    }
#else
    status = callback_.BlockingCall(data);
#endif
    if (status != napi_ok) {
      fprintf(stderr, "[RocketMQ] Failed to schedule JavaScript callback: %d\n", status);
      {
        std::lock_guard<std::mutex> lock(cleanup_ctx_->ctx_mutex);
        cleanup_ctx_->pending.reset(data);
        cleanup_ctx_->delivery_error = "Failed to schedule JavaScript callback";
      }
      // Decrement pending count immediately so WaitForPendingSends won't deadlock.
      // pending_send_released flag ensures idempotency with Finalize.
      ReleasePendingSend(cleanup_ctx_.get());
    }

    // TSFN ownership was claimed above — release unconditionally.
    napi_status release_status = callback_.Release();
    if (release_status != napi_ok) {
      fprintf(stderr, "[RocketMQ] Warning: TSFN Release failed: %d\n", static_cast<int>(release_status));
      if (status != napi_ok) {
        ReleasePendingSend(cleanup_ctx_.get());
        // Do NOT call ResetCallbackReference here — we are on the SDK thread,
        // and Napi::Reference must only be touched on the main JS thread.
        // The reference will be cleaned up during environment teardown.
      }
    }

    // Remove completed handles from tracking vector to prevent unbounded growth.
    {
      auto& pending = cleanup_ctx_->pending_send_state;
      std::lock_guard<std::mutex> tsfn_lock(pending->mutex);
      auto& handles = pending->pending_tsfn_handles;
      handles.erase(
          std::remove_if(handles.begin(), handles.end(),
                         [](const RocketMQProducer::PendingSendState::PendingTSFNHandle& h) {
                           return h.released->load();
                         }),
          handles.end());
    }
  }

  static void CallJs(Napi::Env env, Napi::Function callback, CleanupContext* ctx, CallbackData* data) {
    std::unique_ptr<CallbackData> guard(data);
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    if (env == nullptr || callback == nullptr || IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_CALLJS_NULL_ENV")) {
#else
    if (env == nullptr || callback == nullptr) {
#endif
      ReleasePendingSend(ctx);
      return;
    }

    Napi::HandleScope scope(env);

    if (ctx->generation !=
        RocketMQProducer::GetCurrentGeneration(ctx->pending_send_state)) {
      try {
        callback.Call(env.Global(),
                      {Napi::Error::New(env, "Send cancelled: producer shutdown timeout").Value()});
      } catch (const Napi::Error& e) {
        ReleasePendingSend(ctx);
        ResetCallbackReference(ctx);
        utils::ThrowViaMicrotask(env, e.Value());
        return;
      }
      ReleasePendingSend(ctx);
      ResetCallbackReference(ctx);
      return;
    }

    try {
      if (data->exception) {
        try {
          std::rethrow_exception(data->exception);
        } catch (const std::exception& e) {
          callback.Call(env.Global(), {Napi::Error::New(env, e.what()).Value()});
        } catch (...) {
          callback.Call(env.Global(), {Napi::Error::New(env, "Unknown error during send").Value()});
        }
      } else {
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
        if (IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_CALLJS_THROW")) {
          throw Napi::Error::New(env, "producer calljs throw");
        }
#endif
        callback.Call(env.Global(),
                      {env.Undefined(),
                       Napi::Number::New(env, data->result->send_status()),
                       Napi::String::New(env, data->result->msg_id()),
                       Napi::Number::New(env, data->result->queue_offset())});
      }
    } catch (const Napi::Error& e) {
      ReleasePendingSend(ctx);
      ResetCallbackReference(ctx);
      utils::ThrowViaMicrotask(env, e.Value());
      return;
    } catch (const std::exception& e) {
      fprintf(stderr, "[RocketMQ] Warning: Error in send callback: %s\n", e.what());
    } catch (...) {
      fprintf(stderr, "[RocketMQ] Warning: Unknown error in send callback\n");
    }
    ReleasePendingSend(ctx);
    ResetCallbackReference(ctx);
  }

  static void Finalize(Napi::Env env, ContextRef* ref, CleanupContext* ctx) {
    // ContextRef ensures CleanupContext is alive for this call even if
    // ProducerSendCallback was already deleted by the SDK.
    std::unique_ptr<ContextRef> ref_guard(ref);
    if (ctx == nullptr) {
      return;
    }

    // During env teardown, Node-API passes env=nullptr. Calling
    // napi_delete_reference on a dead env crashes, so we must suppress
    // destructor side-effects and let Node reclaim the handles itself.
    const bool env_teardown = (env == nullptr);

    std::string delivery_err;
    {
      std::lock_guard<std::mutex> lock(ctx->ctx_mutex);
      if (ctx->pending && !ctx->delivery_error.empty()) {
        fprintf(stderr, "[RocketMQ] Warning: Dropped send callback due to: %s\n",
                ctx->delivery_error.c_str());
      }
      ctx->pending.reset();
      delivery_err = ctx->delivery_error;
      if (env_teardown && ctx->prevent_gc) {
        ctx->prevent_gc->SuppressDestruct();
      }
      ctx->prevent_gc.reset();
    }

    // When BlockingCall failed, invoke the JS callback with the error so the
    // JS-layer pendingSendSettler is rejected immediately instead of hanging
    // until shutdown DRAIN.
    if (!env_teardown && !delivery_err.empty() && !ctx->callback_ref.IsEmpty()) {
      try {
        Napi::HandleScope scope(env);
        ctx->callback_ref.Value().Call(
            env.Global(),
            {Napi::Error::New(env, delivery_err).Value()});
      } catch (const Napi::Error& e) {
        utils::ThrowViaMicrotask(env, e.Value());
      } catch (...) {
        fprintf(stderr, "[RocketMQ] Warning: Unknown error invoking send callback in Finalize\n");
      }
    }

    ReleasePendingSend(ctx);
    if (env_teardown) {
      if (!ctx->callback_ref.IsEmpty()) {
        ctx->callback_ref.SuppressDestruct();
      }
    } else {
      ResetCallbackReference(ctx);
    }
  }

  using Callback = Napi::TypedThreadSafeFunction<CleanupContext,
                                                 CallbackData,
                                                 &CallJs>;

  std::shared_ptr<CleanupContext> cleanup_ctx_;
  Callback callback_;
  std::shared_ptr<std::atomic<bool>> tsfn_released_;
  std::atomic<bool> callback_scheduled_;
};

void RocketMQProducer::DeleteOrphanedCallbacks(
    const std::shared_ptr<PendingSendState>& pending_send_state) {
  if (pending_send_state == nullptr) {
    return;
  }
  std::vector<ProducerSendCallback*> orphans;
  {
    std::lock_guard<std::mutex> lock(pending_send_state->mutex);
    orphans = std::move(pending_send_state->pending_callbacks);
  }
  for (auto* cb : orphans) {
    delete cb;
  }
}

Napi::Value RocketMQProducer::Send(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  // Check if required parameters are provided FIRST (before state checks)
  if (info.Length() < 4) {
    Napi::TypeError::New(env, "Wrong number of arguments").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (!info[0].IsString()) {
    Napi::TypeError::New(env, "Topic must be a string").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (!info[1].IsString() && !info[1].IsBuffer()) {
    Napi::TypeError::New(env, "Message body must be a string or buffer").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (!info[3].IsFunction()) {
    Napi::TypeError::New(env, "Callback must be a function").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Atomically check state AND increment pending count using std::lock
  // This prevents TOCTOU race and ensures consistent synchronization
  uint64_t generation = 0;
  {
    std::lock(state_mutex_, pending_send_state_->mutex);
    std::lock_guard<std::mutex> state_lock(state_mutex_, std::adopt_lock);
    std::lock_guard<std::mutex> pending_lock(pending_send_state_->mutex, std::adopt_lock);

    RocketMQProducer::LifecycleState current_state = GetState();
    if (current_state == RocketMQProducer::LifecycleState::kDestroyed) {
      Napi::Error::New(env, "Producer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    if (current_state == RocketMQProducer::LifecycleState::kShuttingDown) {
      Napi::Error::New(env, "Producer is shutting down").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    if (current_state != RocketMQProducer::LifecycleState::kStarted) {
      Napi::Error::New(env, "Producer is not started").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    // Increment pending count while holding both locks
    // This ensures shutdown cannot observe pending=0 and proceed while we're about to send
    generation = pending_send_state_->generation.load();
    pending_send_state_->pending_send_callbacks++;
  }

  // Build message and send - no locks held during native call
  // pending_send_callbacks has been incremented, so shutdown will wait for this send
  std::unique_ptr<ProducerSendCallback> send_callback;
  try {
    Napi::String topic = info[0].As<Napi::String>();
    rocketmq::MQMessage message;
    if (info[1].IsString()) {
      message = rocketmq::MQMessage(topic, info[1].ToString());
    } else {
      Napi::Buffer<char> buffer = info[1].As<Napi::Buffer<char>>();
      message = rocketmq::MQMessage(topic, std::string(buffer.Data(), buffer.Length()));
    }

    const Napi::Value options_v = info[2];
    if (options_v.IsObject()) {
      const Napi::Object options = options_v.ToObject();

      Napi::Value tags = options.Get("tags");
      if (tags.IsString()) {
        message.set_tags(tags.ToString());
      }

      Napi::Value keys = options.Get("keys");
      if (keys.IsString()) {
        message.set_keys(keys.ToString());
      }
    }

    send_callback.reset(
        new ProducerSendCallback(this,
                                 env,
                                 Napi::Persistent(Value()),
                                 info[3].As<Napi::Function>(),
                                 generation));

    auto* raw_callback = send_callback.get();

    // Guard against cancel-after-increment race: if CancelPendingSends ran between
    // our pending_send_callbacks++ and TSFN creation, the generation will have changed.
    // send_callback destructor will Abort the TSFN and trigger cleanup via Finalize.
    if (generation != GetCurrentGeneration(pending_send_state_)) {
      Napi::Error::New(env, "Send cancelled: producer shutdown").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    {
      std::lock_guard<std::mutex> lock(native_access_mutex_);
      producer_.send(message, raw_callback);
    }
    send_callback.release();
    // Note: pending_send_callbacks will be released by callback's CallJs/Finalize
  } catch (const std::exception& e) {
    // On synchronous exception, the callback object may or may not have been constructed.
    // If it was, its destructor will Abort() the TSFN, and Finalize will release pending.
    // If it wasn't, we need to release pending here.
    // The CleanupContext's pending_send_released flag handles this race safely.
    if (send_callback) {
      // Callback was constructed - it owns the pending count now
      // Its Finalize will release it (via Abort path)
    } else {
      // Callback was not constructed - we still own the pending count
      ReleasePendingSend(pending_send_state_);
    }
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
  } catch (...) {
    if (send_callback) {
      // Callback was constructed - it owns the pending count now
    } else {
      // Callback was not constructed - we still own the pending count
      ReleasePendingSend(pending_send_state_);
    }
    Napi::Error::New(env, "Unknown error during producer send").ThrowAsJavaScriptException();
  }

  return env.Undefined();
}

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
Napi::Value RocketMQProducer::ForceDestroyForTest(const Napi::CallbackInfo& info) {
  SafeShutdown(false);  // Do not wait for pending sends in test force destroy
  return info.Env().Undefined();
}

Napi::Value RocketMQProducer::SimulateShutdownTimeoutForTest(const Napi::CallbackInfo& info) {
  // Simulate what happens when shutdown times out:
  // Set cancelled flag without changing lifecycle state
  CancelPendingSends(pending_send_state_);
  return info.Env().Undefined();
}
#endif

}  // namespace __node_rocketmq__
