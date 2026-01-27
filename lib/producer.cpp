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

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <string>

#include <napi.h>

#include <ClientRPCHook.h>
#include <LoggerConfig.h>
#include <MQException.h>
#include <MQMessage.h>
#include <SendCallback.h>

#include "addon_data.h"
#include "common_utils.h"

namespace __node_rocketmq__ {

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
                  });

  addon_data->producer_constructor = Napi::Persistent(func);

  exports.Set("Producer", func);
  return exports;
}

RocketMQProducer::RocketMQProducer(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<RocketMQProducer>(info), producer_("") {
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
  SafeShutdown();
}

void RocketMQProducer::SafeShutdown() {
  std::lock_guard<std::mutex> lock(state_mutex_);
  
  if (is_destroyed_.exchange(true)) {
    return; // Already destroyed
  }
  
  if (is_started_.load() && !is_shutting_down_.exchange(true)) {
    try {
      producer_.shutdown();
    } catch (const std::exception& e) {
      // Log error but don't throw in destructor
      fprintf(stderr, "[RocketMQ] Warning: Producer shutdown failed in destructor: %s\n", e.what());
    } catch (...) {
      fprintf(stderr, "[RocketMQ] Warning: Unknown error during producer shutdown in destructor\n");
    }
  }
  
  is_started_.store(false);
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

  // 使用通用的参数验证函数
  if (!utils::ValidateStringArguments(info, 3, "All arguments must be strings")) {
    return env.Undefined();
  }

  Napi::String access_key = info[0].As<Napi::String>();
  Napi::String secret_key = info[1].As<Napi::String>();
  Napi::String ons_channel = info[2].As<Napi::String>();

  auto rpc_hook = std::make_shared<rocketmq::ClientRPCHook>(
      rocketmq::SessionCredentials(access_key, secret_key, ons_channel));
  producer_.setRPCHook(rpc_hook);

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
    // 在整个操作期间持有锁以避免竞态条件
    std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
    
    if (wrapper_->is_destroyed_.load()) {
      SetError("Producer has been destroyed");
      return;
    }
    
    if (wrapper_->is_started_.load()) {
      SetError("Producer is already started");
      return;
    }
    
    if (wrapper_->is_shutting_down_.load()) {
      SetError("Producer is shutting down");
      return;
    }
    
    try {
      producer_->start();
      wrapper_->is_started_.store(true);
    } catch (const std::exception& e) {
      SetError(e.what());
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

  auto* worker = new ProducerStartWorker(callback, this);
  worker->Queue();
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
    // 在整个操作期间持有锁以避免竞态条件
    std::lock_guard<std::mutex> lock(wrapper_->state_mutex_);
    
    if (wrapper_->is_destroyed_.load()) {
      SetError("Producer has been destroyed");
      return;
    }
    
    if (!wrapper_->is_started_.load()) {
      SetError("Producer is not started");
      return;
    }
    
    if (wrapper_->is_shutting_down_.exchange(true)) {
      SetError("Producer is already shutting down");
      return;
    }
    
    try {
      producer_->shutdown();
      wrapper_->is_started_.store(false);
      wrapper_->is_shutting_down_.store(false); // Reset shutdown flag after successful shutdown
    } catch (const std::exception& e) {
      wrapper_->is_shutting_down_.store(false); // Reset on error
      SetError(e.what());
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

  auto* worker = new ProducerShutdownWorker(callback, this);
  worker->Queue();
  return env.Undefined();
}

class ProducerSendCallback : public rocketmq::AutoDeleteSendCallback {
 public:
  struct CallbackData {
    std::unique_ptr<rocketmq::SendResult> result;
    std::exception_ptr exception;
    std::unique_ptr<Napi::ObjectReference> prevent_gc;
  };

 private:
  struct CleanupContext {
    std::unique_ptr<CallbackData> pending;
  };

 public:
  ProducerSendCallback(Napi::Env env, Napi::ObjectReference&& producer_ref, Napi::Function&& callback)
      : prevent_gc_(new Napi::ObjectReference(std::move(producer_ref))),
        cleanup_ctx_(nullptr),
        callback_(),
        prevent_prevent_release_(false),
        callback_scheduled_(false),
        callback_completed_(false) {
    std::unique_ptr<CleanupContext> ctx(new CleanupContext());
    try {
      callback_ = Callback::New(env,
                                callback,
                                "RocketMQ Send Callback",
                                0,
                                1,
                                ctx.get(),
                                &Finalize,
                                static_cast<void*>(nullptr));
      // 只有在 Callback::New 成功后才释放所有权
      cleanup_ctx_ = ctx.release();
    } catch (...) {
      // 如果 Callback::New 失败，智能指针会自动清理
      throw;
    }
  }

  ~ProducerSendCallback() {
    if (!prevent_prevent_release_.exchange(true)) {
      try {
        callback_.Abort();
      } catch (const std::exception& e) {
        fprintf(stderr, "[RocketMQ] Warning: Error aborting send callback: %s\n", e.what());
      } catch (...) {
        fprintf(stderr, "[RocketMQ] Warning: Unknown error aborting send callback\n");
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

 private:
  void ScheduleCallback(std::unique_ptr<rocketmq::SendResult> result,
                        std::exception_ptr exception) {
    // Prevent multiple callback scheduling
    if (callback_scheduled_.exchange(true)) {
      fprintf(stderr, "[RocketMQ] Warning: Callback already scheduled, ignoring duplicate\n");
      return;
    }
    
    auto prevent_gc = std::move(prevent_gc_);
    auto* data = new CallbackData{
        std::move(result),
        std::move(exception),
        std::move(prevent_gc)
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
      cleanup_ctx_->pending.reset(data);
    } else {
      callback_completed_.store(true);
    }

    if (!prevent_prevent_release_.exchange(true)) {
      try {
        callback_.Release();
      } catch (const std::exception& e) {
        fprintf(stderr, "[RocketMQ] Warning: Error releasing send callback: %s\n", e.what());
      } catch (...) {
        fprintf(stderr, "[RocketMQ] Warning: Unknown error releasing send callback\n");
      }
    }
  }

  static void CallJs(Napi::Env env, Napi::Function callback, CleanupContext*, CallbackData* data) {
    std::unique_ptr<CallbackData> guard(data);
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
    if (env == nullptr || callback == nullptr || IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_CALLJS_NULL_ENV")) {
#else
    if (env == nullptr || callback == nullptr) {
#endif
      return;
    }

    Napi::HandleScope scope(env);
    try {
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
      if (IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_CALLJS_THROW")) {
        throw Napi::Error::New(env, "producer calljs throw");
      }
#endif
      if (data->exception) {
        try {
          std::rethrow_exception(data->exception);
        } catch (const std::exception& e) {
          callback.Call(env.Global(),
                        {Napi::Error::New(env, e.what()).Value()});
        }
      } else {
        callback.Call(env.Global(),
                      {env.Undefined(),
                       Napi::Number::New(env, data->result->send_status()),
                       Napi::String::New(env, data->result->msg_id()),
                       Napi::Number::New(env, data->result->queue_offset())});
      }
    } catch (const Napi::Error& e) {
      e.ThrowAsJavaScriptException();
    } catch (const std::exception& e) {
      fprintf(stderr, "[RocketMQ] Warning: Exception in send callback: %s\n", e.what());
    } catch (...) {
      fprintf(stderr, "[RocketMQ] Warning: Unknown exception in send callback\n");
    }
  }

  static void Finalize(Napi::Env, void*, CleanupContext* ctx) {
    delete ctx;
  }

  using Callback = Napi::TypedThreadSafeFunction<CleanupContext,
                                                 CallbackData,
                                                 &CallJs>;

  std::unique_ptr<Napi::ObjectReference> prevent_gc_;
  CleanupContext* cleanup_ctx_;
  Callback callback_;
  std::atomic<bool> prevent_prevent_release_;
  std::atomic<bool> callback_scheduled_;
  std::atomic<bool> callback_completed_;
};

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

  if (!info[3].IsFunction()) {
    Napi::TypeError::New(env, "Callback must be a function").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Check if producer is in valid state AFTER parameter validation
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (is_destroyed_.load()) {
      Napi::Error::New(env, "Producer has been destroyed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    
    if (!is_started_.load()) {
      Napi::Error::New(env, "Producer is not started").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    
    if (is_shutting_down_.load()) {
      Napi::Error::New(env, "Producer is shutting down").ThrowAsJavaScriptException();
      return env.Undefined();
    }
  }

  rocketmq::MQMessage message = [&]() {
    Napi::String topic = info[0].As<Napi::String>();
    Napi::Value body = info[1];
    if (body.IsString()) {
      return rocketmq::MQMessage(topic, body.ToString());
    } else if (body.IsBuffer()) {
      Napi::Buffer<char> buffer = body.As<Napi::Buffer<char>>();
      return rocketmq::MQMessage(topic,
                                 std::string(buffer.Data(), buffer.Length()));
    } else {
      Napi::TypeError::New(env, "Message body must be a string or buffer").ThrowAsJavaScriptException();
      return rocketmq::MQMessage("", "");
    }
  }();

  // If there was an error creating the message, return early
  if (message.topic().empty() && message.body().empty()) {
    return env.Undefined();
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

  // 使用智能指针管理回调对象的生命周期
  std::unique_ptr<ProducerSendCallback> send_callback(
      new ProducerSendCallback(env, Napi::Persistent(Value()), info[3].As<Napi::Function>()));
  
  // 先获取原始指针，但保持智能指针的所有权
  auto* raw_callback = send_callback.get();
  try {
    producer_.send(message, raw_callback);
    // 只有在 send() 成功后才释放所有权
    send_callback.release();
  } catch (const std::exception& e) {
    // 失败时智能指针自动清理，无内存泄漏
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return env.Undefined();
}

}  // namespace __node_rocketmq__
