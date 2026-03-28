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
#ifndef __ROCKETMQ_PRODUCER_H__
#define __ROCKETMQ_PRODUCER_H__

#include <napi.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <DefaultMQProducer.h>

namespace __node_rocketmq__ {

struct AddonData;
class ProducerStartWorker;
class ProducerShutdownWorker;
class ProducerSendCallback;

class RocketMQProducer : public Napi::ObjectWrap<RocketMQProducer> {
  friend class ProducerStartWorker;
  friend class ProducerShutdownWorker;
  friend class ProducerSendCallback;
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports, AddonData* addon_data);

  RocketMQProducer(const Napi::CallbackInfo& info);
  ~RocketMQProducer();

  enum class LifecycleState : uint8_t {
    kIdle = 0,
    kStarting,
    kStarted,
    kShuttingDown,
    kShutdown,
    kDestroyed
  };

 private:
  struct PendingSendState {
    std::mutex mutex;
    std::condition_variable cv;
    size_t pending_send_callbacks{0};
    std::atomic<uint64_t> generation{0};  // Monotonic generation counter for lifecycle isolation

    // Track TSFN handles for force-abort in CancelPendingSends.
    // When shutdown times out and SDK never calls back, aborting the TSFN
    // triggers Finalize which releases pinned JS objects (prevent_gc, callback_ref).
    struct PendingTSFNHandle {
      napi_threadsafe_function tsfn;
      std::shared_ptr<std::atomic<bool>> released;
    };
    std::vector<PendingTSFNHandle> pending_tsfn_handles;

    // Track raw SendCallback pointers for orphan cleanup after SDK shutdown.
    // When the SDK never calls back (and thus never auto-deletes), these
    // are cleaned up in CleanupOrphanedCallbacks after producer_->shutdown().
    std::vector<ProducerSendCallback*> pending_callbacks;
  };

  Napi::Value SetSessionCredentials(const Napi::CallbackInfo& info);

  Napi::Value Start(const Napi::CallbackInfo& info);
  Napi::Value Shutdown(const Napi::CallbackInfo& info);

  Napi::Value Send(const Napi::CallbackInfo& info);
#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
  Napi::Value ForceDestroyForTest(const Napi::CallbackInfo& info);
  Napi::Value SimulateShutdownTimeoutForTest(const Napi::CallbackInfo& info);
#endif

 private:
  void SetOptions(const Napi::Object& options);
  void SafeShutdown(bool wait_for_pending_sends = true);
  static bool WaitForPendingSends(
      const std::shared_ptr<PendingSendState>& pending_send_state,
      std::chrono::milliseconds timeout = std::chrono::seconds(30));
  static void ReleasePendingSend(const std::shared_ptr<PendingSendState>& pending_send_state);
  static void CancelPendingSends(const std::shared_ptr<PendingSendState>& pending_send_state);
  static void DeleteOrphanedCallbacks(const std::shared_ptr<PendingSendState>& pending_send_state);
  static uint64_t GetCurrentGeneration(const std::shared_ptr<PendingSendState>& pending_send_state);

  bool TryTransitionState(LifecycleState expected, LifecycleState desired);
  LifecycleState GetState() const { return lifecycle_state_.load(); }
  void JoinCancelTimerThread();

 private:
  std::atomic<LifecycleState> lifecycle_state_{LifecycleState::kIdle};
  mutable std::mutex state_mutex_;
  mutable std::mutex native_access_mutex_;  // Serializes native SDK calls (send/start/shutdown)
  bool shutdown_worker_active_{false};  // Protected by state_mutex_
  bool sdk_shutdown_called_{false};     // Protected by state_mutex_; ensures producer_.shutdown() once
  std::shared_ptr<PendingSendState> pending_send_state_;
  std::unique_ptr<std::thread> cancel_timer_thread_;
  std::mutex cancel_timer_mutex_;
  rocketmq::DefaultMQProducer producer_;
};

}  // namespace __node_rocketmq__

#endif
