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
#include <napi.h>

#if (defined(__GLIBC__) || defined(__APPLE__)) && !defined(_WIN32)
#define ROCKETMQ_HAS_EXECINFO 1
#endif

#if ROCKETMQ_HAS_EXECINFO
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <execinfo.h>
#include <unistd.h>
#endif

#include "addon_data.h"
#include "consumer_ack.h"
#include "producer.h"
#include "push_consumer.h"
#include "common_utils.h"

namespace __node_rocketmq__ {

static void DeleteAddonData(Napi::Env, AddonData* data) {
  delete data;
}

AddonData* GetAddonData(Napi::Env env) {
  return env.GetInstanceData<AddonData>();
}

namespace {
#if ROCKETMQ_HAS_EXECINFO && !defined(ROCKETMQ_COVERAGE)
void CrashSignalHandler(int signo) {
  void* frames[config::MAX_BACKTRACE_FRAMES];
  const int count = backtrace(frames, config::MAX_BACKTRACE_FRAMES);
  fprintf(stderr, "[rocketmq-addon] signal %d\n", signo);
  backtrace_symbols_fd(frames, count, STDERR_FILENO);
  std::signal(signo, SIG_DFL);
  std::raise(signo);
}

void MaybeInstallCrashHandler() {
  static bool installed = false;
  if (installed) {
    return;
  }
  const char* flag = std::getenv("ROCKETMQ_DEBUG_STACK");
  if (flag == nullptr || flag[0] == '\0') {
    return;
  }
  installed = true;
  std::signal(SIGSEGV, CrashSignalHandler);
  std::signal(SIGABRT, CrashSignalHandler);
}
#else
inline void MaybeInstallCrashHandler() {}
#endif
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
  MaybeInstallCrashHandler();

  auto* addon_data = new AddonData();
  env.SetInstanceData<AddonData, DeleteAddonData>(addon_data);

  RocketMQProducer::Init(env, exports, addon_data);
  RocketMQPushConsumer::Init(env, exports, addon_data);
  ConsumerAck::Init(env, exports, addon_data);
  return exports;
}

NODE_API_MODULE(rocketmq, Init)

}  // namespace __node_rocketmq__
