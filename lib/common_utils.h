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
#ifndef __ROCKETMQ_COMMON_UTILS_H__
#define __ROCKETMQ_COMMON_UTILS_H__

#include <cstdlib>
#include <chrono>
#include <napi.h>

namespace __node_rocketmq__ {

#if defined(ROCKETMQ_COVERAGE) || defined(ROCKETMQ_USE_STUB)
inline bool IsEnvEnabled(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return false;
  }
  return value[0] != '\0' && value[0] != '0';
}
#endif

// 配置常量
namespace config {
  constexpr std::chrono::seconds DEFAULT_MESSAGE_TIMEOUT{30};
  constexpr int MAX_BACKTRACE_FRAMES = 64;
}

// 通用工具函数
namespace utils {
  // 设置日志配置的通用函数
  void SetLoggerOptions(const Napi::Object& options);

  // 参数验证辅助函数
  bool ValidateStringArguments(const Napi::CallbackInfo& info, size_t count, const char* error_msg);
  bool ValidateCallback(const Napi::CallbackInfo& info, size_t index, const char* error_msg);

  inline void ThrowViaMicrotask(Napi::Env env, napi_value error) {
    napi_ref err_ref = nullptr;
    napi_status status = napi_create_reference(env, error, 1, &err_ref);
    if (status != napi_ok) {
      return;
    }

    napi_value global = nullptr;
    status = napi_get_global(env, &global);
    if (status != napi_ok) {
      napi_delete_reference(env, err_ref);
      return;
    }

    napi_value queue_microtask = nullptr;
    status = napi_get_named_property(env, global, "queueMicrotask", &queue_microtask);
    if (status != napi_ok) {
      napi_delete_reference(env, err_ref);
      return;
    }

    napi_value throw_fn = nullptr;
    status = napi_create_function(env, nullptr, 0,
        [](napi_env env, napi_callback_info info) -> napi_value {
          void* data;
          napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
          auto ref = static_cast<napi_ref>(data);
          napi_value err = nullptr;
          napi_status s = napi_get_reference_value(env, ref, &err);
          napi_delete_reference(env, ref);
          if (s == napi_ok && err != nullptr) {
            napi_throw(env, err);
          }
          return nullptr;
        },
        err_ref, &throw_fn);
    if (status != napi_ok) {
      napi_delete_reference(env, err_ref);
      return;
    }

    status = napi_call_function(env, global, queue_microtask, 1, &throw_fn, nullptr);
    if (status != napi_ok) {
      napi_delete_reference(env, err_ref);
    }
  }
}

}  // namespace __node_rocketmq__

#endif