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
}

}  // namespace __node_rocketmq__

#endif