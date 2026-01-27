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
#include "common_utils.h"

#include <LoggerConfig.h>

namespace __node_rocketmq__ {
namespace utils {

void SetLoggerOptions(const Napi::Object& options) {
  // set log level
  Napi::Value log_level = options.Get("logLevel");
  if (log_level.IsNumber()) {
    int32_t level = log_level.ToNumber();
    if (level >= 0 && level < rocketmq::LogLevel::LOG_LEVEL_LEVEL_NUM) {
      rocketmq::GetDefaultLoggerConfig().set_level(
          static_cast<rocketmq::LogLevel>(level));
    }
  }

  // set log directory
  Napi::Value log_dir = options.Get("logDir");
  if (log_dir.IsString()) {
    rocketmq::GetDefaultLoggerConfig().set_path(log_dir.ToString());
  }

  // set log file size
  Napi::Value log_file_size = options.Get("logFileSize");
  if (log_file_size.IsNumber()) {
    rocketmq::GetDefaultLoggerConfig().set_file_size(log_file_size.ToNumber());
  }

  // set log file num
  Napi::Value log_file_num = options.Get("logFileNum");
  if (log_file_num.IsNumber()) {
    rocketmq::GetDefaultLoggerConfig().set_file_count(log_file_num.ToNumber());
  }
}

bool ValidateStringArguments(const Napi::CallbackInfo& info, size_t count, const char* error_msg) {
  if (info.Length() < count) {
    Napi::TypeError::New(info.Env(), "Wrong number of arguments").ThrowAsJavaScriptException();
    return false;
  }
  
  for (size_t i = 0; i < count; ++i) {
    if (!info[i].IsString()) {
      Napi::TypeError::New(info.Env(), error_msg).ThrowAsJavaScriptException();
      return false;
    }
  }
  
  return true;
}

bool ValidateCallback(const Napi::CallbackInfo& info, size_t index, const char* error_msg) {
  if (info.Length() <= index || !info[index].IsFunction()) {
    Napi::TypeError::New(info.Env(), error_msg).ThrowAsJavaScriptException();
    return false;
  }
  return true;
}

}  // namespace utils
}  // namespace __node_rocketmq__