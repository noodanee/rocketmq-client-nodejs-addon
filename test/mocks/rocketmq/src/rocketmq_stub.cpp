#include <cstdlib>
#include <atomic>
#include <string>
#include <utility>
#include <vector>
#include <chrono>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "ClientRPCHook.h"
#include "DefaultMQProducer.h"
#include "DefaultMQPushConsumer.h"
#include "LoggerConfig.h"
#include "MQException.h"
#include "MQMessage.h"
#include "MQMessageListener.h"
#include "SendCallback.h"

namespace rocketmq {

namespace {

bool IsEnvEnabled(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return false;
  }
  return value[0] != '\0' && value[0] != '0';
}

std::string GetEnvString(const char* name, const std::string& fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return fallback;
  }
  return std::string(value);
}

int64_t GetEnvInt64(const char* name, int64_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return fallback;
  }
  char* end = nullptr;
  long long parsed = std::strtoll(value, &end, 10);
  if (end == value) {
    return fallback;
  }
  return static_cast<int64_t>(parsed);
}

int GetEnvInt(const char* name, int fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return fallback;
  }
  char* end = nullptr;
  long parsed = std::strtol(value, &end, 10);
  if (end == value) {
    return fallback;
  }
  return static_cast<int>(parsed);
}

struct PushConsumerAccessState {
  std::mutex mutex;
  std::unordered_map<const void*, size_t> active;
};

PushConsumerAccessState& GetPushConsumerAccessState() {
  static PushConsumerAccessState state;
  return state;
}

class PushConsumerAccessGuard {
 public:
  explicit PushConsumerAccessGuard(const void* consumer) : consumer_(consumer) {
    auto& state = GetPushConsumerAccessState();
    std::lock_guard<std::mutex> lock(state.mutex);
    size_t& active = state.active[consumer_];
    if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_FAIL_ON_CONCURRENT_ACCESS") && active > 0) {
      throw MQException("consumer concurrent access");
    }
    active++;
    entered_ = true;
  }

  ~PushConsumerAccessGuard() {
    if (!entered_) {
      return;
    }
    auto& state = GetPushConsumerAccessState();
    std::lock_guard<std::mutex> lock(state.mutex);
    auto it = state.active.find(consumer_);
    if (it == state.active.end()) {
      return;
    }
    if (it->second <= 1) {
      state.active.erase(it);
      return;
    }
    it->second--;
  }

 private:
  const void* consumer_;
  bool entered_{false};
};

MQMessageExt BuildMessageFromEnv() {
  MQMessageExt message(
      GetEnvString("ROCKETMQ_STUB_MESSAGE_TOPIC", "TopicTest"),
      GetEnvString("ROCKETMQ_STUB_MESSAGE_BODY", "Hello"));
  message.set_tags(GetEnvString("ROCKETMQ_STUB_MESSAGE_TAGS", "TagA"));
  message.set_keys(GetEnvString("ROCKETMQ_STUB_MESSAGE_KEYS", "KeyA"));
  message.set_msg_id(GetEnvString("ROCKETMQ_STUB_MESSAGE_MSG_ID", "MSGID"));
  message.set_born_timestamp(GetEnvInt64("ROCKETMQ_STUB_MESSAGE_BORN_TS", 1));
  message.set_store_timestamp(GetEnvInt64("ROCKETMQ_STUB_MESSAGE_STORE_TS", 2));
  message.set_queue_id(GetEnvInt("ROCKETMQ_STUB_MESSAGE_QUEUE_ID", 0));
  message.set_queue_offset(GetEnvInt64("ROCKETMQ_STUB_MESSAGE_QUEUE_OFFSET", 3));
  message.set_reconsume_times(GetEnvInt("ROCKETMQ_STUB_MESSAGE_RECONSUME_TIMES", 0));
  return message;
}

}

void LoggerConfig::set_level(LogLevel) {}

void LoggerConfig::set_path(const std::string&) {}

void LoggerConfig::set_file_size(int64_t) {}

void LoggerConfig::set_file_count(int) {}

LoggerConfig& GetDefaultLoggerConfig() {
  static LoggerConfig config;
  return config;
}

SessionCredentials::SessionCredentials(const std::string& access_key,
                                       const std::string& secret_key,
                                       const std::string& ons_channel)
    : access_key_(access_key),
      secret_key_(secret_key),
      ons_channel_(ons_channel) {}

const std::string& SessionCredentials::access_key() const { return access_key_; }

const std::string& SessionCredentials::secret_key() const { return secret_key_; }

const std::string& SessionCredentials::ons_channel() const { return ons_channel_; }

ClientRPCHook::ClientRPCHook(const SessionCredentials& credentials)
    : credentials_(credentials) {}

const SessionCredentials& ClientRPCHook::credentials() const { return credentials_; }

MQException::MQException(const std::string& message) : message_(message) {}

const char* MQException::what() const noexcept { return message_.c_str(); }

SendResult::SendResult() : status_(SEND_OK), msg_id_(""), queue_offset_(0) {}

SendResult::SendResult(SendStatus status,
                       const std::string& msg_id,
                       int64_t queue_offset)
    : status_(status), msg_id_(msg_id), queue_offset_(queue_offset) {}

SendStatus SendResult::send_status() const { return status_; }

const std::string& SendResult::msg_id() const { return msg_id_; }

int64_t SendResult::queue_offset() const { return queue_offset_; }

SendCallback::~SendCallback() = default;

AutoDeleteSendCallback::~AutoDeleteSendCallback() = default;

MQMessage::MQMessage() : topic_(), body_(), tags_(), keys_() {}

MQMessage::MQMessage(const std::string& topic, const std::string& body)
    : topic_(topic), body_(body), tags_(), keys_() {}

const std::string& MQMessage::topic() const { return topic_; }

const std::string& MQMessage::body() const { return body_; }

const std::string& MQMessage::tags() const { return tags_; }

const std::string& MQMessage::keys() const { return keys_; }

void MQMessage::set_tags(const std::string& tags) { tags_ = tags; }

void MQMessage::set_keys(const std::string& keys) { keys_ = keys; }

MQMessageExt::MQMessageExt()
    : MQMessage(),
      msg_id_(),
      born_timestamp_(0),
      store_timestamp_(0),
      queue_id_(0),
      queue_offset_(0),
      reconsume_times_(0) {}

MQMessageExt::MQMessageExt(const std::string& topic, const std::string& body)
    : MQMessage(topic, body),
      msg_id_(),
      born_timestamp_(0),
      store_timestamp_(0),
      queue_id_(0),
      queue_offset_(0),
      reconsume_times_(0) {}

const std::string& MQMessageExt::msg_id() const { return msg_id_; }

int64_t MQMessageExt::born_timestamp() const { return born_timestamp_; }

int64_t MQMessageExt::store_timestamp() const { return store_timestamp_; }

int32_t MQMessageExt::queue_id() const { return queue_id_; }

int64_t MQMessageExt::queue_offset() const { return queue_offset_; }

int32_t MQMessageExt::reconsume_times() const { return reconsume_times_; }

void MQMessageExt::set_msg_id(const std::string& msg_id) { msg_id_ = msg_id; }

void MQMessageExt::set_born_timestamp(int64_t ts) { born_timestamp_ = ts; }

void MQMessageExt::set_store_timestamp(int64_t ts) { store_timestamp_ = ts; }

void MQMessageExt::set_queue_id(int32_t id) { queue_id_ = id; }

void MQMessageExt::set_queue_offset(int64_t offset) { queue_offset_ = offset; }

void MQMessageExt::set_reconsume_times(int32_t times) { reconsume_times_ = times; }

MessageListenerConcurrently::~MessageListenerConcurrently() = default;

DefaultMQProducer::DefaultMQProducer(const std::string& group_name)
    : group_name_(group_name),
      instance_name_(),
      namesrv_addr_(),
      max_message_size_(0),
      compress_level_(0),
      send_msg_timeout_(0),
      rpc_hook_(nullptr) {}

void DefaultMQProducer::set_group_name(const std::string& group_name) {
  group_name_ = group_name;
}

void DefaultMQProducer::set_instance_name(const std::string& instance_name) {
  instance_name_ = instance_name;
}

void DefaultMQProducer::set_namesrv_addr(const std::string& namesrv_addr) {
  namesrv_addr_ = namesrv_addr;
}

void DefaultMQProducer::set_max_message_size(int max_message_size) {
  max_message_size_ = max_message_size;
}

void DefaultMQProducer::set_compress_level(int compress_level) {
  compress_level_ = compress_level;
}

void DefaultMQProducer::set_send_msg_timeout(int timeout_ms) {
  send_msg_timeout_ = timeout_ms;
}

void DefaultMQProducer::setRPCHook(std::shared_ptr<ClientRPCHook> rpc_hook) {
  rpc_hook_ = std::move(rpc_hook);
}

void DefaultMQProducer::start() {
  if (shutdown_) {
    throw MQException("producer: SHUTDOWN_ALREADY");
  }
  if (IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_START_ERROR")) {
    throw MQException("producer start error");
  }
  int delay_ms = GetEnvInt("ROCKETMQ_STUB_PRODUCER_START_DELAY_MS", 0);
  if (delay_ms > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
  }
  static std::atomic<int> start_error_once{0};
  if (IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_START_ERROR_ONCE")) {
    if (start_error_once.fetch_add(1) == 0) {
      throw MQException("producer start error");
    }
  } else {
    start_error_once.store(0);
  }
}

void DefaultMQProducer::shutdown() {
  {
    std::vector<std::thread> pending;
    {
      std::lock_guard<std::mutex> lock(threads_mutex_);
      pending.swap(threads_);
    }
    for (auto& t : pending) {
      if (t.joinable()) {
        t.join();
      }
    }
  }
  if (IsEnvEnabled("ROCKETMQ_STUB_PRODUCER_SHUTDOWN_ERROR")) {
    throw MQException("producer shutdown error");
  }
  shutdown_ = true;
}

void DefaultMQProducer::send(MQMessage&, SendCallback* callback) {
  if (IsEnvEnabled("ROCKETMQ_STUB_SEND_THROW")) {
    if (callback) {
      MQException exception("producer send throw");
      callback->onException(exception);
    }
    return;
  }

  if (callback == nullptr) {
    return;
  }

  int async_delay_ms = GetEnvInt("ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS", 0);
  if (async_delay_ms > 0) {
    bool send_exception = IsEnvEnabled("ROCKETMQ_STUB_SEND_EXCEPTION");
    SendStatus status = static_cast<SendStatus>(
        GetEnvInt("ROCKETMQ_STUB_SEND_STATUS", static_cast<int>(SEND_OK)));
    std::string msg_id = GetEnvString("ROCKETMQ_STUB_SEND_MSG_ID", "MSGID");
    int64_t queue_offset = GetEnvInt64("ROCKETMQ_STUB_SEND_QUEUE_OFFSET", 0);
    std::thread t([callback, async_delay_ms, send_exception, status, msg_id, queue_offset]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(async_delay_ms));
      if (send_exception) {
        MQException exception("producer send exception");
        callback->onException(exception);
      } else {
        SendResult result(status, msg_id, queue_offset);
        callback->onSuccess(result);
      }
    });
    {
      std::lock_guard<std::mutex> lock(threads_mutex_);
      threads_.push_back(std::move(t));
    }
    return;
  }

  if (IsEnvEnabled("ROCKETMQ_STUB_SEND_EXCEPTION")) {
    MQException exception("producer send exception");
    callback->onException(exception);
    return;
  }

  SendStatus status = static_cast<SendStatus>(
      GetEnvInt("ROCKETMQ_STUB_SEND_STATUS", static_cast<int>(SEND_OK)));
  SendResult result(
      status,
      GetEnvString("ROCKETMQ_STUB_SEND_MSG_ID", "MSGID"),
      GetEnvInt64("ROCKETMQ_STUB_SEND_QUEUE_OFFSET", 0));
  callback->onSuccess(result);
}

DefaultMQPushConsumer::DefaultMQPushConsumer(const std::string& group_name)
    : group_name_(group_name),
      instance_name_(),
      namesrv_addr_(),
      consume_thread_nums_(0),
      consume_message_batch_max_size_(0),
      max_reconsume_times_(0),
      rpc_hook_(nullptr),
      listener_(nullptr),
      listener_holder_(nullptr) {}

void DefaultMQPushConsumer::set_group_name(const std::string& group_name) {
  group_name_ = group_name;
}

void DefaultMQPushConsumer::set_instance_name(const std::string& instance_name) {
  instance_name_ = instance_name;
}

void DefaultMQPushConsumer::set_namesrv_addr(const std::string& namesrv_addr) {
  namesrv_addr_ = namesrv_addr;
}

void DefaultMQPushConsumer::set_consume_thread_nums(int nums) {
  consume_thread_nums_ = nums;
}

void DefaultMQPushConsumer::set_consume_message_batch_max_size(int size) {
  consume_message_batch_max_size_ = size;
}

void DefaultMQPushConsumer::set_max_reconsume_times(int times) {
  max_reconsume_times_ = times;
}

void DefaultMQPushConsumer::setRPCHook(std::shared_ptr<ClientRPCHook> rpc_hook) {
  PushConsumerAccessGuard guard(this);
  rpc_hook_ = std::move(rpc_hook);
}

void DefaultMQPushConsumer::start() {
  PushConsumerAccessGuard guard(this);
  if (shutdown_) {
    throw MQException("consumer: SHUTDOWN_ALREADY");
  }
  if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_START_ERROR")) {
    throw MQException("consumer start error");
  }
  int delay_ms = GetEnvInt("ROCKETMQ_STUB_CONSUMER_START_DELAY_MS", 0);
  if (delay_ms > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
  }

  if (listener_ != nullptr) {
    if (IsEnvEnabled("ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC")) {
      auto listener = listener_holder_;
      if (!listener && listener_ != nullptr) {
        listener = std::shared_ptr<MessageListenerConcurrently>(
            listener_, [](MessageListenerConcurrently*) {});
      }
      if (!listener) {
        return;
      }
      auto* self = this;
      std::thread([listener, self]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (self->shutdown_.load()) {
          return;
        }
        std::vector<MQMessageExt> messages;
        messages.push_back(BuildMessageFromEnv());
        listener->consumeMessage(messages);
      }).detach();
      return;
    }

    if (IsEnvEnabled("ROCKETMQ_STUB_CONSUME_MESSAGE")) {
      std::vector<MQMessageExt> messages;
      messages.push_back(BuildMessageFromEnv());
      listener_->consumeMessage(messages);
    }
  }
}

void DefaultMQPushConsumer::shutdown() {
  PushConsumerAccessGuard guard(this);
  if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_SHUTDOWN_ERROR")) {
    throw MQException("consumer shutdown error");
  }
  shutdown_ = true;
  listener_holder_.reset();
}

void DefaultMQPushConsumer::subscribe(const std::string&, const std::string&) {
  PushConsumerAccessGuard guard(this);
  if (IsEnvEnabled("ROCKETMQ_STUB_CONSUMER_SUBSCRIBE_ERROR")) {
    throw MQException("consumer subscribe error");
  }
}

void DefaultMQPushConsumer::registerMessageListener(MessageListenerConcurrently* listener) {
  PushConsumerAccessGuard guard(this);
  listener_ = listener;
  listener_holder_.reset();
}

void DefaultMQPushConsumer::registerMessageListener(std::shared_ptr<MessageListenerConcurrently> listener) {
  PushConsumerAccessGuard guard(this);
  listener_holder_ = std::move(listener);
  listener_ = listener_holder_.get();
}

}
