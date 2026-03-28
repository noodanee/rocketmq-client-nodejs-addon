#ifndef ROCKETMQ_STUB_DEFAULT_MQ_PUSH_CONSUMER_H
#define ROCKETMQ_STUB_DEFAULT_MQ_PUSH_CONSUMER_H

#include <atomic>
#include <memory>
#include <string>

#include "ClientRPCHook.h"
#include "MQMessageListener.h"

namespace rocketmq {

class DefaultMQPushConsumer {
 public:
  explicit DefaultMQPushConsumer(const std::string& group_name);

  void set_group_name(const std::string& group_name);
  void set_instance_name(const std::string& instance_name);
  void set_namesrv_addr(const std::string& namesrv_addr);
  void set_consume_thread_nums(int nums);
  void set_consume_message_batch_max_size(int size);
  void set_max_reconsume_times(int times);
  void setRPCHook(std::shared_ptr<ClientRPCHook> rpc_hook);

  void start();
  void shutdown();
  void subscribe(const std::string& topic, const std::string& expression);
  void registerMessageListener(MessageListenerConcurrently* listener);
  void registerMessageListener(std::shared_ptr<MessageListenerConcurrently> listener);
  void set_shutdown_on_destroy(bool enabled) { (void)enabled; }

 private:
  std::string group_name_;
  std::string instance_name_;
  std::string namesrv_addr_;
  int consume_thread_nums_;
  int consume_message_batch_max_size_;
  int max_reconsume_times_;
  std::shared_ptr<ClientRPCHook> rpc_hook_;
  MessageListenerConcurrently* listener_;
  std::shared_ptr<MessageListenerConcurrently> listener_holder_;
  std::atomic<bool> shutdown_{false};
};

}

#endif
