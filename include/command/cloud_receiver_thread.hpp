#ifndef CLOUD_RECEIVER_THREAD_HPP
#define CLOUD_RECEIVER_THREAD_HPP

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <zmq.hpp>

class CloudReceiver {
public:
  CloudReceiver(zmq::context_t &context, const std::string &cloud_endpoint,
                const std::string &internal_endpoint);
  ~CloudReceiver();

  void start();
  void stop();

private:
  void run();

  zmq::context_t &context_;
  std::string cloud_endpoint_;
  std::string internal_endpoint_;

  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> thread_;
};

#endif // CLOUD_RECEIVER_THREAD_HPP
