#ifndef COMMAND_INGRESS_NODE_HPP
#define COMMAND_INGRESS_NODE_HPP

#include "command/cloud_receiver_thread.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <zmq.hpp>

class CommandIngressNode {
public:
  CommandIngressNode(zmq::context_t &context, const std::string &cloud_endpoint,
                     const std::string &mcu_cmd_endpoint,
                     const std::string &status_endpoint);
  ~CommandIngressNode();

  void start();
  void stop();

  // To be called in a main loop or its own thread to process incoming internal
  // messages
  void spin_once();

private:
  zmq::context_t &context_;

  std::string internal_endpoint_ = "inproc://command_ingress_queue";
  std::unique_ptr<zmq::socket_t> internal_router_socket_;

  std::string mcu_cmd_endpoint_;
  std::unique_ptr<zmq::socket_t> mcu_req_socket_;

  uint8_t cmd_seq_ = 0;

  std::unique_ptr<zmq::socket_t> cmd_exec_push_socket_;
  std::unique_ptr<zmq::socket_t> cmd_exec_pull_socket_;

  std::unique_ptr<zmq::socket_t> status_pub_socket_;

  std::unique_ptr<CloudReceiver> cloud_thread_;
  
  // ── Threaded Worker ──────────────────────────────────────────────────
  void command_worker_thread_func();
  std::unique_ptr<std::thread> worker_thread_;
  std::atomic<bool> running_{false};
};

#endif // COMMAND_INGRESS_NODE_HPP