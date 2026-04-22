#include "command/cloud_receiver_thread.hpp"
#include <chrono>
#include <iostream>

CloudReceiver::CloudReceiver(zmq::context_t &context,
                             const std::string &cloud_endpoint,
                             const std::string &internal_endpoint)
    : context_(context), cloud_endpoint_(cloud_endpoint),
      internal_endpoint_(internal_endpoint) {}

CloudReceiver::~CloudReceiver() { stop(); }

void CloudReceiver::start() {
  if (!running_) {
    running_ = true;
    thread_ = std::make_unique<std::thread>(&CloudReceiver::run, this);
  }
}

void CloudReceiver::stop() {
  if (running_) {
    running_ = false;
    if (thread_ && thread_->joinable()) {
      thread_->join();
    }
  }
}

void CloudReceiver::run() {
  try {
    // REP socket to receive external commands
    zmq::socket_t rep_socket(context_, zmq::socket_type::rep);
    // Set receive timeout so thread can gracefully exit on stop()
    rep_socket.set(zmq::sockopt::rcvtimeo, 500);
    rep_socket.bind(cloud_endpoint_);

    // REQ socket to send parsed commands to the main CommandIngressNode via
    // inproc and wait for the synchronous MCU result.
    zmq::socket_t req_socket(context_, zmq::socket_type::req);
    req_socket.connect(internal_endpoint_);

    std::cout << "[CloudReceiver] Listening on " << cloud_endpoint_
              << std::endl;

    while (running_) {
      zmq::message_t request;
      auto res = rep_socket.recv(request, zmq::recv_flags::none);

      if (res) {
        std::string req_str(static_cast<char *>(request.data()),
                            request.size());
        std::cout << "[CloudReceiver] Received external request: " << req_str << std::endl;

        // Forward to main input layer via inproc REQ
        zmq::message_t internal_msg(req_str.size());
        memcpy(internal_msg.data(), req_str.data(), req_str.size());
        req_socket.send(internal_msg, zmq::send_flags::none);

        // Block until CommandIngressNode finishes MCU cycle and replies
        zmq::message_t mcu_result;
        auto mcu_res = req_socket.recv(mcu_result, zmq::recv_flags::none);

        if (mcu_res) {
            // Forward the actual MCU result (Success/Timeout) back to the client
            rep_socket.send(mcu_result, zmq::send_flags::none);
        } else {
            std::string err_rep = "{\"status\":\"error\",\"message\":\"internal_timeout\"}";
            zmq::message_t err_msg(err_rep.size());
            memcpy(err_msg.data(), err_rep.data(), err_rep.size());
            rep_socket.send(err_msg, zmq::send_flags::none);
        }
      }
    }
  } catch (const zmq::error_t &e) {
    std::cerr << "[CloudReceiver] ZMQ Error: " << e.what() << std::endl;
  }
}
