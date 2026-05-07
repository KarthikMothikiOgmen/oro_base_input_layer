#include "command/command_ingress_node.hpp"
#include "data/oro_protocol.hpp"
#include "data/topic_registry.hpp"
#include "radxa_services.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace oro;

CommandIngressNode::CommandIngressNode(zmq::context_t &context,
                                       const std::string &cloud_endpoint,
                                       const std::string &mcu_cmd_endpoint)
    : context_(context), mcu_cmd_endpoint_(mcu_cmd_endpoint) {

  internal_router_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::router);
  internal_router_socket_->bind(internal_endpoint_);

  mcu_req_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::req);
  mcu_req_socket_->connect(mcu_cmd_endpoint_);

  cmd_exec_push_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::push);
  cmd_exec_push_socket_->connect("ipc:///tmp/oro_cmd_exec.ipc");

  cmd_exec_pull_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::pull);
  cmd_exec_pull_socket_->connect("ipc:///tmp/oro_cmd_result.ipc");

  cloud_thread_ = std::make_unique<CloudReceiver>(context_, cloud_endpoint,
                                                  internal_endpoint_);
}

CommandIngressNode::~CommandIngressNode() { stop(); }

void CommandIngressNode::start() {
  std::cout
      << "[CommandIngressNode] Starting specialized receiver threads...\n";
  cloud_thread_->start();
}

void CommandIngressNode::stop() {
  std::cout << "[CommandIngressNode] Stopping receiver threads...\n";
  cloud_thread_->stop();
}

void CommandIngressNode::spin_once() {
  // 1. Check for internal command requests (ROUTER socket)
  // ROUTER receives: [Identity] [Empty] [Payload]
  zmq::message_t identity;
  auto res_id =
      internal_router_socket_->recv(identity, zmq::recv_flags::dontwait);
  if (!res_id)
    return;

  zmq::message_t empty;
  // (void) cast used to suppress [[nodiscard]] warning; blocking recv expected here
  (void)internal_router_socket_->recv(empty, zmq::recv_flags::none);

  zmq::message_t payload;
  // (void) cast used to suppress [[nodiscard]] warning; blocking recv expected here
  (void)internal_router_socket_->recv(payload, zmq::recv_flags::none);

  std::string cmd_str(static_cast<char *>(payload.data()), payload.size());
  std::cout << "[CommandIngressNode] Processed aggregate command: " << cmd_str
            << std::endl;

  std::string final_mcu_response =
      "{\"status\":\"error\",\"message\":\"unsupported_topic\"}";

  try {
    json j = json::parse(cmd_str);
    if (j.contains("header") || j.contains("signal_id")) {
      std::cout << "[CommandIngressNode] Routing UCES Command via CommandExecutor..." << std::endl;
      zmq::message_t push_msg(cmd_str.data(), cmd_str.size());
      cmd_exec_push_socket_->send(push_msg, zmq::send_flags::none);

      zmq::message_t result_msg;
      if (cmd_exec_pull_socket_->recv(result_msg, zmq::recv_flags::none)) {
        final_mcu_response = std::string(static_cast<char *>(result_msg.data()), result_msg.size());
        std::cout << "[CommandIngressNode] CommandExecutor Result: " << final_mcu_response << std::endl;
      } else {
        final_mcu_response = "{\"status\":\"timeout\",\"message\":\"command_executor_timeout\"}";
      }
    } else {
      std::string topic_str = j.value("topic", "");
      float value = j.value("value", 0.0f);

      // ── Unified Topic Routing ───────────────────────────────────────────────
      const TopicDescriptor *desc = nullptr;
      for (const auto &t : TOPIC_REGISTRY) {
        if (t.zmq_topic && std::string(t.zmq_topic) == topic_str) {
          desc = &t;
          break;
        }
      }

      if (!desc) {
        final_mcu_response = "{\"status\":\"error\",\"message\":\"unsupported_topic\"}";
      } else if (desc->sensor_id == -1 && desc->source == TopicSource::SYSTEM) {
        // ── Host-Side Services (JSON Native) ──────────────────────────────────
        std::cout << "[CommandIngressNode] Routing JSON to Radxa Service: " << topic_str << std::endl;
        final_mcu_response = RadxaServices::process_command(j);
        
      } else if (desc->sensor_id != -1 && (topic_str.find("/commands/") == 0 || desc->topic_id == TID_CMD_CAMERA_ROTATION)) {
        // ── MCU-Bound Commands (Binary Protocol) ───────────────────────────────
        OroPacket pkt{};
        pkt.start = START_BYTE;
        pkt.msg_type = PACK_MSG_TYPE(PRIO_HIGH, MSG_COMMAND);
        pkt.id_seq = PACK_ID_SEQ(cmd_seq_, static_cast<uint8_t>(desc->sensor_id));
        cmd_seq_ = (cmd_seq_ + 1) & 0x0F;

        // Pack float as fixed-point for specific actuators, or raw int for others
        if (desc->topic_id == TID_CMD_CAMERA_ROTATION || 
            desc->topic_id == TID_CMD_CAMERA_SERVO ||
            desc->topic_id == TID_CMD_DISPLAY || 
            desc->topic_id == TID_CMD_LED) {
          pack_value_i32(pkt.value, static_cast<int32_t>(value * 100.0f));
        } else {
          pack_value_i32(pkt.value, static_cast<int32_t>(value));
        }
        pkt.crc = oro_crc8(&pkt.msg_type, 6);

        zmq::message_t mcu_msg(sizeof(OroPacket));
        std::memcpy(mcu_msg.data(), &pkt, sizeof(OroPacket));

        std::cout << "[CommandIngressNode] Dispatching " << topic_str << " to MCU..." << std::endl;
        mcu_req_socket_->send(mcu_msg, zmq::send_flags::none);

        std::cout << "[CommandIngressNode] Waiting for MCU ACK..." << std::endl;
        
        // Wait for result from McuSerialReaderNode
        zmq::message_t mcu_response;
        if (mcu_req_socket_->recv(mcu_response, zmq::recv_flags::none)) {
          final_mcu_response = std::string(static_cast<char *>(mcu_response.data()), mcu_response.size());
          std::cout << "[CommandIngressNode] Final MCU Result: " << final_mcu_response << std::endl;
        } else {
          final_mcu_response = "{\"status\":\"timeout\",\"message\":\"mcu_communication_failure\"}";
        }
      }
    }
  } catch (const json::parse_error &e) {
    final_mcu_response =
        "{\"status\":\"error\",\"message\":\"json_parse_error\"}";
  }

  // 3. Reply back to the receiver thread: [Identity] [Empty] [Result]
  internal_router_socket_->send(identity, zmq::send_flags::sndmore);
  internal_router_socket_->send(zmq::message_t(0), zmq::send_flags::sndmore);
  zmq::message_t res_msg(final_mcu_response.size());
  std::memcpy(res_msg.data(), final_mcu_response.data(),
              final_mcu_response.size());
  internal_router_socket_->send(res_msg, zmq::send_flags::none);
}
