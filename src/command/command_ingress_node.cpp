#include "command/command_ingress_node.hpp"
#include "data/oro_protocol.hpp"
#include "data/topic_registry.hpp"
//#include "radxa_drivers/radxa_services.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace oro;

CommandIngressNode::CommandIngressNode(zmq::context_t &context,
                                       const std::string &cloud_endpoint,
                                       const std::string &mcu_cmd_endpoint,
                                       const std::string &status_endpoint)
    : context_(context), mcu_cmd_endpoint_(mcu_cmd_endpoint) {

  internal_router_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::router);
  internal_router_socket_->bind(internal_endpoint_);

  mcu_req_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::req);
  mcu_req_socket_->set(zmq::sockopt::rcvtimeo, 15000);
  mcu_req_socket_->connect(mcu_cmd_endpoint_);

  cmd_exec_push_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::push);
  cmd_exec_push_socket_->connect("ipc:///tmp/oro_cmd_exec.ipc");

  cmd_exec_pull_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::pull);
  cmd_exec_pull_socket_->set(zmq::sockopt::rcvtimeo, 15000);
  cmd_exec_pull_socket_->connect("ipc:///tmp/oro_cmd_result.ipc");

  status_pub_socket_ =
      std::make_unique<zmq::socket_t>(context_, zmq::socket_type::pub);
  status_pub_socket_->connect(status_endpoint);

  cloud_thread_ = std::make_unique<CloudReceiver>(context_, cloud_endpoint,
                                                  internal_endpoint_);
}

CommandIngressNode::~CommandIngressNode() { stop(); }

void CommandIngressNode::start() {
  if (running_.load())
    return;

  std::cout
      << "[CommandIngressNode] Starting specialized receiver threads...\n";
  
  running_ = true;
  cloud_thread_->start();
  
  worker_thread_ = std::make_unique<std::thread>(&CommandIngressNode::command_worker_thread_func, this);
}

void CommandIngressNode::stop() {
  if (!running_.load())
    return;

  std::cout << "[CommandIngressNode] Stopping receiver threads...\n";
  
  running_ = false;
  cloud_thread_->stop();

  if (worker_thread_ && worker_thread_->joinable()) {
    worker_thread_->join();
  }
}

void CommandIngressNode::spin_once() {
  // Now a no-op as all processing happens in command_worker_thread_func
}

void CommandIngressNode::command_worker_thread_func() {
  std::cout << "[CommandWorkerThread] Running" << std::endl;

  while (running_.load(std::memory_order_relaxed)) {
    // 1. Check for internal command requests (ROUTER socket)
    // ROUTER receives: [Identity] [Empty] [Payload]
    zmq::message_t identity;
    auto res_id =
        internal_router_socket_->recv(identity, zmq::recv_flags::dontwait);
    
    if (!res_id) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    zmq::message_t empty;
  // (void) cast used to suppress [[nodiscard]] warning; blocking recv expected here
    (void)internal_router_socket_->recv(empty, zmq::recv_flags::none);

    zmq::message_t payload;
  // (void) cast used to suppress [[nodiscard]] warning; blocking recv expected here
    (void)internal_router_socket_->recv(payload, zmq::recv_flags::none);

    std::string cmd_str(static_cast<char *>(payload.data()), payload.size());
    std::cout << "[CommandWorkerThread] Processed aggregate command: " << cmd_str
              << std::endl;

    std::string final_mcu_response =
        "{\"status\":\"error\",\"message\":\"unsupported_topic\"}";

    try {
      json j = json::parse(cmd_str);
      if (j.contains("header") || j.contains("signal_id")) {
        // ... (UCES logic preserved)
        zmq::message_t push_msg(cmd_str.data(), cmd_str.size());
        cmd_exec_push_socket_->send(push_msg, zmq::send_flags::none);

        zmq::message_t result_msg;
        if (cmd_exec_pull_socket_->recv(result_msg, zmq::recv_flags::none)) {
          final_mcu_response = std::string(static_cast<char *>(result_msg.data()), result_msg.size());
        } else {
          final_mcu_response = "{\"status\":\"timeout\",\"message\":\"command_executor_timeout\"}";
        }
      } else {
        std::string topic_str = j.value("topic", "");
        float value = j.value("value", 0.0f);

        const TopicDescriptor *desc = nullptr;
        for (const auto &t : TOPIC_REGISTRY) {
          if (t.zmq_topic && std::string(t.zmq_topic) == topic_str) {
            desc = &t;
            break;
          }
        }

        if (!desc) {
          final_mcu_response = "{\"status\":\"error\",\"message\":\"unsupported_topic\"}";
        } else if ((desc->sensor_id == -1 || topic_str == "/commands/camera_rotation") && desc->source == TopicSource::SYSTEM) {
          // ── UCES / Host Routing ───────────────────────────────────────────────
          int signal_id = -1;
          std::string signal_type;
          json payload_obj = {{"value", value}};
          if (j.contains("payload") && j["payload"].is_object()) {
            for (auto& el : j["payload"].items()) {
              payload_obj[el.key()] = el.value();
            }
          }

          if (topic_str == "/commands/treat/dispense") {
            signal_id = 85; signal_type = "treat_dispense_command_event"; payload_obj["treat_quantity"] = value;
          } else if (topic_str == "/commands/photo_capture") {
            signal_id = 91; signal_type = "photo_capture_command_event";
          } else if (topic_str == "/commands/live_session/start") {
            signal_id = 88; signal_type = "live_session_start_event";
          } else if (topic_str == "/commands/live_session/end") {
            signal_id = 133; signal_type = "live_session_end_event";
          } else if (topic_str == "/commands/lid/1") {
            signal_id = 64; signal_type = "lid_actuation_command"; payload_obj["lid_id"] = 1; payload_obj["action"] = static_cast<int>(value);
          } else if (topic_str == "/commands/lid/2") {
            signal_id = 64; signal_type = "lid_actuation_command"; payload_obj["lid_id"] = 2; payload_obj["action"] = static_cast<int>(value);
          } else if (topic_str == "/commands/audio/speakers") {
            int action_code = -1;
            if (payload_obj.contains("action_code")) {
              if (payload_obj["action_code"].is_number()) {
                action_code = payload_obj["action_code"].get<int>();
              } else if (payload_obj["action_code"].is_string()) {
                action_code = std::stoi(payload_obj["action_code"].get<std::string>());
              }
            } else {
              action_code = static_cast<int>(value);
            }

            std::string track_name = "";
            if (payload_obj.contains("track") && payload_obj["track"].is_string()) {
              track_name = payload_obj["track"].get<std::string>();
            }

            if (!track_name.empty()) {
              if (track_name.find("breaking_bad") != std::string::npos) {
                action_code = 1;
              } else if (track_name.find("dandelions") != std::string::npos) {
                action_code = 2;
              } else if (track_name.find("call_this_love") != std::string::npos) {
                action_code = 3;
              } else if (action_code <= 0) {
                action_code = 1;
              }
            }

            if (action_code == 0 || action_code == 97) {
              signal_id = 138;
              signal_type = "stop_music_event";
            } else {
              signal_id = 137;
              signal_type = "play_music_event";
            }

            std::string file_id = "unknown_track";
            std::string storage_path = "";
            if (action_code == 1) {
              file_id = "breaking_bad_intro";
              storage_path = "/home/radxa/Music/breaking_bad_intro.mp3";
            } else if (action_code == 2) {
              file_id = "dandelions_violin";
              storage_path = "/home/radxa/Music/dandelions_violin.mp3";
            } else if (action_code == 3) {
              file_id = "I_think_they_call_this_love";
              storage_path = "/home/radxa/Music/I_think_they_call_this_love.mp3";
            } else if (!track_name.empty()) {
              file_id = track_name;
              size_t dot_pos = file_id.rfind('.');
              if (dot_pos != std::string::npos) {
                file_id = file_id.substr(0, dot_pos);
              }
              storage_path = "/home/radxa/Music/" + track_name;
            } else {
              file_id = "breaking_bad_intro";
              storage_path = "/home/radxa/Music/breaking_bad_intro.mp3";
            }

            payload_obj["action_code"] = action_code;
            payload_obj["file_id"] = file_id;
            payload_obj["storage_path"] = storage_path;
            payload_obj["event_time"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
          } else if (topic_str == "/commands/feed") {
            signal_id = 64;
            signal_type = "lid_actuation_command";
            payload_obj["lid_id"] = j.value("lid_id", 1);
            payload_obj["action"] = j.value("action", 0);
          } else if (topic_str == "/commands/settings/apply") {
            signal_id = 98; signal_type = "settings_apply_success_status"; payload_obj["settings_profile_id"] = "default_profile_v1";
          } else if (topic_str == "/commands/camera_rotation") {
            signal_id = 134; signal_type = "camera_rotation_command";
            if (!payload_obj.contains("angle")) {
              payload_obj["angle"] = value;
            }
          } else if (topic_str == "/commands/privacy_mode") {
            signal_id = 140; signal_type = "privacy_mode_command_event";
            payload_obj["enabled"] = static_cast<bool>(value >= 0.5f);
          }

          if (signal_id != -1) {
            json uces_cmd;
            uces_cmd["header"] = {
                {"signal_id", signal_id},
                {"signal_type", signal_type},
                {"command_id", "CMD_" + std::to_string(signal_id) + "_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count())},
                {"issued_by", "cloud_ingress"},
                {"event_time", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()}
            };
            uces_cmd["payload"] = payload_obj;
            
            std::string uces_cmd_str = uces_cmd.dump();
            zmq::message_t push_msg(uces_cmd_str.data(), uces_cmd_str.size());
            cmd_exec_push_socket_->send(push_msg, zmq::send_flags::none);

            zmq::message_t result_msg;
            if (cmd_exec_pull_socket_->recv(result_msg, zmq::recv_flags::none)) {
              final_mcu_response = std::string(static_cast<char *>(result_msg.data()), result_msg.size());
              if (topic_str == "/commands/settings/apply") {
                zmq::message_t topic_msg("/status/settings/apply", 23);
                zmq::message_t status_msg(final_mcu_response.size());
                std::memcpy(status_msg.data(), final_mcu_response.data(), final_mcu_response.size());
                status_pub_socket_->send(topic_msg, zmq::send_flags::sndmore);
                status_pub_socket_->send(status_msg, zmq::send_flags::none);
              }
            } else {
              final_mcu_response = "{\"status\":\"timeout\",\"message\":\"command_executor_timeout\"}";
            }
          } else {
            //final_mcu_response = RadxaServices::process_command(j);
          }
        } else if (desc->sensor_id != -1 && (topic_str.find("/commands/") == 0 || desc->topic_id == TID_CMD_CAMERA_ROTATION)) {
          // ── MCU-Bound Commands ───────────────────────────────────────────────
          OroPacket pkt{};
          pkt.start = START_BYTE;
          pkt.msg_type = PACK_MSG_TYPE(PRIO_HIGH, MSG_CONTROL);
          pkt.id_seq = PACK_ID_SEQ(cmd_seq_, static_cast<uint8_t>(desc->sensor_id));
          cmd_seq_ = (cmd_seq_ + 1) & 0x0F;

          if (desc->topic_id == TID_CMD_CAMERA_ROTATION ||
              desc->topic_id == TID_CMD_DISPLAY || desc->topic_id == TID_CMD_LED) {
            pack_value_i32(pkt.value, static_cast<int32_t>(value * 100.0f));
          } else {
            pack_value_i32(pkt.value, static_cast<int32_t>(value));
          }
          pkt.crc = oro_crc8(&pkt.msg_type, 6);

          zmq::message_t mcu_msg(sizeof(OroPacket));
          std::memcpy(mcu_msg.data(), &pkt, sizeof(OroPacket));

          mcu_req_socket_->send(mcu_msg, zmq::send_flags::none);
          
          zmq::message_t mcu_response;
          if (mcu_req_socket_->recv(mcu_response, zmq::recv_flags::none)) {
            final_mcu_response = std::string(static_cast<char *>(mcu_response.data()), mcu_response.size());
          } else {
            final_mcu_response = "{\"status\":\"timeout\",\"message\":\"mcu_communication_failure\"}";
          }
        }
      }
    } catch (const json::parse_error &e) {
      final_mcu_response = "{\"status\":\"error\",\"message\":\"json_parse_error\"}";
    }

    // 3. Reply back to the receiver thread
    internal_router_socket_->send(identity, zmq::send_flags::sndmore);
    internal_router_socket_->send(zmq::message_t(0), zmq::send_flags::sndmore);
    zmq::message_t res_msg(final_mcu_response.size());
    std::memcpy(res_msg.data(), final_mcu_response.data(), final_mcu_response.size());
    internal_router_socket_->send(res_msg, zmq::send_flags::none);
  }
}
