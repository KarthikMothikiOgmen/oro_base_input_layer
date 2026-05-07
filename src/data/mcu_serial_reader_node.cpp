// ============================================================================
// mcu_serial_reader_node.cpp — MCU Serial Reader Node Implementation
//
// Full UART-to-ZMQ pipeline:
//   Serial I/O Thread → rx_queue → Decode → CRC Check → Field Extract →
//   Sensor Route (LUT) → Value Convert → Publish Filter → ZMQ PUB
// ============================================================================

#include "data/mcu_serial_reader_node.hpp"

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>

namespace oro {

// ── Constructor / Destructor ────────────────────────────────────────────────

McuSerialReaderNode::McuSerialReaderNode(zmq::context_t &context,
                                         const std::string &sensor_endpoint,
                                         const std::string &system_endpoint,
                                         const std::string &status_endpoint,
                                         const std::string &cmd_endpoint)
    : context_(context), sensor_pub_(context, zmq::socket_type::pub),
      system_pub_(context, zmq::socket_type::pub),
      status_pub_(context, zmq::socket_type::pub),
      sensor_endpoint_(sensor_endpoint), system_endpoint_(system_endpoint),
      status_endpoint_(status_endpoint), cmd_endpoint_(cmd_endpoint) {

  // Bind sockets
  sensor_pub_.bind(sensor_endpoint_);
  system_pub_.bind(system_endpoint_);
  status_pub_.bind(status_endpoint_);

  std::cout << "[McuSerialReaderNode] PUB sockets bound:\n"
            << "  - SENSORS: " << sensor_endpoint_ << "\n"
            << "  - SYSTEM:  " << system_endpoint_ << "\n"
            << "  - STATUS:  " << status_endpoint_ << std::endl;
}

McuSerialReaderNode::~McuSerialReaderNode() { stop(); }

// ── Start / Stop ────────────────────────────────────────────────────────────

void McuSerialReaderNode::start() {
  if (running_.load())
    return;

  // Open serial port
  if (serial_port_.open() != 0) {
    std::cerr << "[McuSerialReaderNode] FATAL: Cannot open serial port. "
              << "Node will run without UART data." << std::endl;
    // Continue running — system topics still work, and the port may
    // become available later (hot-plug)
  }

  running_ = true;

  // Launch Thread 1: Serial I/O
  serial_thread_ = std::make_unique<std::thread>(
      &McuSerialReaderNode::serial_io_thread_func, this);

  // Launch Thread 2: Command REP
  cmd_thread_ = std::make_unique<std::thread>(
      &McuSerialReaderNode::command_rep_thread_func, this);

  // Launch Thread 3: Internet Connectivity Monitor
  connectivity_thread_ = std::make_unique<std::thread>(
      &McuSerialReaderNode::connectivity_monitor_loop, this);

  std::cout << "[McuSerialReaderNode] Started (serial I/O + command REP + "
               "connectivity threads)"
            << std::endl;
}

void McuSerialReaderNode::stop() {
  if (!running_.load())
    return;

  running_ = false;

  if (serial_thread_ && serial_thread_->joinable()) {
    serial_thread_->join();
  }
  if (cmd_thread_ && cmd_thread_->joinable()) {
    cmd_thread_->join();
  }
  if (connectivity_thread_ && connectivity_thread_->joinable()) {
    connectivity_thread_->join();
  }

  serial_port_.close();

  std::cout << "[McuSerialReaderNode] Stopped. Stats: "
            << "ok=" << packets_ok_ << " crc_fail=" << packets_crc_fail_
            << " invalid_id=" << packets_invalid_id_ << std::endl;
}

// ── Thread 1: Serial I/O ───────────────────────────────────────────────────

void McuSerialReaderNode::serial_io_thread_func() {
  std::cout << "[SerialIOThread] Running" << std::endl;

  uint8_t read_buf[128];

  while (running_.load(std::memory_order_relaxed)) {
    // ── 1. Read from UART → push into rx_buffer_ directly ─────────────
    if (serial_port_.is_open()) {
      ssize_t n = serial_port_.read(read_buf, sizeof(read_buf));
      if (n > 0) {
        // Copy into our internal assembly buffer
        const size_t space = rx_buffer_.size() - rx_head_;
        const size_t to_copy = std::min(static_cast<size_t>(n), space);
        std::memcpy(rx_buffer_.data() + rx_head_, read_buf, to_copy);
        rx_head_ += to_copy;

        // ── 2. Decode all complete packets in this thread ─────────────
        OroPacket pkt;
        while (try_decode_packet(pkt)) {
          dispatch_packet(pkt);
        }
      }
    }

    // ── 3. Drain tx_queue → write to UART ─────────────────────────────
    OroPacket tx_pkt;
    while (tx_queue_.try_pop(tx_pkt)) {
      if (serial_port_.is_open()) {
        serial_port_.write(reinterpret_cast<const uint8_t *>(&tx_pkt),
                           sizeof(OroPacket));
      }
    }

    // Yield CPU briefly — 1ms polling interval balances latency vs CPU
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  std::cout << "[SerialIOThread] Exiting" << std::endl;
}

// ── Thread 2: Command REP (TBD — scaffold) ─────────────────────────────────

void McuSerialReaderNode::command_rep_thread_func() {
  std::cout << "[CommandREPThread] Running on: " << cmd_endpoint_ << std::endl;

  try {
    zmq::socket_t rep_socket(context_, zmq::socket_type::rep);
    rep_socket.set(zmq::sockopt::rcvtimeo,
                   500); // 500ms timeout for graceful shutdown
    rep_socket.bind(cmd_endpoint_);

    while (running_.load(std::memory_order_relaxed)) {
      zmq::message_t request;
      auto res = rep_socket.recv(request, zmq::recv_flags::none);

      if (res) {
        std::string req_str(static_cast<char *>(request.data()),
                            request.size());
        // Receive OroPacket from CommandIngressNode
        if (request.size() != sizeof(OroPacket)) {
          std::string reply_str =
              "{\"status\":\"error\",\"message\":\"invalid_packet_size\"}";
          zmq::message_t reply(reply_str.size());
          std::memcpy(reply.data(), reply_str.data(), reply_str.size());
          rep_socket.send(reply, zmq::send_flags::none);
          continue;
        }

        OroPacket pkt;
        std::memcpy(&pkt, request.data(), sizeof(OroPacket));
        uint8_t target_id = GET_ID(pkt.id_seq);
        uint8_t target_seq = GET_SEQ(pkt.id_seq);

        std::cout << "[CommandREPThread] Forwarding command to MCU: ID="
                  << (int)target_id << " SEQ=" << (int)target_seq << std::endl;

        uint8_t ack_key = (target_id & 0x0F) | ((target_seq & 0x0F) << 4);
        auto pending = std::make_shared<PendingAck>();
        {
          std::lock_guard<std::mutex> lock(pending_acks_mtx_);
          pending_acks_[ack_key] = pending;
        }

        // Push to serial TX queue
        if (!tx_queue_.try_push(pkt)) {
          {
            std::lock_guard<std::mutex> lock(pending_acks_mtx_);
            pending_acks_.erase(ack_key);
          }
          std::string reply_str =
              "{\"status\":\"error\",\"message\":\"tx_queue_full\"}";
          zmq::message_t reply(reply_str.size());
          std::memcpy(reply.data(), reply_str.data(), reply_str.size());
          rep_socket.send(reply, zmq::send_flags::none);
          continue;
        }

        // Wait for matching ACK (up to 10 seconds for completion)
        auto start_wait = std::chrono::steady_clock::now();
        bool success = false;
        int32_t ack_status = -1;
        {
          std::unique_lock<std::mutex> lock(pending->mtx);
          success = pending->cv.wait_for(lock, std::chrono::seconds(10),
                                         [&]() { return pending->received; });
          if (success)
            ack_status = pending->status;
        }

        // Cleanup map
        {
          std::lock_guard<std::mutex> lock(pending_acks_mtx_);
          pending_acks_.erase(ack_key);
        }

        auto end_wait = std::chrono::steady_clock::now();
        auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              end_wait - start_wait)
                              .count();

        std::string reply_str;
        if (success) {
          std::string status_str =
              (ack_status == ACK_SUCCESS) ? "success" : "error";
          reply_str = "{\"status\":\"" + status_str +
                      "\",\"latency_ms\":" + std::to_string(latency_ms) +
                      ",\"fw_status\":" + std::to_string(ack_status) + "}";
        } else {
          reply_str = "{\"status\":\"timeout\",\"latency_ms\":" +
                      std::to_string(latency_ms) + "}";
        }

        zmq::message_t reply(reply_str.size());
        std::memcpy(reply.data(), reply_str.data(), reply_str.size());
        rep_socket.send(reply, zmq::send_flags::none);
      }
    }
  } catch (const zmq::error_t &e) {
    if (running_.load()) {
      std::cerr << "[CommandREPThread] ZMQ error: " << e.what() << std::endl;
    }
  }

  std::cout << "[CommandREPThread] Exiting" << std::endl;
}

// ── Main Thread: spin_once ──────────────────────────────────────────────────

void McuSerialReaderNode::spin_once() {
  const uint64_t current_ms = now_ms();

  // ── 1. Publish host-generated system topics ─────────────────────────
  // Note: These use system_pub_, which is NOT used by the Serial thread.
  publish_system_data(current_ms);

  // ── 2. Check periodic fallback publishes ────────────────────────────
  // We keep this in the main thread to avoid complex locking on the filter
  // state.
  filter_.check_periodic(current_ms, [this, current_ms](uint8_t topic_id) {
    const auto &desc = TOPIC_REGISTRY[topic_id];
    const auto &st = filter_.get_state(topic_id);
    MsgHeader hdr{};
    hdr.sensor_id =
        desc.sensor_id >= 0 ? static_cast<uint8_t>(desc.sensor_id) : 0;
    hdr.seq_num = 0; // Periodic re-publish, no new seq
    hdr.timestamp_ms = current_ms;

    switch (desc.category) {
    case TopicCategory::ANALOG:
      send_zmq_analog(desc, hdr, st.last_analog_value);
      break;
    case TopicCategory::DIGITAL:
    case TopicCategory::HEARTBEAT:
      send_zmq_digital(desc, hdr, st.last_digital_state);
      break;
    case TopicCategory::ENCODER:
      send_zmq_encoder(desc, hdr, st.last_encoder_ticks);
      break;
    case TopicCategory::THERMAL:
      // TODO: Not implemented yet
      break;
    }
  });

  // ── 3. Check Hardware Heartbeat (Non-Blocking) ───────────────────────
  // We flag failure if:
  // 1. Packets stop arriving (silent)
  // 2. Sequence number stops incrementing (stagnant)
  if (last_hb_arrival_time_ms_ > 0) {
    bool silent = (current_ms - last_hb_arrival_time_ms_) >= 3000;
    bool stagnant = (current_ms - last_hb_change_time_ms_) >= 3000;

    if ((silent || stagnant) && !hb_stale_) {
      std::cerr << "[McuSerialReaderNode] CRITICAL: Hardware heartbeat "
                << (silent ? "SILENT" : "STAGNANT")
                << " for >= 3s. Possible hardware/firmware failure!"
                << std::endl;
      hb_stale_ = true;
    }
  }
}

// ── Packet Decoder ──────────────────────────────────────────────────────────

bool McuSerialReaderNode::try_decode_packet(OroPacket &out) {
  // Scan forward to find start byte (desync recovery)
  size_t pos = 0;
  while (pos < rx_head_ && rx_buffer_[pos] != START_BYTE) {
    ++pos;
  }

  // Discard bytes before start byte
  if (pos > 0) {
    const size_t remaining = rx_head_ - pos;
    std::memmove(rx_buffer_.data(), rx_buffer_.data() + pos, remaining);
    rx_head_ = remaining;
  }

  // Need at least PACKET_SIZE bytes
  if (rx_head_ < PACKET_SIZE) {
    return false;
  }

  // Copy candidate packet
  std::memcpy(&out, rx_buffer_.data(), PACKET_SIZE);

  // CRC validation
  if (!validate_packet_crc(out)) {
    ++packets_crc_fail_;
    // Skip this start byte and try next
    const size_t remaining = rx_head_ - 1;
    std::memmove(rx_buffer_.data(), rx_buffer_.data() + 1, remaining);
    rx_head_ = remaining;
    return false;
  }

  // Success! Consume the packet from the buffer
  const size_t remaining = rx_head_ - PACKET_SIZE;
  std::memmove(rx_buffer_.data(), rx_buffer_.data() + PACKET_SIZE, remaining);
  rx_head_ = remaining;
  ++packets_ok_;

  return true;
}

// ── Packet Dispatcher ───────────────────────────────────────────────────────

void McuSerialReaderNode::dispatch_packet(const OroPacket &pkt) {
  const uint8_t id = GET_ID(pkt.id_seq);
  const uint8_t seq_num = GET_SEQ(pkt.id_seq);
  const uint8_t msg_type = GET_MSG_TYPE(pkt.msg_type);

  const TopicDescriptor *desc = nullptr;

  if (msg_type == MSG_ACK) {
    uint8_t ack_key = (id & 0x0F) | ((seq_num & 0x0F) << 4);
    std::shared_ptr<PendingAck> pending;
    {
      std::lock_guard<std::mutex> lock(pending_acks_mtx_);
      auto it = pending_acks_.find(ack_key);
      if (it != pending_acks_.end()) {
        pending = it->second;
      }
    }

    if (pending) {
      {
        std::lock_guard<std::mutex> lock(pending->mtx);
        pending->status = extract_value_i32(pkt.value);
        pending->received = true;
      }
      pending->cv.notify_all();
    }
    return;
  }

  if (msg_type == MSG_SENSOR_DATA || msg_type == MSG_HEARTBEAT) {
    if (!is_valid_sensor_id(id)) {
      std::cerr << "[MCU Serial] Invalid sensor ID: " << (int)id
                << " (msg_type: " << (int)msg_type << ")" << std::endl;
      ++packets_invalid_id_;
      return;
    }
    desc = lookup_by_sensor_id(id);
  } else if (msg_type == MSG_PERIPHERAL_STATE) {
    desc = lookup_by_peripheral_id(id);
  }

  // If ID didn't route to any valid TopicDescriptor (or invalid msg_type)
  if (!desc) {
    ++packets_invalid_id_;
    return;
  }

  // Track sequence number (diagnostic only)
  last_seq_[id] = seq_num;

  // Build common header
  const uint64_t ts = now_ms();
  MsgHeader hdr{};
  hdr.sensor_id = id;
  hdr.seq_num = seq_num;
  hdr.timestamp_ms = ts;

  // Route by category
  switch (desc->category) {
  case TopicCategory::HEARTBEAT: {
    // ── Special Heartbeat Monitoring Logic ────────────────────────────
    uint64_t now = now_ms();
    last_hb_arrival_time_ms_ = now;
    if (seq_num != last_hb_seq_) {
      last_hb_seq_ = seq_num;
      last_hb_change_time_ms_ = now;
      if (hb_stale_) {
        std::cout << "[McuSerialReaderNode] INFO: Hardware heartbeat recovered."
                  << std::endl;
        hb_stale_ = false;
      }
    }
    // Proceed to standard digital publish
    publish_digital(*desc, hdr, pkt.value[3]);
    break;
  }
  case TopicCategory::ANALOG: {
    float value = fixed_to_float(pkt.value);
    publish_analog(*desc, hdr, value);
    break;
  }
  case TopicCategory::DIGITAL: {
    uint8_t state = pkt.value[3]; // LSB of value field
    publish_digital(*desc, hdr, state);
    break;
  }
  case TopicCategory::ENCODER: {
    int32_t ticks = extract_value_i32(pkt.value);
    publish_encoder(*desc, hdr, ticks);
    break;
  }
  case TopicCategory::THERMAL: {
    // TODO: Not implemented yet
    break;
  }
  }
}

// ── Publishers ──────────────────────────────────────────────────────────────

void McuSerialReaderNode::publish_analog(const TopicDescriptor &desc,
                                         const MsgHeader &hdr, float value) {
  if (filter_.should_publish(desc, value, 0, 0, hdr.timestamp_ms)) {
    send_zmq_analog(desc, hdr, value);
  }
}

void McuSerialReaderNode::publish_digital(const TopicDescriptor &desc,
                                          const MsgHeader &hdr, uint8_t state) {
  if (filter_.should_publish(desc, 0.0f, state, 0, hdr.timestamp_ms)) {
    send_zmq_digital(desc, hdr, state);
  }
}

void McuSerialReaderNode::publish_encoder(const TopicDescriptor &desc,
                                          const MsgHeader &hdr, int32_t ticks) {
  if (filter_.should_publish(desc, 0.0f, 0, ticks, hdr.timestamp_ms)) {
    send_zmq_encoder(desc, hdr, ticks);
  }
}

// ── ZMQ Low-Level Senders ───────────────────────────────────────────────────

void McuSerialReaderNode::send_zmq_analog(const TopicDescriptor &desc,
                                          const MsgHeader &hdr, float value) {
  AnalogPayload payload{};
  payload.header = hdr;
  payload.value = value;

  zmq::socket_t &socket = get_socket_for_topic(desc.zmq_topic);
  socket.send(zmq::const_buffer(desc.zmq_topic, std::strlen(desc.zmq_topic)),
              zmq::send_flags::sndmore);
  socket.send(zmq::const_buffer(&payload, sizeof(payload)),
              zmq::send_flags::none);
}

void McuSerialReaderNode::send_zmq_digital(const TopicDescriptor &desc,
                                           const MsgHeader &hdr,
                                           uint8_t state) {
  DigitalPayload payload{};
  payload.header = hdr;
  payload.state = state;

  zmq::socket_t &socket = get_socket_for_topic(desc.zmq_topic);
  socket.send(zmq::const_buffer(desc.zmq_topic, std::strlen(desc.zmq_topic)),
              zmq::send_flags::sndmore);
  socket.send(zmq::const_buffer(&payload, sizeof(payload)),
              zmq::send_flags::none);
}

void McuSerialReaderNode::send_zmq_encoder(const TopicDescriptor &desc,
                                           const MsgHeader &hdr,
                                           int32_t ticks) {
  EncoderPayload payload{};
  payload.header = hdr;
  payload.ticks = ticks;

  zmq::socket_t &socket = get_socket_for_topic(desc.zmq_topic);
  socket.send(zmq::const_buffer(desc.zmq_topic, std::strlen(desc.zmq_topic)),
              zmq::send_flags::sndmore);
  socket.send(zmq::const_buffer(&payload, sizeof(payload)),
              zmq::send_flags::none);
}

zmq::socket_t &
McuSerialReaderNode::get_socket_for_topic(const char *zmq_topic) {
  if (std::strncmp(zmq_topic, "/sensors/", 9) == 0) {
    return sensor_pub_;
  } else if (std::strncmp(zmq_topic, "/system/", 8) == 0) {
    return system_pub_;
  } else if (std::strncmp(zmq_topic, "/status/", 8) == 0) {
    return status_pub_;
  }
  // Default to sensors if prefix is unknown
  return sensor_pub_;
}

// ── System Data Publisher ───────────────────────────────────────────────────

void McuSerialReaderNode::publish_system_data(uint64_t current_ms) {
  // /system/time/clock — publish system time on first call and when significant
  // drift
  {
    const auto &desc = TOPIC_REGISTRY[TID_CLOCK];
    float clock_val =
        static_cast<float>(current_ms / 1000.0); // Seconds since epoch

    MsgHeader hdr{};
    hdr.sensor_id = 0;
    hdr.seq_num = clock_seq_;
    hdr.timestamp_ms = current_ms;

    if (filter_.should_publish(desc, clock_val, 0, 0, current_ms)) {
      send_zmq_analog(desc, hdr, clock_val);
      clock_seq_ = (clock_seq_ + 1) & 0x0F;
    }
  }

  // /system/connectivity/state — check actual WiFi status via /sys
  {
    const auto &desc = TOPIC_REGISTRY[TID_CONNECTIVITY];
    bool wifi_up = check_wifi_connectivity();
    bool internet_up = has_internet_.load();

    // 0: Disconnected, 1: Local Only, 2: Internet Access
    uint8_t connectivity_state = wifi_up ? (internet_up ? 2 : 1) : 0;

    // 3. Terminal Log Throttling (Hybrid On-Change + Periodic for Internet)
    bool state_changed = (connectivity_state != last_conn_log_state_);
    bool periodic_internet = (connectivity_state == 2 &&
                              (current_ms - last_conn_log_time_ms_ >= 30000));

    if (state_changed || periodic_internet) {
      if (connectivity_state == 1) {
        std::cerr << "[McuSerialReaderNode] WARNING: Connected to local "
                     "network but NO INTERNET ACCESS!"
                  << std::endl;
      } else if (connectivity_state == 2) {
        // std::cout << "[McuSerialReaderNode] INFO: Wifi is connected and has "
        //              "internet access"
        //           << std::endl;
      } else {
        std::cerr << "[McuSerialReaderNode] WARNING: Wifi isn't connected"
                  << std::endl;
      }
      last_conn_log_time_ms_ = current_ms;
      last_conn_log_state_ = connectivity_state;
    }

    MsgHeader hdr{};
    hdr.sensor_id = 0;
    hdr.seq_num = connectivity_seq_;
    hdr.timestamp_ms = current_ms;

    if (filter_.should_publish(desc, 0.0f, connectivity_state, 0, current_ms)) {
      send_zmq_digital(desc, hdr, connectivity_state);
      connectivity_seq_ = (connectivity_seq_ + 1) & 0x0F;
    }
  }
}

// ── WiFi Status Checker ───────────────────────────────────────────────────

bool McuSerialReaderNode::check_wifi_connectivity() {
  namespace fs = std::filesystem;
  const std::string sys_net = "/sys/class/net/";

  try {
    if (!fs::exists(sys_net))
      return false;

    for (const auto &entry : fs::directory_iterator(sys_net)) {
      std::string iface = entry.path().filename().string();
      // Look for WiFi interfaces (standard prefix "wl")
      if (iface.rfind("wl", 0) == 0) {
        std::ifstream operstate_file(sys_net + iface + "/operstate");
        std::string state;
        if (operstate_file >> state) {
          if (state == "up") {
            return true;
          }
        }
      }
    }
  } catch (const std::exception &e) {
    // Keep silent on errors, just fallback to disconnected
  }

  return false;
}

// ── Internet Connectivity Monitor ──────────────────────────────────────────

void McuSerialReaderNode::connectivity_monitor_loop() {
  std::cout << "[ConnectivityMonitor] Running" << std::endl;

  while (running_.load(std::memory_order_relaxed)) {
    // Perform lightweight probe: ping 8.8.8.8 with 2s timeout
    // Using 8.8.8.8 is more robust than google.com as it avoids DNS resolution
    // overhead/failures.
    int res = std::system("ping -c 1 -W 2 8.8.8.8 > /dev/null 2>&1");

    // exit code 0 means success
    has_internet_ = (res == 0);

    // Sleep for 5s before next probe (check running_ every 1s for graceful
    // shutdown). The 5s interval ensures internet loss is detected within ~7s
    // (5s sleep + 2s ping timeout). Terminal log throttling is handled
    // separately in publish_system_data.
    for (int i = 0; i < 5 && running_.load(std::memory_order_relaxed); ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  std::cout << "[ConnectivityMonitor] Exiting" << std::endl;
}

// ── Time Helper ─────────────────────────────────────────────────────────────

uint64_t McuSerialReaderNode::now_ms() const {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

} // namespace oro
