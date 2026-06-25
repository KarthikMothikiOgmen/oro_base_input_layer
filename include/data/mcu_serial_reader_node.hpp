#ifndef DATA_MCU_SERIAL_READER_NODE_HPP
#define DATA_MCU_SERIAL_READER_NODE_HPP
// ============================================================================
// mcu_serial_reader_node.hpp — MCU Serial Reader Node
//
// Multi-threaded UART-to-ZMQ middleware node. Reads 8-byte OroPacket frames
// from /dev/ttyACM0, validates CRC-8, decodes fields, and publishes
// strongly-typed binary payloads over ZMQ PUB-SUB topics.
//
// Threading model:
//   Thread 1 (Serial I/O)  — continuous non-blocking read/write on UART
//   Thread 2 (Command REP) — ZMQ REP socket for client commands (TBD)
//   Main thread             — decode pipeline + ZMQ PUB dispatch
//
// TODO: load serial port config from config.yaml
// ============================================================================

#include "data/oro_protocol.hpp"
#include "data/sensor_payloads.hpp"
#include "data/topic_registry.hpp"
#include "data/publish_filter.hpp"
#include "data/serial_port.hpp"
#include "data/ring_buffer.hpp"

#include <zmq.hpp>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <map>

#include "radxa_drivers/camhead_rotation.hpp"

namespace oro {

class McuSerialReaderNode {
public:
    // @param context           Shared ZMQ context
    // @param sensor_endpoint   IPC endpoint for sensor data ("/sensors/...")
    // @param system_endpoint   IPC endpoint for system data ("/system/...")
    // @param status_endpoint   IPC endpoint for status data ("/status/...")
    // @param cmd_endpoint      IPC endpoint for REP socket
    McuSerialReaderNode(zmq::context_t& context,
                        const std::string& sensor_endpoint,
                        const std::string& system_endpoint,
                        const std::string& status_endpoint,
                        const std::string& cmd_endpoint);
    ~McuSerialReaderNode();

    // Non-copyable
    McuSerialReaderNode(const McuSerialReaderNode&) = delete;
    McuSerialReaderNode& operator=(const McuSerialReaderNode&) = delete;

    // Launch serial I/O and command REP threads
    void start();

    // Signal threads to stop and join them
    void stop();

    // Called from the main event loop. Drains rx_queue, decodes packets,
    // dispatches to ZMQ PUB. Not thread-safe — call from main thread only.
    void spin_once();

    // ── Shared Access ───────────────────────────────────────────────────
    zmq::socket_t& get_sensor_publisher() { return sensor_pub_; }

    // ── Diagnostics ─────────────────────────────────────────────────────

    uint64_t packets_ok()        const { return packets_ok_; }
    uint64_t packets_crc_fail()  const { return packets_crc_fail_; }
    uint64_t packets_invalid_id() const { return packets_invalid_id_; }

private:
    // ── Thread functions ────────────────────────────────────────────────

    // Thread 1: Continuous serial read/write
    void serial_io_thread_func();

    // Thread 2: ZMQ REP command handler (TBD — scaffold)
    void command_rep_thread_func();

    // ── Decode pipeline ─────────────────────────────────────────────────

    // Scan rx_buffer for a valid packet. Returns true if decoded.
    bool try_decode_packet(OroPacket& out);

    // Route a validated packet to the correct publisher
    void dispatch_packet(const OroPacket& pkt);

    // ── Publishers (zero heap alloc) ────────────────────────────────────

    void publish_analog(const TopicDescriptor& desc,
                        const MsgHeader& hdr, float value);

    void publish_digital(const TopicDescriptor& desc,
                         const MsgHeader& hdr, uint8_t state);

    void publish_encoder(const TopicDescriptor& desc,
                         const MsgHeader& hdr, int32_t ticks);

    // ── System Data Publisher ───────────────────────────────────────────────
    void publish_system_data(uint64_t current_ms);
    bool check_wifi_connectivity();

    // ── Time Helper ─────────────────────────────────────────────────────────────

    uint64_t now_ms() const;
    
    // ── ZMQ Low-Level Senders (Zero-Filter) ─────────────────────────────
    // These methods perform the actual ZMQ multipart send. They assume
    // the calling logic has already performed all necessary filtering.
    
    void send_zmq_analog(const TopicDescriptor& desc, 
                         const MsgHeader& hdr, float value);
                         
    void send_zmq_digital(const TopicDescriptor& desc, 
                          const MsgHeader& hdr, uint8_t state);
                          
    void send_zmq_encoder(const TopicDescriptor& desc, 
                          const MsgHeader& hdr, int32_t ticks);

    // Resolve the appropriate ZMQ socket based on topic string prefix
    zmq::socket_t& get_socket_for_topic(const char* zmq_topic);

    // ── Members ─────────────────────────────────────────────────────────
 
    // ZMQ
    zmq::context_t& context_;
    zmq::socket_t   sensor_pub_;
    zmq::socket_t   system_pub_;
    zmq::socket_t   status_pub_;
    
    std::string     sensor_endpoint_;
    std::string     system_endpoint_;
    std::string     status_endpoint_;
    std::string     cmd_endpoint_;

    // Serial
    SerialPort serial_port_;

    // Threading
    std::atomic<bool> running_{false};
    std::unique_ptr<std::thread> serial_thread_;
    std::unique_ptr<std::thread> cmd_thread_;

    // Inter-thread queues (lock-free SPSC)
    RingBuffer<uint8_t, 2048>    rx_queue_;   // Serial → Main (raw bytes)
    RingBuffer<OroPacket, 32>    tx_queue_;   // Main → Serial (command packets)

    // Packet assembly buffer (main thread only — not shared)
    std::array<uint8_t, 512> rx_buffer_{};
    size_t rx_head_ = 0;

    // Publish filter engine
    PublishFilter filter_;

    // Diagnostics
    uint64_t packets_ok_         = 0;
    uint64_t packets_crc_fail_   = 0;
    uint64_t packets_invalid_id_ = 0;

    // Per-sensor sequence tracking (for debug/diagnostics)
    std::array<uint8_t, SID_COUNT> last_seq_{};

    // Command-ACK Synchronization
    struct PendingAck {
        std::mutex mtx;
        std::condition_variable cv;
        int32_t status = -1;
        bool received = false;
    };
    std::mutex pending_acks_mtx_;
    std::map<uint8_t, std::shared_ptr<PendingAck>> pending_acks_;

    // Rolling sequence numbers for host-generated system topics (4-bit, 0–15)
    uint8_t clock_seq_ = 0;
    uint8_t connectivity_seq_ = 0;
    uint8_t limit_switch1_seq_ = 0;
    uint8_t limit_switch2_seq_ = 0;
    uint8_t home_sensor_seq_ = 0;
    uint8_t stepper_status_seq_ = 0;
    uint8_t stepper_encoder_seq_ = 0;

    // Hardware Heartbeat Monitoring (Thread-safe)
    std::atomic<uint64_t> last_hb_arrival_time_ms_{0};
    std::atomic<uint64_t> last_hb_change_time_ms_{0};
    std::atomic<uint8_t> last_hb_seq_{0xFF};
    std::atomic<bool> hb_stale_{false};

    // Internet Connectivity Monitoring
    std::atomic<bool> has_internet_{false};
    uint8_t last_conn_log_state_ = 0xFF; // Initial invalid state to trigger first log
    uint64_t last_conn_log_time_ms_ = 0;
    std::unique_ptr<std::thread> connectivity_thread_;

    void connectivity_monitor_loop();

    // Cam Head Rotation (handled locally by Radxa GPIOs)
    std::unique_ptr<CamHeadRotationNode> cam_head_;
    void publish_cam_head_data(uint64_t current_ms);
};

}  // namespace oro
#endif // DATA_MCU_SERIAL_READER_NODE_HPP
