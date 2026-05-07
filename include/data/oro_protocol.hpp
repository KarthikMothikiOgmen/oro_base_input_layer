#ifndef DATA_ORO_PROTOCOL_HPP
#define DATA_ORO_PROTOCOL_HPP
// ============================================================================
// oro_protocol.hpp — ORo Serial Protocol V2
//
// Defines the 8-byte fixed-size binary packet format for MCU ↔ Host
// communication. This header is the single source of truth for:
//   - OroPacket struct layout
//   - CRC-8 integrity validation (polynomial 0x07)
//   - Bit-packing macros for msg_type and id_seq fields
//   - Sensor/Peripheral ID enumerations
//   - Message type constants
//
// TODO: load config from config.yaml for future protocol revisions
// ============================================================================

#include <cstddef>
#include <cstdint>

namespace oro {

// ── Wire Constants ──────────────────────────────────────────────────────────

static constexpr uint8_t START_BYTE = 0xAA;
static constexpr size_t PACKET_SIZE = 8;

// ── Message Types (lower 6 bits of msg_type byte) ──────────────────────────

static constexpr uint8_t MSG_SENSOR_DATA = 0x01;
static constexpr uint8_t MSG_PERIPHERAL_STATE = 0x02;
static constexpr uint8_t MSG_HEARTBEAT = 0x03;
static constexpr uint8_t MSG_COMMAND = 0x04;
static constexpr uint8_t MSG_ACK = 0x05;

// ── Priority Levels (upper 2 bits of msg_type byte) ────────────────────────

static constexpr uint8_t PRIO_LOW = 0x00;  // 0b00
static constexpr uint8_t PRIO_MED = 0x01;  // 0b01
static constexpr uint8_t PRIO_HIGH = 0x02; // 0b10
static constexpr uint8_t PRIO_CRIT = 0x03; // 0b11

// ── Bit-packing macros: msg_type byte ───────────────────────────────────────
//    [7:6] = priority (2 bits)
//    [5:0] = message type (6 bits)

inline constexpr uint8_t GET_MSG_TYPE(uint8_t b) { return b & 0x3F; }
inline constexpr uint8_t GET_PRIORITY(uint8_t b) { return (b >> 6) & 0x03; }
inline constexpr uint8_t PACK_MSG_TYPE(uint8_t prio, uint8_t type) {
  return static_cast<uint8_t>(((prio & 0x03) << 6) | (type & 0x3F));
}

// ── Bit-packing macros: id_seq byte ─────────────────────────────────────────
//    [7:4] = sequence number (4 bits, 0–15 rolling)
//    [3:0] = sensor/peripheral ID (4 bits, 0–15)

inline constexpr uint8_t GET_ID(uint8_t b) { return b & 0x0F; }
inline constexpr uint8_t GET_SEQ(uint8_t b) { return (b >> 4) & 0x0F; }
inline constexpr uint8_t PACK_ID_SEQ(uint8_t seq, uint8_t id) {
  return static_cast<uint8_t>(((seq & 0x0F) << 4) | (id & 0x0F));
}

// ── Sensor IDs (4-bit, max 16) ──────────────────────────────────────────────
// These correspond to the firmware SID_* definitions.
// The user will modify these later as needed.

enum SensorID : uint8_t {
  SID_LOAD_LEFT = 0x00,   // Food weight bowl 1
  SID_LOAD_RIGHT = 0x01,  // Food weight bowl 2
  SID_WATER_LEVEL = 0x02, // Water level tank
  SID_WATER_BOWL = 0x03,  // Water level bowl
  SID_HUMIDITY = 0x04,    // Ambient humidity
  SID_TEMPERATURE = 0x05, // Ambient temperature
  SID_LIMIT_SW1 = 0x06,   // Camera rotation limit switch 1
  SID_LIMIT_SW2 = 0x07,   // Camera rotation limit switch 2
  SID_ENCODER = 0x08,     // Camera rotation optical encoder
  SID_HOME_SENSOR = 0x09, // Camera home position sensor
  SID_POWER_SW = 0x0A,    // Power switch
  SID_BATTERY = 0x0B,     // Battery level
  SID_HEARTBEAT = 0x0C,   // Device heartbeat
  SID_NAV_BUTTON = 0x0D,  // Navigation button state (display cycling)
  SID_LID1_HALL = 0x0E,   // Lid 1 Hall sensors (bits 0=closed, 1=opened)
  SID_LID2_HALL = 0x0F,   // Lid 2 Hall sensors (bits 0=closed, 1=opened)

  SID_COUNT = 16 // Max sensor IDs in 4-bit field
};

// ── Peripheral IDs (4-bit, max 16) ──────────────────────────────────────────
// These correspond to the firmware PID_* definitions.
// Multiplexed with SensorID via MSG_PERIPHERAL_STATE.

enum PeripheralID : uint8_t {
  PID_PUMP = 0x00,
  PID_LID1_STEPPER = 0x01,
  PID_LID2_STEPPER = 0x02,
  PID_CAMERA_STEPPER = 0x03,
  PID_DISPLAY = 0x04,
  PID_INDICATOR_LED = 0x05,
  PID_CAMERA_SERVO = 0x06,
};

// ── ACK Status Codes (used in MSG_ACK payload) ─────────────────────────────

static constexpr int32_t ACK_ERROR = 0;
static constexpr int32_t ACK_SUCCESS = 1;
static constexpr int32_t ACK_TIMEOUT = 2;
static constexpr int32_t ACK_BUSY = 3;
static constexpr int32_t ACK_INVALID = 4;

// ── OroPacket: 8-byte fixed-size wire format ────────────────────────────────

struct OroPacket {
  uint8_t start;    // Byte 0: Start-of-frame marker (0xAA)
  uint8_t msg_type; // Byte 1: [7:6]=priority, [5:0]=type
  uint8_t id_seq;   // Byte 2: [7:4]=seq_num,  [3:0]=sensor_id
  uint8_t value[4]; // Bytes 3-6: int32_t big-endian payload
  uint8_t crc;      // Byte 7: CRC-8 over bytes[1..6]
} __attribute__((packed));

static_assert(sizeof(OroPacket) == 8, "OroPacket must be exactly 8 bytes");

// ── CRC-8 (polynomial 0x07) ─────────────────────────────────────────────────
// Computes CRC-8 over a byte range. Used to validate bytes 1–6 of OroPacket.
// Standard CRC-8 with polynomial x^8 + x^2 + x^1 + x^0.

inline uint8_t oro_crc8(const uint8_t *data, size_t len) {
  uint8_t crc = 0x00;
  for (size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (uint8_t bit = 0; bit < 8; ++bit) {
      if (crc & 0x80) {
        crc = static_cast<uint8_t>((crc << 1) ^ 0x07);
      } else {
        crc = static_cast<uint8_t>(crc << 1);
      }
    }
  }
  return crc;
}

// ── Payload value helpers ───────────────────────────────────────────────────

// Extract int32_t from big-endian value[4] field
inline int32_t extract_value_i32(const uint8_t value[4]) {
  return static_cast<int32_t>((static_cast<uint32_t>(value[0]) << 24) |
                              (static_cast<uint32_t>(value[1]) << 16) |
                              (static_cast<uint32_t>(value[2]) << 8) |
                              (static_cast<uint32_t>(value[3])));
}

// Convert fixed-point int32 to float (value / 100.0)
inline float fixed_to_float(const uint8_t value[4]) {
  return static_cast<float>(extract_value_i32(value)) / 100.0f;
}

// Pack int32_t into big-endian value[4] field
inline void pack_value_i32(uint8_t value[4], int32_t v) {
  auto u = static_cast<uint32_t>(v);
  value[0] = static_cast<uint8_t>((u >> 24) & 0xFF);
  value[1] = static_cast<uint8_t>((u >> 16) & 0xFF);
  value[2] = static_cast<uint8_t>((u >> 8) & 0xFF);
  value[3] = static_cast<uint8_t>((u)&0xFF);
}

// ── Packet validation helpers ───────────────────────────────────────────────

// Validate CRC of a received packet (bytes 1–6)
inline bool validate_packet_crc(const OroPacket &pkt) {
  uint8_t computed = oro_crc8(&pkt.msg_type, 6);
  return computed == pkt.crc;
}

// Check if start byte is valid
inline bool is_valid_start(uint8_t byte) { return byte == START_BYTE; }

// Check if sensor ID is within valid range
inline bool is_valid_sensor_id(uint8_t id) { return id < SID_COUNT; }

} // namespace oro
#endif // DATA_ORO_PROTOCOL_HPP
