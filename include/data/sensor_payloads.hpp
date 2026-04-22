#ifndef DATA_SENSOR_PAYLOADS_HPP
#define DATA_SENSOR_PAYLOADS_HPP
// ============================================================================
// sensor_payloads.hpp — Strongly-Typed ZMQ Payload Structures
//
// Defines packed binary structs published as Frame 1 of ZMQ multi-part
// messages. Every payload carries a common MsgHeader for traceability.
//
// Wire format guarantee: all structs are __attribute__((packed)) with
// static_assert size checks. No JSON, no string parsing — pure binary.
// ============================================================================

#include <cstdint>

namespace oro {

// ── Common Message Header ───────────────────────────────────────────────────
// Present in every payload. Provides per-message identity and timing.

struct MsgHeader {
    uint8_t  sensor_id;      // Source sensor/peripheral ID
    uint8_t  seq_num;        // 4-bit rolling sequence number (0–15)
    uint64_t timestamp_ms;   // Host-side epoch milliseconds at dispatch time
} __attribute__((packed));

static_assert(sizeof(MsgHeader) == 10, "MsgHeader must be exactly 10 bytes");

// ── Analog Payload ──────────────────────────────────────────────────────────
// Used for: food weight, water level, temperature, humidity, battery level

struct AnalogPayload {
    MsgHeader header;
    float     value;         // Converted float value (e.g., from fixed-point / 100.0)
} __attribute__((packed));

static_assert(sizeof(AnalogPayload) == 14, "AnalogPayload must be exactly 14 bytes");

// ── Digital Payload ─────────────────────────────────────────────────────────
// Used for: limit switches, home sensor, power switch, heartbeat,
//           connectivity, lid status, pump state, motor state, display, LED

struct DigitalPayload {
    MsgHeader header;
    uint8_t   state;         // 0 = OFF/inactive, 1 = ON/active, or arbitrary state byte
} __attribute__((packed));

static_assert(sizeof(DigitalPayload) == 11, "DigitalPayload must be exactly 11 bytes");

// ── Encoder Payload ─────────────────────────────────────────────────────────
// Used for: optical encoder tick count (camera rotation)

struct EncoderPayload {
    MsgHeader header;
    int32_t   ticks;         // Raw encoder tick count (signed)
} __attribute__((packed));

static_assert(sizeof(EncoderPayload) == 14, "EncoderPayload must be exactly 14 bytes");

// ── Thermal Payload ──────────────────────────────────────────────────────────
// Used for: AMG8833 8x8 IR array frame data

typedef struct __attribute__((packed)) {
    uint32_t  timestamp_ms;    //  4 bytes — monotonic clock
    float     ambient_temp;    //  4 bytes — thermistor °C
    float     pixels[64];      // 256 bytes — 8×8 array °C, row-major
    float     min_temp;        //  4 bytes — frame minimum
    float     max_temp;        //  4 bytes — frame maximum
    uint8_t   overflow;        //  1 byte  — sensor overflow flag
    uint8_t   _pad[3];         //  3 bytes — alignment padding
} amg_frame_t;                 // 276 bytes total

struct ThermalPayload {
    MsgHeader   header;
    amg_frame_t frame;
} __attribute__((packed));

static_assert(sizeof(ThermalPayload) == 286, "ThermalPayload must be exactly 286 bytes");

}  // namespace oro
#endif // DATA_SENSOR_PAYLOADS_HPP
