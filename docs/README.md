# `oro_base_input_layer` — Package Documentation

> **Scope**: Multi-threaded UART/ZMQ middleware that bridges MCU telemetry and external commands into a unified publish-subscribe data bus for the ORo Base platform.

---

## L0 — System Context

The Input Layer is the **central nervous system** of the ORo Base stack. It sits between the physical hardware (MCU + Host sensors) and all upstream consumers (Edge Layer, Web Dashboard, Health Monitor). Every byte of sensor data and every command flows through this node.

```mermaid
graph TB
    subgraph External["External Command Sources"]
        Cloud["☁️ Cloud Backend<br/><i>tcp://*:5555 (REQ-REP)</i>"]
    end

    subgraph IL["oro_base_input_layer (This Package)"]
        direction TB
        CMD["Command Ingress Node"]
        DATA["MCU Serial Reader Node"]
        RADXA["Radxa Drivers Node"]
    end

    subgraph HW["oro_base_hardware_layer"]
        MCU["ESP32-S3 MCU<br/><i>/dev/ttyACM0 @ 115200</i>"]
        SVC["RadxaServices"]
    end

    subgraph Consumers["Downstream Consumers (PUB-SUB)"]
        EDGE["Edge Layer"]
        DASH["Web Dashboard"]
        HEALTH["Health Monitor"]
    end

    Cloud -->|"JSON REQ"| CMD
    CMD -->|"OroPacket via IPC"| DATA
    CMD -->|"JSON call"| SVC
    MCU <-->|"UART 8-byte frames"| DATA
    RADXA -->|"Binary payloads"| Consumers

    DATA -->|"ipc:///tmp/oro_sensors.ipc"| Consumers
    DATA -->|"ipc:///tmp/oro_system.ipc"| Consumers
    DATA -->|"ipc:///tmp/oro_status.ipc"| Consumers

    style IL fill:#1a1a2e,stroke:#e94560,color:#fff,stroke-width:2px
    style CMD fill:#16213e,stroke:#0f3460,color:#fff
    style DATA fill:#16213e,stroke:#0f3460,color:#fff
    style RADXA fill:#16213e,stroke:#0f3460,color:#fff
    style HW fill:#2d132c,stroke:#e94560,color:#fff
    style Consumers fill:#0a3d62,stroke:#0f3460,color:#fff
```

### What This Package Does

| Responsibility | How |
|:---|:---|
| Ingest MCU telemetry | Read 8-byte `OroPacket` from UART, validate CRC, decode, and publish |
| Publish host-generated data | System clock, WiFi connectivity state |
| Accept external commands | Cloud (`:5555`) via ZMQ REQ-REP |
| Route commands to hardware | MCU-bound → binary OroPacket; Host-bound → `RadxaServices` JSON |
| Filter publish noise | Per-topic policies: ON_CHANGE, ON_THRESHOLD, PERIODIC, CONTINUOUS |
| Drive Radxa-native sensors | Thermal IR array (10 Hz) and treat dispenser IR sensors (1 Hz) |

### ZMQ Endpoint Map

| Endpoint | Socket Type | Direction | Data |
|:---|:---|:---|:---|
| `ipc:///tmp/oro_sensors.ipc` | PUB | Outbound | `/sensors/*` topics |
| `ipc:///tmp/oro_system.ipc` | PUB | Outbound | `/system/*` topics |
| `ipc:///tmp/oro_status.ipc` | PUB | Outbound | `/status/*` topics |
| `ipc:///tmp/oro_mcu_cmd.ipc` | REP | Internal | MCU command forwarding |
| `tcp://*:5555` | REP | Inbound | Cloud commands |
| `inproc://command_ingress_queue` | ROUTER-REQ | Internal | Command aggregation |

---

## L1 — Package Architecture

The Input Layer runs as a **single process with 4 threads**, orchestrated from `main.cpp`. Three logical nodes share one ZMQ context and cooperate through lock-free queues and inproc sockets.

```mermaid
graph TB
    subgraph MAIN["main.cpp — Process Entry Point"]
        direction TB
        CTX["zmq::context_t(1)"]
        ML["Main Event Loop<br/><i>1ms poll: spin_once() × 3 nodes</i>"]
    end

    subgraph CMD_NODE["CommandIngressNode"]
        direction TB
        ROUTER["ROUTER Socket<br/><i>inproc://command_ingress_queue</i>"]
        CT["☁️ CloudReceiver Thread<br/><i>REP tcp://*:5555</i>"]
    end

    subgraph MCU_NODE["McuSerialReaderNode"]
        direction TB
        SIO["Serial I/O Thread<br/><i>UART read/write + decode</i>"]
        CREP["Command REP Thread<br/><i>REP ipc://oro_mcu_cmd.ipc</i>"]
        PUB_S["PUB /sensors/*"]
        PUB_Y["PUB /system/*"]
        PUB_T["PUB /status/*"]
    end

    subgraph RADXA_NODE["RadxaDriversNode"]
        direction TB
        TREAT["Treat IR Publisher<br/><i>1 Hz (3 topics)</i>"]
        THERM["Thermal Array Publisher<br/><i>10 Hz (1 topic)</i>"]
    end

    CTX --> CMD_NODE
    CTX --> MCU_NODE
    CTX --> RADXA_NODE

    CT -->|"inproc REQ"| ROUTER
    ROUTER -->|"IPC REQ"| CREP

    CREP -->|"OroPacket"| SIO
    SIO -->|"rx bytes → decode → dispatch"| PUB_S

    RADXA_NODE -->|"shares sensor_pub_"| PUB_S

    ML -.->|"spin_once()"| CMD_NODE
    ML -.->|"spin_once()"| MCU_NODE
    ML -.->|"spin_once()"| RADXA_NODE

    style MAIN fill:#0d1117,stroke:#e94560,color:#fff,stroke-width:2px
    style CMD_NODE fill:#1a1a2e,stroke:#533483,color:#fff
    style MCU_NODE fill:#1a1a2e,stroke:#0f3460,color:#fff
    style RADXA_NODE fill:#1a1a2e,stroke:#0a3d62,color:#fff
```

### Thread Ownership Matrix

| Thread | Owner | Responsibility | Hot Path? |
|:---|:---|:---|:---|
| **Main thread** | `main.cpp` | `spin_once()` on all 3 nodes: periodic publishes, system topics, command routing | Yes |
| **Serial I/O** | `McuSerialReaderNode` | Non-blocking UART read → decode → CRC → dispatch → ZMQ PUB; drain tx_queue → UART write | Yes |
| **Command REP** | `McuSerialReaderNode` | Accept `OroPacket` via IPC → push to tx_queue → wait for ACK (500ms timeout) | No |
| **Cloud Receiver** | `CommandIngressNode` | Accept JSON on tcp:5555 → forward to ROUTER → relay MCU result back | No |

### Directory Layout

```
oro_base_input_layer/
├── CMakeLists.txt                          ← Builds input_layer_node executable
├── include/
│   ├── command/
│   │   ├── cloud_receiver_thread.hpp       ← Cloud REQ-REP receiver
│   │   └── command_ingress_node.hpp        ← Command aggregation + routing
│   └── data/
│       ├── oro_protocol.hpp                ← Host-side OroPacket (mirrors firmware)
│       ├── topic_registry.hpp              ← 36-topic compile-time routing table
│       ├── sensor_payloads.hpp             ← Binary payload structs (Analog/Digital/Encoder/Thermal)
│       ├── mcu_serial_reader_node.hpp      ← UART-to-ZMQ middleware node
│       ├── publish_filter.hpp              ← Per-topic publish policy engine
│       ├── serial_port.hpp                 ← POSIX serial port RAII wrapper
│       └── ring_buffer.hpp                 ← Lock-free SPSC ring buffer
├── src/
│   ├── main.cpp                            ← Process entry point, node orchestration
│   ├── command/
│   │   ├── cloud_receiver_thread.cpp       ← REP listener + inproc forwarding
│   │   └── command_ingress_node.cpp        ← JSON parse → topic lookup → route
│   └── data/
│       ├── mcu_serial_reader_node.cpp      ← Full UART → ZMQ pipeline
│       ├── publish_filter.cpp              ← Policy evaluation logic
│       └── serial_port.cpp                 ← termios 8N1 raw mode config
└── build/
```

### Build Target

| Target | Type | Dependencies |
|:---|:---|:---|
| `input_layer_node` | Executable | `libzmq`, `pthread`, `libradxa_drivers.so`, `nlohmann/json` |

---

## L2 — Component Deep-Dive

### 2.1 McuSerialReaderNode — The Data Pipeline

The core data path that transforms raw UART bytes into typed ZMQ messages. This is the highest-throughput component in the system.

#### Pipeline Stages

```mermaid
graph LR
    A["UART<br/>/dev/ttyACM0<br/>115200 baud"] -->|"non-blocking read"| B["rx_buffer_<br/>512-byte assembly"]
    B -->|"scan for 0xAA"| C["try_decode_packet()<br/>CRC-8 validation"]
    C -->|"valid"| D["dispatch_packet()<br/>LUT field extract"]
    C -->|"CRC fail"| B2["skip byte,<br/>re-scan"]
    D -->|"MSG_SENSOR_DATA"| E["lookup_by_sensor_id()<br/>O(1) SID→TID"]
    D -->|"MSG_PERIPHERAL_STATE"| F["lookup_by_peripheral_id()<br/>O(1) PID→TID"]
    D -->|"MSG_ACK"| G["Notify ack_cv_<br/>(condition variable)"]
    E --> H["publish_analog/digital/encoder()"]
    F --> H
    H -->|"filter_.should_publish()"| I["send_zmq_*()<br/>2-frame multipart"]
    I --> J["sensor_pub_ / system_pub_ / status_pub_<br/>Route by topic prefix"]

    style A fill:#2d132c,stroke:#e94560,color:#fff
    style C fill:#1a1a2e,stroke:#e94560,color:#fff
    style I fill:#0a3d62,stroke:#0f3460,color:#fff
    style J fill:#16213e,stroke:#533483,color:#fff
```

#### Decode Algorithm (`try_decode_packet`)

1. **Scan** `rx_buffer_` for `START_BYTE (0xAA)`, discard preceding garbage
2. **Check** if ≥ 8 bytes available after start byte
3. **Copy** candidate 8-byte frame into `OroPacket`
4. **Validate** CRC-8 over bytes 1–6
5. On CRC failure → skip one byte, re-scan (desync recovery)
6. On success → consume 8 bytes, increment `packets_ok_`

#### Inter-Thread Communication

```mermaid
graph LR
    subgraph "Serial I/O Thread"
        READ["UART Read"] --> DECODE["Decode + Dispatch"]
        TX_DRAIN["Drain tx_queue_"] --> WRITE["UART Write"]
    end

    subgraph "Command REP Thread"
        RX_CMD["Recv OroPacket<br/>via IPC REP"] --> TX_PUSH["tx_queue_.try_push()"]
        TX_PUSH --> WAIT["ack_cv_.wait_for(500ms)"]
        WAIT --> REPLY["Send JSON result"]
    end

    subgraph "Main Thread"
        SPIN["spin_once()"]
        SYS["publish_system_data()"]
        PER["filter_.check_periodic()"]
    end

    TX_PUSH -.->|"lock-free SPSC"| TX_DRAIN
    DECODE -.->|"ack_mtx_ + ack_cv_"| WAIT

    style READ fill:#1a1a2e,stroke:#0f3460,color:#fff
    style DECODE fill:#1a1a2e,stroke:#0f3460,color:#fff
    style RX_CMD fill:#16213e,stroke:#533483,color:#fff
    style SPIN fill:#0a3d62,stroke:#0f3460,color:#fff
```

| Mechanism | Direction | Type | Hot Path? |
|:---|:---|:---|:---|
| `rx_queue_` | Serial → Main | `RingBuffer<uint8_t, 2048>` SPSC | ~~Unused~~ (decode moved to serial thread) |
| `tx_queue_` | CMD REP → Serial | `RingBuffer<OroPacket, 32>` SPSC | Yes |
| `ack_mtx_` + `ack_cv_` | Serial → CMD REP | `mutex` + `condition_variable` | Infrequent (command ACKs only) |

#### ZMQ Socket Routing

The node maintains **3 separate PUB sockets**, routed by topic string prefix:

| Prefix | Socket | IPC Endpoint |
|:---|:---|:---|
| `/sensors/*` | `sensor_pub_` | `ipc:///tmp/oro_sensors.ipc` |
| `/system/*` | `system_pub_` | `ipc:///tmp/oro_system.ipc` |
| `/status/*` | `status_pub_` | `ipc:///tmp/oro_status.ipc` |

#### Host-Generated System Topics

Published from `spin_once()` on the main thread (no UART involvement):

| Topic | Source | Policy | Rate |
|:---|:---|:---|:---|
| `/system/time/clock` | `steady_clock::now()` | PERIODIC | 1s |
| `/system/connectivity/state` | `/sys/class/net/wl*/operstate` | ON_CHANGE | 30s fallback |

#### Diagnostic Counters

| Counter | Meaning |
|:---|:---|
| `packets_ok_` | Successfully decoded and dispatched packets |
| `packets_crc_fail_` | Packets dropped due to CRC-8 mismatch |
| `packets_invalid_id_` | Valid CRC but unmapped SID/PID |

---

### 2.2 Topic Registry — The Routing Brain

A **compile-time constexpr lookup table** of 36 topic descriptors. Zero runtime cost, zero heap allocation. This is the single source of truth for all data routing decisions.

#### Topic Descriptor Schema

```cpp
struct TopicDescriptor {
    uint8_t       topic_id;    // Index into TOPIC_REGISTRY[N]
    TopicCategory category;    // ANALOG | DIGITAL | ENCODER | THERMAL
    PublishPolicy policy;      // ON_CHANGE | ON_THRESHOLD | PERIODIC | CONTINUOUS | ON_UPDATE
    TopicSource   source;      // UART (from MCU) | SYSTEM (host-generated)
    const char*   zmq_topic;   // ZMQ Frame 0 topic string
    float         threshold;   // Delta for ON_THRESHOLD (0.0 if N/A)
    uint32_t      period_ms;   // Interval for PERIODIC / fallback (0 if N/A)
    int8_t        sensor_id;   // Mapped SID/PID for UART (-1 if SYSTEM)
};
```

#### Complete Topic Map

| TID | ZMQ Topic | Category | Policy | Source | Threshold | Period |
|:---|:---|:---|:---|:---|:---|:---|
| 0 | `/sensors/food_weight/bowl_1` | ANALOG | ON_THRESHOLD | UART | 1.0 | 5s |
| 1 | `/sensors/food_weight/bowl_2` | ANALOG | ON_THRESHOLD | UART | 1.0 | 5s |
| 2 | `/sensors/water_level/tank` | ANALOG | ON_THRESHOLD | UART | 0.5 | 5s |
| 3 | `/sensors/water_level/bowl` | ANALOG | ON_THRESHOLD | UART | 0.5 | 3s |
| 4 | `/sensors/environment/humidity` | ANALOG | ON_THRESHOLD | UART | 0.5 | 10s |
| 5 | `/sensors/environment/temperature` | ANALOG | ON_THRESHOLD | UART | 0.1 | 10s |
| 6 | `/sensors/camera_rotation/limit_switch_1` | DIGITAL | ON_CHANGE | UART | — | 10s |
| 7 | `/sensors/camera_rotation/limit_switch_2` | DIGITAL | ON_CHANGE | UART | — | 10s |
| 8 | `/sensors/camera_rotation/optical_encoder` | ENCODER | CONTINUOUS | UART | — | — |
| 9 | `/sensors/camera_rotation/home` | DIGITAL | ON_CHANGE | UART | — | — |
| 10 | `/system/power/switch` | DIGITAL | ON_CHANGE | UART | — | — |
| 11 | `/system/power/battery_level` | ANALOG | ON_THRESHOLD | UART | 1.0 | 60s |
| 12 | `/system/time/clock` | ANALOG | PERIODIC | SYSTEM | — | 1s |
| 13 | `/system/device/heartbeat` | DIGITAL | PERIODIC | UART | — | 10s |
| 14 | `/system/connectivity/state` | DIGITAL | ON_CHANGE | SYSTEM | — | 30s |
| 15–16 | `/status/lid/{1,2}` | DIGITAL | ON_CHANGE | UART | — | — |
| 17 | `/status/water_pump` | DIGITAL | ON_CHANGE | UART | — | — |
| 18 | `/status/camera_rotation/stepper_motor` | DIGITAL | ON_CHANGE | UART | — | — |
| 19 | `/status/display/seven_segment` | ANALOG | ON_UPDATE | UART | — | — |
| 20 | `/status/led_indicator` | ANALOG | ON_UPDATE | UART | — | — |
| 21–30 | `/commands/*` | ANALOG | ON_UPDATE | SYSTEM | — | — |
| 31–33 | `/sensors/treat/*` | DIGITAL | ON_CHANGE | SYSTEM | — | 1s |
| 34 | `/sensors/thermal/ir_array` | THERMAL | PERIODIC | SYSTEM | — | 100ms |

#### Lookup Performance

| Function | Lookup | Cost |
|:---|:---|:---|
| `lookup_by_topic_id(tid)` | Direct array index | O(1), zero branches |
| `lookup_by_sensor_id(sid)` | Static `SID_TO_TID[16]` LUT | O(1), one indirection |
| `lookup_by_peripheral_id(pid)` | Static `PID_TO_TID[16]` LUT | O(1), one indirection |

---

### 2.3 Publish Filter — Noise Suppression Engine

Stateful filter that enforces per-topic publish policies. Prevents redundant ZMQ publishes by tracking last-known values and timestamps for each of the 36 topics.

#### Policy Decision Matrix

```mermaid
graph TD
    IN["Incoming packet for Topic N"] --> SW{"desc.policy?"}

    SW -->|ON_CHANGE| OC{"state ≠ last_state?"}
    OC -->|Yes| PUB["✅ PUBLISH"]
    OC -->|No| FB1{"Δt ≥ period_ms?"}
    FB1 -->|Yes| PUB
    FB1 -->|No| DROP["❌ SUPPRESS"]

    SW -->|ON_THRESHOLD| OT{"|Δvalue| ≥ threshold?"}
    OT -->|Yes| PUB
    OT -->|No| FB2{"Δt ≥ period_ms?"}
    FB2 -->|Yes| PUB
    FB2 -->|No| DROP

    SW -->|PERIODIC| PER{"Δt ≥ period_ms × 0.9?"}
    PER -->|Yes| PUB
    PER -->|No| DROP

    SW -->|CONTINUOUS| PUB
    SW -->|ON_UPDATE| PUB

    style PUB fill:#0a3d62,stroke:#0f3460,color:#fff
    style DROP fill:#2d132c,stroke:#e94560,color:#fff
```

#### Per-Topic State Cache

```cpp
struct TopicState {
    float    last_analog_value;     // Last published analog reading
    uint8_t  last_digital_state;    // Last published digital state (0xFF = never)
    int32_t  last_encoder_ticks;    // Last published encoder count
    uint64_t last_publish_time_ms;  // Epoch ms of last publish
    bool     ever_published;        // True after first publish
};
```

#### Periodic Fallback Re-Publish

The `check_periodic()` method is called from `spin_once()` and scans all 36 topics. For any topic where `Δt ≥ 1.5 × period_ms`, it triggers a re-publish using the **last cached value** — ensuring downstream consumers always have fresh data even if the upstream sensor goes silent.

---

### 2.4 CommandIngressNode — The Command Router

Aggregates external commands from multiple sources and routes them to the appropriate executor.

#### Command Flow Architecture

```mermaid
sequenceDiagram
    participant Client as Cloud / MAPP Client
    participant RX as Receiver Thread<br/>(Cloud)
    participant ROUTER as CommandIngressNode<br/>(ROUTER socket)
    participant LUT as TOPIC_REGISTRY<br/>(Lookup)
    participant SVC as RadxaServices<br/>(Host Commands)
    participant REP as MCU Command REP<br/>(IPC)
    participant MCU as ESP32-S3

    Client->>RX: JSON {"topic": "...", "value": N}
    RX->>ROUTER: inproc REQ forward

    ROUTER->>LUT: Find TopicDescriptor by topic string

    alt TopicSource::SYSTEM (non-camera)
        ROUTER->>SVC: process_command(json)
        SVC-->>ROUTER: JSON result
    else TopicSource::UART or camera_rotation
        ROUTER->>ROUTER: Build OroPacket<br/>(PACK_MSG_TYPE, PACK_ID_SEQ, CRC)
        ROUTER->>REP: Send OroPacket via IPC REQ
        REP->>MCU: Push to tx_queue_ → UART write
        MCU-->>REP: MSG_ACK (via ack_cv_)
        REP-->>ROUTER: JSON {status, latency_ms}
    end

    ROUTER-->>RX: JSON result
    RX-->>Client: REP response
```

#### Topic-Based Routing Logic

1. **Parse** incoming JSON for `topic` and `value` fields
2. **Scan** `TOPIC_REGISTRY` for matching `zmq_topic` string
3. **Route** based on `TopicSource`:
   - `SYSTEM` → `RadxaServices::process_command()` (JSON in, JSON out)
   - `UART` or `camera_rotation` → Build `OroPacket`, send via IPC to MCU command REP thread
4. **Reply** JSON result back through the ROUTER → REQ → REP chain to the original client

#### Sequence Number Management

- `cmd_seq_` is a 4-bit rolling counter (0–15) maintained by `CommandIngressNode`
- Each outgoing `MSG_COMMAND` packet carries the current `cmd_seq_`, then increments
- The MCU echoes the same `seq` in its `MSG_ACK`, enabling correlation

---

### 2.5 Receiver Threads — Cloud

Initially separate threads, each runs a ZMQ REP socket on a dedicated TCP port and forwards commands to the `CommandIngressNode` via an inproc REQ socket. Currently only Cloud receiver is implemented.

#### Thread Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Idle: start()
    Idle --> Listening: Bind REP + Connect inproc REQ
    Listening --> Processing: recv() returns (500ms timeout)
    Processing --> Forwarding: Forward JSON to ROUTER
    Forwarding --> WaitingACK: Block on inproc recv()
    WaitingACK --> Replying: Got MCU result
    WaitingACK --> Replying: Internal timeout
    Replying --> Listening: Send REP response
    Listening --> [*]: stop() → running_ = false
```

| Parameter | CloudReceiver |
|:---|:---|
| TCP endpoint | `tcp://*:5555` |
| Internal endpoint | `inproc://command_ingress_queue` |
| Recv timeout | 500ms |
| Thread join | On `stop()` call |

> **Design Note**: The ROUTER socket uses ZMQ identity frames to demultiplex responses back to the correct originating thread.

---

### 2.6 Sensor Payloads — Binary Wire Format

All ZMQ messages use **packed binary structs** with no JSON overhead. Every payload carries a common `MsgHeader` for traceability.

#### Common Header (10 bytes)

```
┌──────────┬──────────┬──────────────────┐
│ sensor_id│ seq_num  │  timestamp_ms    │
│ 1B       │ 1B       │ 8B (uint64)      │
└──────────┴──────────┴──────────────────┘
```

#### Payload Types

| Struct | Size | Fields | Used For |
|:---|:---|:---|:---|
| `AnalogPayload` | 14B | `header` + `float value` | Weight, water level, temp, humidity, battery |
| `DigitalPayload` | 11B | `header` + `uint8_t state` | Switches, heartbeat, connectivity, lid, pump |
| `EncoderPayload` | 14B | `header` + `int32_t ticks` | Camera encoder tick count |
| `ThermalPayload` | 286B | `header` + `amg_frame_t` | 8×8 thermal IR array frame |

#### Thermal Frame Detail (`amg_frame_t`, 276 bytes)

| Field | Type | Size | Description |
|:---|:---|:---|:---|
| `timestamp_ms` | `uint32_t` | 4B | Monotonic clock |
| `ambient_temp` | `float` | 4B | Thermistor reading °C |
| `pixels[64]` | `float[64]` | 256B | 8×8 array, row-major, °C |
| `min_temp` | `float` | 4B | Frame minimum |
| `max_temp` | `float` | 4B | Frame maximum |
| `overflow` | `uint8_t` | 1B | Sensor overflow flag |
| `_pad[3]` | `uint8_t[3]` | 3B | Alignment padding |

---

### 2.7 Ring Buffer — Lock-Free SPSC Queue

Template-based single-producer, single-consumer ring buffer used for inter-thread communication on the hot path.

#### Design Properties

| Property | Detail |
|:---|:---|
| **Lock-free** | Uses `std::atomic` with acquire/release ordering — no mutexes |
| **Power-of-2 capacity** | Enables `mask = N-1` modular arithmetic (no division) |
| **Cache-line aligned** | `head_`, `tail_`, `buffer_` on separate 64-byte cache lines |
| **Trivially copyable** | `static_assert` enforces `std::is_trivially_copyable_v<T>` |
| **Bulk operations** | `push_bulk()` / `pop_bulk()` handle wrap-around in ≤ 2 memcpy calls |

#### Instantiations

| Instance | Template | Usage |
|:---|:---|:---|
| `rx_queue_` | `RingBuffer<uint8_t, 2048>` | Reserved for future use (decode moved to serial thread) |
| `tx_queue_` | `RingBuffer<OroPacket, 32>` | CMD REP → Serial I/O: outgoing command packets |

---

### 2.8 Serial Port — POSIX UART Wrapper

RAII wrapper for `/dev/ttyACM0` with termios configuration.

#### Configuration

| Parameter | Value |
|:---|:---|
| Baud rate | 115200 (configurable) |
| Data bits | 8 |
| Parity | None |
| Stop bits | 1 |
| Flow control | None (no HW or SW flow control) |
| Mode | Raw (no canonical processing) |
| Read | Non-blocking (`VMIN=0, VTIME=0`) |
| Write | Blocking |

---

## Process Lifecycle

```mermaid
sequenceDiagram
    participant M as main()
    participant CMD as CommandIngressNode
    participant MCU as McuSerialReaderNode
    participant RDX as RadxaDriversNode

    M->>M: Register SIGINT/SIGTERM handlers
    M->>M: Create zmq::context_t(1)

    M->>CMD: Construct (cloud, mapp, mcu endpoints)
    M->>MCU: Construct (sensor, system, status, cmd endpoints)
    M->>RDX: Construct (shared sensor_pub_ reference)

    M->>CMD: start() → spawn CloudReceiver
    M->>MCU: start() → open UART + spawn Serial I/O + Command REP
    M->>RDX: start()

    loop Every 1ms until SIGINT
        M->>CMD: spin_once() → drain ROUTER
        M->>MCU: spin_once() → system topics + periodic re-publish
        M->>RDX: spin_once() → treat IR + thermal array
    end

    M->>CMD: stop() → join receiver threads
    M->>MCU: stop() → join serial/cmd threads, close UART
    M->>RDX: stop()
    M->>M: zmq::context_t RAII cleanup
```

---

## Cross-Package Dependencies

```mermaid
graph LR
    subgraph "oro_base_input_layer"
        MAIN["main.cpp"]
        CIN["CommandIngressNode"]
        MSR["McuSerialReaderNode"]
        TR["topic_registry.hpp"]
        PF["PublishFilter"]
        SP["sensor_payloads.hpp"]
        PROTO["oro_protocol.hpp"]
        RB["RingBuffer"]
        SERIAL["SerialPort"]
    end

    subgraph "oro_base_hardware_layer"
        RDN["RadxaDriversNode"]
        RS["RadxaServices"]
    end

    subgraph "External"
        ZMQ["libzmq"]
        JSON["nlohmann/json"]
        PTHREAD["pthread"]
    end

    MAIN --> CIN
    MAIN --> MSR
    MAIN --> RDN
    CIN --> RS
    CIN --> TR
    CIN --> PROTO
    MSR --> TR
    MSR --> PF
    MSR --> SP
    MSR --> PROTO
    MSR --> RB
    MSR --> SERIAL
    RDN --> TR
    RDN --> SP

    CIN --> ZMQ
    CIN --> JSON
    MSR --> ZMQ
    MAIN --> PTHREAD

    style MAIN fill:#e94560,stroke:#1a1a2e,color:#fff
    style TR fill:#533483,stroke:#1a1a2e,color:#fff
    style PROTO fill:#533483,stroke:#1a1a2e,color:#fff
```
