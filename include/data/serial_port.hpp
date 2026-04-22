#ifndef DATA_SERIAL_PORT_HPP
#define DATA_SERIAL_PORT_HPP
// ============================================================================
// serial_port.hpp — POSIX Serial Port Wrapper
//
// RAII wrapper for the hardware UART connection to the ESP32 MCU.
// Handles open/close, termios configuration, non-blocking read, and write.
//
// TODO: load port path and baud rate from config.yaml
// ============================================================================

#include <cstdint>
#include <cstddef>
#include <string>

namespace oro {

class SerialPort {
public:
    // Default configuration — hardcoded for now
    // TODO: load from config.yaml
    static constexpr const char*    DEFAULT_PORT      = "/dev/ttyACM0";
    static constexpr int            DEFAULT_BAUD_RATE = 115200;

    explicit SerialPort(const std::string& port = DEFAULT_PORT,
                        int baud_rate = DEFAULT_BAUD_RATE);
    ~SerialPort();

    // Non-copyable, non-movable
    SerialPort(const SerialPort&) = delete;
    SerialPort& operator=(const SerialPort&) = delete;
    SerialPort(SerialPort&&) = delete;
    SerialPort& operator=(SerialPort&&) = delete;

    // Open and configure the serial port. Returns 0 on success, -1 on error.
    int open();

    // Close the serial port.
    void close();

    // Non-blocking read. Returns bytes read (>= 0) or -1 on error.
    ssize_t read(uint8_t* buf, size_t max_len);

    // Blocking write. Returns bytes written or -1 on error.
    ssize_t write(const uint8_t* buf, size_t len);

    // Check if port is currently open
    bool is_open() const { return fd_ >= 0; }

    // Get the file descriptor (for advanced polling if needed)
    int fd() const { return fd_; }

private:
    std::string port_path_;
    int baud_rate_;
    int fd_ = -1;
};

}  // namespace oro
#endif // DATA_SERIAL_PORT_HPP
