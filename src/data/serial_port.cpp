// ============================================================================
// serial_port.cpp — POSIX Serial Port Implementation
//
// Configures /dev/ttyACM0 at 115200 baud, 8N1, raw mode.
// TODO: load port path and baud rate from config.yaml
// ============================================================================

#include "data/serial_port.hpp"

#include <fcntl.h>
#include <termios.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iostream>

namespace oro {

SerialPort::SerialPort(const std::string& port, int baud_rate)
    : port_path_(port), baud_rate_(baud_rate) {}

SerialPort::~SerialPort() {
    close();
}

int SerialPort::open() {
    // Open in read-write, non-blocking, no controlling terminal
    fd_ = ::open(port_path_.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (fd_ < 0) {
        std::cerr << "[SerialPort] Failed to open " << port_path_
                  << ": " << std::strerror(errno) << std::endl;
        return -1;
    }

    // Configure termios
    struct termios tty{};
    if (tcgetattr(fd_, &tty) != 0) {
        std::cerr << "[SerialPort] tcgetattr failed: "
                  << std::strerror(errno) << std::endl;
        close();
        return -1;
    }

    // ── Baud rate ───────────────────────────────────────────────────────
    speed_t baud_flag;
    switch (baud_rate_) {
        case 9600:    baud_flag = B9600;    break;
        case 19200:   baud_flag = B19200;   break;
        case 38400:   baud_flag = B38400;   break;
        case 57600:   baud_flag = B57600;   break;
        case 115200:  baud_flag = B115200;  break;
        case 230400:  baud_flag = B230400;  break;
        case 460800:  baud_flag = B460800;  break;
        case 921600:  baud_flag = B921600;  break;
        default:
            std::cerr << "[SerialPort] Unsupported baud rate: "
                      << baud_rate_ << ", defaulting to 115200" << std::endl;
            baud_flag = B115200;
            break;
    }
    cfsetispeed(&tty, baud_flag);
    cfsetospeed(&tty, baud_flag);

    // ── 8N1 configuration ──────────────────────────────────────────────
    tty.c_cflag &= ~PARENB;        // No parity
    tty.c_cflag &= ~CSTOPB;        // 1 stop bit
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;            // 8 data bits
    tty.c_cflag &= ~CRTSCTS;       // No hardware flow control
    tty.c_cflag |= CREAD | CLOCAL; // Enable receiver, ignore modem controls

    // ── Raw mode (no canonical processing) ─────────────────────────────
    tty.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);  // Raw input
    tty.c_iflag &= ~(IXON | IXOFF | IXANY);           // No software flow control
    tty.c_iflag &= ~(IGNBRK | BRKINT | PARMRK | ISTRIP |
                      INLCR | IGNCR | ICRNL);          // No special input handling
    tty.c_oflag &= ~OPOST;                             // Raw output

    // ── Read behavior ──────────────────────────────────────────────────
    // VMIN=0, VTIME=0: non-blocking read, return immediately with
    // whatever bytes are available (0 if none)
    tty.c_cc[VMIN]  = 0;
    tty.c_cc[VTIME] = 0;

    // Apply configuration
    if (tcsetattr(fd_, TCSANOW, &tty) != 0) {
        std::cerr << "[SerialPort] tcsetattr failed: "
                  << std::strerror(errno) << std::endl;
        close();
        return -1;
    }

    // Flush any stale data
    tcflush(fd_, TCIOFLUSH);

    std::cout << "[SerialPort] Opened " << port_path_
              << " at " << baud_rate_ << " baud (8N1, raw mode)" << std::endl;
    return 0;
}

void SerialPort::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
        std::cout << "[SerialPort] Closed " << port_path_ << std::endl;
    }
}

ssize_t SerialPort::read(uint8_t* buf, size_t max_len) {
    if (fd_ < 0) return -1;
    ssize_t n = ::read(fd_, buf, max_len);
    if (n < 0 && errno == EAGAIN) {
        return 0;  // No data available (non-blocking)
    }
    return n;
}

ssize_t SerialPort::write(const uint8_t* buf, size_t len) {
    if (fd_ < 0) return -1;
    return ::write(fd_, buf, len);
}

}  // namespace oro
