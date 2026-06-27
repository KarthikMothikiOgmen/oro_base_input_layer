#include "command/command_ingress_node.hpp"
#include "data/mcu_serial_reader_node.hpp"
#include "radxa_drivers/radxa_drivers_node.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>
#include <zmq.hpp>

std::atomic<bool> g_running{true};

void signal_handler(int) {
  std::cout << "\n[InputLayer] Terminating...\n";
  g_running = false;
}

int main() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  std::cout << "Starting oro_base_input_layer...\n";

  // 1. Initialize global ZMQ Context
  // Using a single IO thread is usually sufficient, but can be scaled up
  zmq::context_t context(1);

  // Endpoints for this Phase are placeholders (e.g., localhost ports or IPC)
  const std::string CLOUD_ENDPOINT = "tcp://127.0.0.1:5555";
  const std::string SENSOR_PUB_ENDPOINT = "ipc:///tmp/oro_sensors.ipc";
  const std::string SYSTEM_PUB_ENDPOINT = "ipc:///tmp/oro_system.ipc";
  const std::string STATUS_PUB_ENDPOINT = "ipc:///tmp/oro_status.ipc";
  const std::string MCU_CMD_ENDPOINT = "ipc:///tmp/oro_mcu_cmd.ipc";

  // 2. Initialize Nodes
  CommandIngressNode command_node(context, CLOUD_ENDPOINT, MCU_CMD_ENDPOINT, STATUS_PUB_ENDPOINT);
  oro::McuSerialReaderNode mcu_node(context, SENSOR_PUB_ENDPOINT,
                                    SYSTEM_PUB_ENDPOINT, STATUS_PUB_ENDPOINT,
                                    MCU_CMD_ENDPOINT);
  oro::RadxaDriversNode radxa_node(mcu_node.get_sensor_publisher());

  // 3. Start Background Threads
  command_node.start();
  mcu_node.start();
  radxa_node.start();

  // 4. Main Event Loop
  std::cout << "[InputLayer] Entering main polling loop...\n";
  while (g_running) {
    // Spin non-blocking over internal nodes
    command_node.spin_once();
    mcu_node.spin_once();
    radxa_node.spin_once();

    // Brief sleep to yield CPU. In high-performance, we would use zmq::poll
    // tightly here.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // 5. Teardown
  command_node.stop();
  mcu_node.stop();
  radxa_node.stop();
  // ZMQ context will gracefully shut itself down via RAII when it goes out of
  // scope.

  std::cout << "oro_base_input_layer shutdown complete.\n";
  return 0;
}

