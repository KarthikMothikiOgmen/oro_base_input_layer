cmake_minimum_required(VERSION 3.15)
project(oro_input_layer_node VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -Wpedantic -Werror=return-type)

find_package(Threads REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ZMQ REQUIRED libzmq)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../oro_base_hardware_layer/radxa_drivers/include
    ${ZMQ_INCLUDE_DIRS}
)

add_subdirectory(../oro_base_hardware_layer/radxa_drivers radxa_drivers)

add_executable(input_layer_node
    src/main.cpp
    src/command/command_ingress_node.cpp
    src/command/cloud_receiver_thread.cpp
    src/data/mcu_serial_reader_node.cpp
    src/data/publish_filter.cpp
    src/data/serial_port.cpp
)

target_link_libraries(input_layer_node
    PRIVATE
    Threads::Threads
    ${ZMQ_LIBRARIES}
    radxa_drivers
)
target_link_directories(input_layer_node PRIVATE ${ZMQ_LIBRARY_DIRS})

install(TARGETS input_layer_node
    RUNTIME DESTINATION bin
)
