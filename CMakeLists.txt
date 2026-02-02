cmake_minimum_required(VERSION 3.16)

project(QtNatsProject)

include(CMakePrintHelpers)
include(FetchContent)
include(GenerateExportHeader)

set(default_build_type "Release")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_AUTOMOC ON)

find_package(QT NAMES Qt6 Qt5 COMPONENTS Core REQUIRED)
# according to https://gitlab.kitware.com/cmake/cmake/-/issues/20843 I need 2 find_package's to detect Qt5/Qt6
find_package(Qt${QT_VERSION_MAJOR} COMPONENTS Core Test Qml Quick REQUIRED)
message("Using Qt version ${QT_VERSION}")

# show that we're git-cloning cnats
set(FETCHCONTENT_QUIET FALSE)
FetchContent_Declare(
  cnats
  GIT_REPOSITORY https://github.com/nats-io/nats.c.git
  GIT_TAG        v3.3.0
  GIT_SHALLOW    ON
)

set(NATS_BUILD_WITH_TLS OFF)
set(NATS_BUILD_EXAMPLES OFF)
set(NATS_BUILD_STREAMING OFF)
set(NATS_BUILD_LIB_SHARED OFF)

set(BUILD_TESTING OFF)  # do not build cnats tests
set(BUILD_QT_NATS_INTEGRATION_TESTING OFF)

FetchContent_MakeAvailable(cnats)

# ------------- qtnats library ----------------------
add_library(qtnats
    STATIC
        src/qtnats/jetstream.cpp
        src/qtnats/qtnats.cpp
        src/qtnats/qtnats.h
        src/qtnats/qtnats_p.h
        ${CMAKE_CURRENT_BINARY_DIR}/qtnats_export.h
)

target_include_directories(qtnats
    PUBLIC
        src
        ${CMAKE_CURRENT_BINARY_DIR}
        ${cnats_SOURCE_DIR}/src
)


target_link_libraries(qtnats nats_static Qt::Core)

# this has no effect on Windows due to https://gitlab.kitware.com/cmake/cmake/-/issues/19618
set_target_properties(qtnats PROPERTIES VERSION 0.1.0 SOVERSION 0.1)

GENERATE_EXPORT_HEADER(qtnats)

# integration tests (these spin up nats server and nats client)
if (BUILD_QT_NATS_INTEGRATION_TESTING)
    add_executable(test_core test/test_core.cpp)
    add_test(NAME test_core COMMAND test_core)
    target_link_libraries(test_core PRIVATE qtnats Qt::Test)

    add_executable(test_jetstream test/test_jetstream.cpp)
    add_test(NAME test_jetstream COMMAND test_jetstream)
    target_link_libraries(test_jetstream PRIVATE qtnats Qt::Test)
endif ()

if(COMMAND dc_register_thirdparty_library)
    dc_register_thirdparty_library(qtnats)
endif()
