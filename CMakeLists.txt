cmake_minimum_required(VERSION 3.16)

project(QtNatsProject VERSION 0.1.0)

include(CMakePrintHelpers)
include(GenerateExportHeader)
include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_AUTOMOC ON)

find_package(cnats CONFIG REQUIRED)
find_package(Qt6 REQUIRED COMPONENTS Core)

# ------------- qtnats library ----------------------
add_library(qtnats
    STATIC
        src/qtnats/jetstream.cpp
        src/qtnats/qtnats.cpp
        src/qtnats/qtnats.h
        src/qtnats/qtnats_p.h
        ${CMAKE_CURRENT_BINARY_DIR}/qtnats/qtnats_export.h
)

target_include_directories(qtnats
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(qtnats PUBLIC cnats::nats_static Qt6::Core)

set_target_properties(qtnats PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 0.1
    EXPORT_NAME qtnats
)

GENERATE_EXPORT_HEADER(qtnats
    EXPORT_FILE_NAME qtnats/qtnats_export.h
)

# ------------- install rules ----------------------
install(TARGETS qtnats
    EXPORT qtnats-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(FILES
    src/qtnats/qtnats.h
    src/qtnats/qtnats_p.h
    ${CMAKE_CURRENT_BINARY_DIR}/qtnats/qtnats_export.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/qtnats
)

install(EXPORT qtnats-targets
    NAMESPACE qtnats::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/qtnats
)

include(CMakePackageConfigHelpers)

configure_package_config_file(
    cmake/qtnats-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/qtnats-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/qtnats
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/qtnats-config-version.cmake
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/qtnats-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/qtnats-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/qtnats
)

# integration tests (these spin up nats server and nats client)
set(BUILD_QT_NATS_INTEGRATION_TESTING OFF CACHE BOOL "Build qtnats integration tests")
if (BUILD_QT_NATS_INTEGRATION_TESTING)
    add_executable(test_core test/test_core.cpp)
    add_test(NAME test_core COMMAND test_core)
    target_link_libraries(test_core PRIVATE qtnats Qt::Test)

    add_executable(test_jetstream test/test_jetstream.cpp)
    add_test(NAME test_jetstream COMMAND test_jetstream)
    target_link_libraries(test_jetstream PRIVATE qtnats Qt::Test)
endif ()
