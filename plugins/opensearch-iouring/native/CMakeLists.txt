cmake_minimum_required(VERSION 3.21)

project(opensearch_iouring
    VERSION 1.0.0
    LANGUAGES C
    DESCRIPTION "IO-uring wrapper for OpenSearch"
)

# -----------------------------
# C standard
# -----------------------------
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# -----------------------------
# Dependencies
# -----------------------------
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing>=2.0)

# -----------------------------
# Shared library
# -----------------------------
add_library(opensearch_iouring SHARED
    src/opensearch_iouring.c
)

set_target_properties(opensearch_iouring PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME "opensearch_iouring"
)

# -----------------------------
# Include paths
# -----------------------------
target_include_directories(opensearch_iouring PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
)

# -----------------------------
# Compiler flags
# -----------------------------
target_compile_options(opensearch_iouring PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    ${LIBURING_CFLAGS_OTHER}
)

target_compile_definitions(opensearch_iouring PRIVATE
    _GNU_SOURCE
)

# -----------------------------
# Link libraries
# -----------------------------
target_link_libraries(opensearch_iouring PRIVATE
    ${LIBURING_LIBRARIES}
    ${LIBURING_LDFLAGS_OTHER}
)


include(GNUInstallDirs)

install(
    TARGETS opensearch_iouring
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(
    FILES include/opensearch_iouring.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# -----------------------------
# Status output
# -----------------------------
message(STATUS "")
message(STATUS "OpenSearch IO-uring Configuration:")
message(STATUS "  Version:      ${PROJECT_VERSION}")
message(STATUS "  liburing:     ${LIBURING_VERSION}")
message(STATUS "  Build type:   ${CMAKE_BUILD_TYPE}")
message(STATUS "")
