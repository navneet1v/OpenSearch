# CMakeLists.txt for opensearch-iouring native library

cmake_minimum_required(VERSION 3.16)
project(opensearch_iouring
    VERSION 1.0.0
    LANGUAGES C
    DESCRIPTION "IO-uring wrapper for OpenSearch")

# Options
option(BUILD_TESTS "Build test programs" ON)
option(BUILD_SHARED_LIBS "Build shared library" ON)

# C Standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_C_FLAGS_DEBUG "-g -O0")

# Find liburing using pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing>=2.0)

# Create the shared library
add_library(opensearch_iouring
    src/opensearch_iouring.c
)

# Include directories
target_include_directories(opensearch_iouring
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${LIBURING_INCLUDE_DIRS}
)

# Link libraries
target_link_libraries(opensearch_iouring
    PRIVATE
        ${LIBURING_LIBRARIES}
)

# Compile definitions
target_compile_definitions(opensearch_iouring
    PRIVATE
        _GNU_SOURCE
)

# Set library properties
set_target_properties(opensearch_iouring PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER include/opensearch_iouring.h
)

# Install rules
include(GNUInstallDirs)

install(TARGETS opensearch_iouring
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Generate pkg-config file
# configure_file(
#     ${CMAKE_CURRENT_SOURCE_DIR}/opensearch_iouring.pc.in
#     ${CMAKE_CURRENT_BINARY_DIR}/opensearch_iouring.pc
#     @ONLY
# )

# install(FILES ${CMAKE_CURRENT_BINARY_DIR}/opensearch_iouring.pc
#     DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
# )

# Tests
# if(BUILD_TESTS)
#     enable_testing()

#     add_executable(test_wrapper test/test_wrapper.c)
#     target_link_libraries(test_wrapper opensearch_iouring ${LIBURING_LIBRARIES})
#     target_include_directories(test_wrapper PRIVATE include)

#     add_test(NAME wrapper_test COMMAND test_wrapper)
# endif()

# Print configuration
message(STATUS "")
message(STATUS "OpenSearch IO-uring Configuration:")
message(STATUS "  Version:      ${PROJECT_VERSION}")
message(STATUS "  liburing:     ${LIBURING_VERSION}")
message(STATUS "  Build type:   ${CMAKE_BUILD_TYPE}")
message(STATUS "  Install lib:  ${CMAKE_INSTALL_FULL_LIBDIR}")
message(STATUS "")
