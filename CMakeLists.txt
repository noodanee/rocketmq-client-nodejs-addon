cmake_minimum_required(VERSION 3.26)

project(rocketmq)

add_definitions(-DNAPI_VERSION=9)
add_definitions(-DNAPI_CPP_EXCEPTIONS)
add_definitions(-DNODE_RUNTIME=node)
add_definitions(-DBUILDING_NODE_EXTENSION)

option(ROCKETMQ_FORCE_STATIC_MUSL "Link against a provided static musl libc archive" OFF)
option(ROCKETMQ_ENABLE_COVERAGE "Enable coverage flags" OFF)
option(ROCKETMQ_USE_STUB "Use RocketMQ stub library for tests" OFF)

set(NPX_CMD npx)

execute_process(
        COMMAND ${NPX_CMD} cmake-js print-cmakejs-include
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE CMAKE_JS_INC
)

# cmake-js 8.x prints INFO log lines to stdout — strip everything except the
# final include-path line (the only line not beginning with INFO/WARN/ERROR).
string(REGEX REPLACE "(^|\n)(INFO|WARN|ERROR)[^\n]*" "" CMAKE_JS_INC "${CMAKE_JS_INC}")
string(REPLACE "\n" "" CMAKE_JS_INC "${CMAKE_JS_INC}")
string(REPLACE "\r" "" CMAKE_JS_INC "${CMAKE_JS_INC}")
string(STRIP "${CMAKE_JS_INC}" CMAKE_JS_INC)

message(STATUS "CMake.js configurations: INC=${CMAKE_JS_INC}")

include_directories(${CMAKE_JS_INC})

if(ROCKETMQ_USE_STUB)
    add_definitions(-DROCKETMQ_USE_STUB=1)
    set(ROCKETMQ_STUB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/test/mocks/rocketmq")
    file(GLOB ROCKETMQ_STUB_SOURCES "${ROCKETMQ_STUB_ROOT}/src/*.cpp")
    add_library(rocketmq_stub STATIC ${ROCKETMQ_STUB_SOURCES})
    set_target_properties(rocketmq_stub PROPERTIES
            POSITION_INDEPENDENT_CODE ON
            ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build")
    target_include_directories(rocketmq_stub PUBLIC "${ROCKETMQ_STUB_ROOT}/include")
    include_directories("${ROCKETMQ_STUB_ROOT}/include")
    set(ROCKETMQ_LIB rocketmq_stub)
else()
    set(ROCKETMQ_SDK_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/deps/rocketmq")

    file(GLOB_RECURSE ROCKETMQ_SDK_SOURCES
            "${ROCKETMQ_SDK_ROOT}/src/*.cpp"
            "${ROCKETMQ_SDK_ROOT}/src/*.c")
    list(FILTER ROCKETMQ_SDK_SOURCES EXCLUDE REGEX "dllmain\\.cpp$")

    file(GLOB ROCKETMQ_SIG_SOURCES "${ROCKETMQ_SDK_ROOT}/libs/signature/src/*.c")

    add_library(rocketmq_sdk STATIC ${ROCKETMQ_SDK_SOURCES} ${ROCKETMQ_SIG_SOURCES})
    set_target_properties(rocketmq_sdk PROPERTIES
            POSITION_INDEPENDENT_CODE ON
            ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build")

    # Newer Clang/GCC toolchains promote warnings the vendored SDK can't avoid
    # to errors; downgrade those to warnings so cross-compilation succeeds.
    # The std::function<...noexcept> typedefs in TcpTransport.h were rewritten
    # to drop the noexcept qualifier (C++20-only) — see the header for context.
    target_compile_options(rocketmq_sdk PRIVATE
            -Wno-error
            -Wno-error=date-time
            $<$<COMPILE_LANGUAGE:CXX>:-Wno-error=deprecated-literal-operator>
            -Wno-error=deprecated-declarations
            -Wno-error=unused-but-set-variable)

    file(GLOB ROCKETMQ_SRC_CHILDREN "${ROCKETMQ_SDK_ROOT}/src/*")
    set(ROCKETMQ_SRC_INCLUDE_DIRS "${ROCKETMQ_SDK_ROOT}/src")
    foreach(child ${ROCKETMQ_SRC_CHILDREN})
        if(IS_DIRECTORY ${child})
            list(APPEND ROCKETMQ_SRC_INCLUDE_DIRS ${child})
        endif()
    endforeach()

    target_include_directories(rocketmq_sdk PRIVATE
            "${ROCKETMQ_SDK_ROOT}/include"
            ${ROCKETMQ_SRC_INCLUDE_DIRS}
            "${ROCKETMQ_SDK_ROOT}/bin/include"
            "${ROCKETMQ_SDK_ROOT}/libs/signature/include")

    include_directories("${ROCKETMQ_SDK_ROOT}/include")
    set(ROCKETMQ_LIB rocketmq_sdk
            "${ROCKETMQ_SDK_ROOT}/bin/lib/libevent.a"
            "${ROCKETMQ_SDK_ROOT}/bin/lib/libevent_pthreads.a"
            "${ROCKETMQ_SDK_ROOT}/bin/lib/libjsoncpp.a"
            "${ROCKETMQ_SDK_ROOT}/bin/lib/libz.a")
endif()

file(GLOB SOURCE_FILES "lib/*.cpp")
add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
set_target_properties(${PROJECT_NAME}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build")
target_link_libraries(${PROJECT_NAME} ${ROCKETMQ_LIB})

# Keep CMake dependency tracking enabled to avoid missing compiler_depend.ts with newer CMake
# versions when using Makefile generators.

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++17")
    if(ROCKETMQ_FORCE_STATIC_MUSL)
        set(MUSL_LIBC_PATH $ENV{MUSL_LIBC_PATH})
        if(NOT MUSL_LIBC_PATH)
            message(FATAL_ERROR "ROCKETMQ_FORCE_STATIC_MUSL=ON but MUSL_LIBC_PATH is not set")
        endif()
        if(NOT EXISTS "${MUSL_LIBC_PATH}")
            message(FATAL_ERROR "musl libc archive not found at ${MUSL_LIBC_PATH}")
        endif()
        target_link_libraries(${PROJECT_NAME} ${MUSL_LIBC_PATH})
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++ -static")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fPIC")
        set_target_properties(${PROJECT_NAME} PROPERTIES LINK_DEPENDS_NO_SHARED TRUE)
    endif()
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++17 -stdlib=libc++")
endif()

if(ROCKETMQ_ENABLE_COVERAGE)
    add_definitions(-DROCKETMQ_COVERAGE=1)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fprofile-arcs -ftest-coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
endif()
