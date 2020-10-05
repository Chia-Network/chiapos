CMAKE_MINIMUM_REQUIRED(VERSION 3.14 FATAL_ERROR)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

IF(NOT CMAKE_BUILD_TYPE)
  SET(CMAKE_BUILD_TYPE "RELEASE")
ENDIF()

project(chiapos C CXX ASM)

set(FSE_LIB ${CMAKE_CURRENT_SOURCE_DIR}/lib/FiniteStateEntropy/lib)
set(FSE_FILES
    ${FSE_LIB}/fse_compress.c
    ${FSE_LIB}/fse_decompress.c
    ${FSE_LIB}/entropy_common.c
    ${FSE_LIB}/hist.c
)

include_directories(
  ${INCLUDE_DIRECTORIES}
  ${CMAKE_CURRENT_SOURCE_DIR}/../lib/include
  ${CMAKE_CURRENT_SOURCE_DIR}/../lib/FiniteStateEntropy/lib
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/test
  )

add_library(uint128 _deps/uint128_t/uint128_t.cpp)
target_include_directories(uint128 PUBLIC _deps/uint128_t)

add_library(fse ${FSE_FILES})

# CMake 3.14+
include(FetchContent)

FetchContent_Declare(
  pybind11-src
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG        v2.5.0
)
FetchContent_MakeAvailable(pybind11-src)

set (CMAKE_CXX_FLAGS "-g -O3 -Wall")
set (CMAKE_C_FLAGS "-g -O3 -Wall")

set(BLAKE3_SRC
    src/b3/blake3.c
    src/b3/blake3_portable.c
    src/b3/blake3_dispatch.c
    src/b3/blake3_avx2_x86-64_unix.S
    src/b3/blake3_avx512_x86-64_unix.S
    src/b3/blake3_sse41_x86-64_unix.S
)

pybind11_add_module(chiapos ${CMAKE_CURRENT_SOURCE_DIR}/python-bindings/chiapos.cpp src/chacha8.c ${BLAKE3_SRC})

add_executable(ProofOfSpace
    src/cli.cpp
    src/chacha8.c
    ${BLAKE3_SRC}
)

add_executable(RunTests
    tests/test-main.cpp
    tests/test.cpp
    src/chacha8.c
    ${BLAKE3_SRC}
)

target_compile_features(fse PUBLIC cxx_std_17)
target_compile_features(chiapos PUBLIC cxx_std_17)
target_compile_features(RunTests PUBLIC cxx_std_17)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries(chiapos PRIVATE fse uint128)
  target_link_libraries(ProofOfSpace fse uint128)
  target_link_libraries(RunTests fse uint128)
elseif (${CMAKE_SYSTEM_NAME} MATCHES "OpenBSD")
  target_link_libraries(chiapos PRIVATE fse uint128)
  target_link_libraries(ProofOfSpace fse uint128)
  target_link_libraries(RunTests fse uint128)
elseif (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  target_link_libraries(chiapos PRIVATE fse uint128)
  target_link_libraries(ProofOfSpace fse uint128)
  target_link_libraries(RunTests fse uint128)
elseif (WIN32)
  target_link_libraries(chiapos PRIVATE fse uint128)
  target_link_libraries(ProofOfSpace fse uint128)
  target_link_libraries(RunTests fse uint128)
else()
  target_link_libraries(chiapos PRIVATE rt pthread fse uint128 stdc++fs)
  target_link_libraries(ProofOfSpace rt pthread fse uint128 stdc++fs)
  target_link_libraries(RunTests rt pthread fse uint128 stdc++fs)
endif()

enable_testing()
add_test(NAME RunTests COMMAND RunTests)
