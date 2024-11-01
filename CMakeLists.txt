CMAKE_MINIMUM_REQUIRED(VERSION 3.14 FATAL_ERROR)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

IF(NOT CMAKE_BUILD_TYPE)
  SET(CMAKE_BUILD_TYPE "RELEASE")
ENDIF()

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

project(chiapos C CXX ASM)

# CMake 3.14+
include(FetchContent)

if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
include(${CMAKE_INSTALL_PREFIX}/share/cmake/pybind11/pybind11Config.cmake)
else()
FetchContent_Declare(
  pybind11-src
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG        v2.11.1
)
FetchContent_MakeAvailable(pybind11-src)
endif()

FetchContent_Declare(
  cxxopts
  GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git
  GIT_TAG        v3.1.1
)
FetchContent_MakeAvailable(cxxopts)

option(CP_LINK_BLADEBIT_HARVESTER "Links libbladebit_harvester at build time instead of dynamically loading it." OFF)
option(CP_BUILD_BLADEBIT_HARVESTER "Pulls bladebit harvester target from git and builds it as a dependency.")

if (${CP_BUILD_BLADEBIT_HARVESTER} AND NOT ${CP_LINK_BLADEBIT_HARVESTER})
  set(CP_LINK_BLADEBIT_HARVESTER ON)
endif()

if (${CP_BUILD_BLADEBIT_HARVESTER})
  FetchContent_Declare(
    bladebit
    GIT_REPOSITORY https://github.com/Chia-Network/bladebit.git
    GIT_TAG        cuda-compression
  )

  set(BB_HARVESTER_ONLY ON)
  set(BB_HARVESTER_STATIC ON)
  FetchContent_MakeAvailable(bladebit)
endif()

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
  ${cxxopts_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/../lib/FiniteStateEntropy/lib
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/test
)

IF (${CP_LINK_BLADEBIT_HARVESTER})
  message ("Bladebit Harvesting Enabled")
ENDIF ()

add_library(fse ${FSE_FILES})

IF (MSVC)
IF (CMAKE_BUILD_TYPE STREQUAL "RELEASE")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /Oy")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /O2 /Oy")
ELSE()
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Ob1")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /Ob1")
ENDIF()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /Zi")

ELSE()
IF (CMAKE_BUILD_TYPE STREQUAL "RELEASE")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
ELSE()
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
ENDIF()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -g")

ENDIF()

IF (CMAKE_BUILD_TYPE STREQUAL "ASAN")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1 -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O1 -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
ENDIF()

IF (CMAKE_BUILD_TYPE STREQUAL "TSAN")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -fno-omit-frame-pointer -fsanitize=thread")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2 -fno-omit-frame-pointer -fsanitize=thread")
set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=thread")
ENDIF()

pybind11_add_module(chiapos ${CMAKE_CURRENT_SOURCE_DIR}/python-bindings/chiapos.cpp src/chacha8.c)
add_executable(ProofOfSpace
    src/cli.cpp
    src/chacha8.c
)

FetchContent_Declare(
  blake3
  GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3.git
  GIT_TAG        1.5.0
)

FetchContent_GetProperties(blake3)
if(NOT blake3_POPULATED)
  FetchContent_Populate(blake3)

  # Set BLAKE3 to build as a static library
  set(BUILD_SHARED_LIBS FALSE CACHE BOOL "Build static libraries" FORCE)

  add_subdirectory(${blake3_SOURCE_DIR}/c ${blake3_BINARY_DIR})
endif()

set(BLAKE3_SRC ${blake3_SOURCE_DIR}/c)
set(BLAKE3_INCLUDE_DIR ${blake3_SOURCE_DIR}/c)
set_target_properties(blake3 PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib$<0:>")
target_link_libraries(chiapos PRIVATE blake3)
target_link_libraries(ProofOfSpace PRIVATE blake3)
include_directories(
  ${INCLUDE_DIRECTORIES}
  ${BLAKE3_INCLUDE_DIR}
)

option(BUILD_PROOF_OF_SPACE_STATICALLY "Build ProofOfSpace target statically" OFF)
IF (BUILD_PROOF_OF_SPACE_STATICALLY)
  message("Statically build ProofOfSpace")
  target_link_libraries(ProofOfSpace PUBLIC -static -Wl,--whole-archive -lrt -lpthread -Wl,--no-whole-archive)
ENDIF()

option(BUILD_STATIC_CHIAPOS_LIBRARY "Build chiapos static library (verify-only)" OFF)
IF (BUILD_STATIC_CHIAPOS_LIBRARY)
  message("Build chiapos static library (verify-only)")
  add_library(chiapos_static STATIC src/chacha8.c c-bindings/wrapper.cpp)
  target_link_libraries(chiapos_static PRIVATE blake3 $<$<CXX_COMPILER_ID:MSVC>:uint128>)
  target_include_directories(chiapos_static PUBLIC lib/include uint128_t)
  set_target_properties(chiapos_static PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib$<0:>")
ENDIF()

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.5.2
)
FetchContent_MakeAvailable(Catch2)

add_executable(RunTests
    tests/test.cpp
    src/chacha8.c
)

target_link_libraries(RunTests
    PRIVATE
    fse
    Threads::Threads
    Catch2::Catch2
    blake3
)

if(WITH_COVERAGE)
target_compile_options(RunTests PRIVATE --coverage)
target_link_options(RunTests PRIVATE --coverage)
endif()

find_package(Threads REQUIRED)

add_library(uint128 STATIC uint128_t/uint128_t.cpp)
target_include_directories(uint128 PUBLIC uint128_t)
set_target_properties(uint128 PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib$<0:>")

target_compile_features(fse PUBLIC cxx_std_17)
target_compile_features(chiapos PUBLIC cxx_std_17)
if (WIN32)
  # workaround for constexpr mutex constructor change in MSVC 2022
  # https://stackoverflow.com/questions/78598141/first-stdmutexlock-crashes-in-application-built-with-latest-visual-studio
  target_compile_definitions(chiapos PUBLIC _DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
endif()
# target_compile_features(RunTests PUBLIC cxx_std_17)

target_link_libraries(chiapos PRIVATE fse Threads::Threads
  $<$<CXX_COMPILER_ID:MSVC>:uint128>
  $<$<NOT:$<PLATFORM_ID:Darwin,OpenBSD,FreeBSD,Windows>>:stdc++fs>
)
target_link_libraries(ProofOfSpace PRIVATE fse Threads::Threads
  $<$<CXX_COMPILER_ID:MSVC>:uint128>
  $<$<NOT:$<PLATFORM_ID:Darwin,OpenBSD,FreeBSD,Windows>>:stdc++fs>
)
target_link_libraries(RunTests PRIVATE fse Threads::Threads Catch2::Catch2WithMain
  $<$<CXX_COMPILER_ID:MSVC>:uint128>
  $<$<NOT:$<PLATFORM_ID:Darwin,OpenBSD,FreeBSD,Windows>>:stdc++fs>
)


if (${CP_LINK_BLADEBIT_HARVESTER})

  set(bb_defs
    USE_GREEN_REAPER=1
    BLADEBIT_HARVESTER_LINKED=1
    $<$<BOOL:${CP_BUILD_BLADEBIT_HARVESTER}>:BLADEBIT_IS_PROJECT_DEPENDENCY=1>
  )
  set(bb_libs
    bladebit_harvester
    $<$<NOT:$<PLATFORM_ID:Windows>>:dl>
  )

  include_directories(
    ${INCLUDE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/libs/green_reaper/include
  )

  link_directories(
    ${LINK_DIRECTORIES}
    ${CMAKE_SOURCE_DIR}/libs/green_reaper/lib
  )

  target_compile_definitions(chiapos PUBLIC ${bb_defs})
  target_compile_definitions(ProofOfSpace PUBLIC ${bb_defs})
  target_compile_definitions(RunTests PUBLIC ${bb_defs})

  target_link_libraries(chiapos PUBLIC ${bb_libs})
  target_link_libraries(ProofOfSpace PUBLIC ${bb_libs})
  target_link_libraries(RunTests PUBLIC ${bb_libs})

  target_link_directories(chiapos PUBLIC ${CMAKE_SOURCE_DIR}/libs/green_reaper/lib)
  target_link_directories(ProofOfSpace PUBLIC ${CMAKE_SOURCE_DIR}/libs/green_reaper/lib)
  target_link_directories(RunTests PUBLIC ${CMAKE_SOURCE_DIR}/libs/green_reaper/lib)

  set_property(TARGET chiapos APPEND PROPERTY BUILD_RPATH "$ORIGIN")
  set_property(TARGET ProofOfSpace APPEND PROPERTY BUILD_RPATH "$ORIGIN")
  set_property(TARGET RunTests APPEND PROPERTY BUILD_RPATH "$ORIGIN")

  if (WIN32)
    add_custom_command(TARGET chiapos POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_SOURCE_DIR}/libs/green_reaper/lib/bladebit_harvester.dll"
        "$<TARGET_FILE_DIR:chiapos>/bladebit_harvester.dll"
    )
    message("The bladebit dll was copied to: $<TARGET_FILE_DIR:chiapos>/bladebit_harvester.dll")
  endif()
endif()


enable_testing()
add_test(NAME RunTests COMMAND RunTests)
