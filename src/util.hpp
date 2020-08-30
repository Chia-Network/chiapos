// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_CPP_UTIL_HPP_
#define SRC_CPP_UTIL_HPP_

#include <random>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <numeric>
#include <cstring>
#include <utility>
#include <cassert>
#include <chrono>
#include <set>
#include <map>
#include <queue>

#define CDIV(a, b) (((a) + (b) - 1) / (b))

#ifdef _WIN32
#include "uint128_t.h"
#else
// __uint__128_t is only available in 64 bit architectures and on certain
// compilers.
typedef __uint128_t uint128_t;

// Allows printing of uint128_t
std::ostream &operator<<(std::ostream & strm, uint128_t const & v) {
    strm << "uint128(" << (uint64_t)(v >> 64) << ","
         << (uint64_t)(v & (((uint128_t)1 << 64) - 1)) << ")";
    return strm;
}
#endif

/* Platform-specific byte swap macros. */
#if defined(_WIN32)
#include <cstdlib>

#define bswap_16(x) _byteswap_ushort(x)
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>

#define bswap_16(x) OSSwapInt16(x)
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)
#else
#include <byteswap.h>
#endif


class Timer {
 public:
    Timer() :
        wall_clock_time_start_(std::chrono::steady_clock::now()),
        cpu_time_start_(clock())
    {
    }

    static char* GetNow()
    {
        auto now = std::chrono::system_clock::now();
        auto tt = std::chrono::system_clock::to_time_t(now);
        return ctime(&tt); // ctime includes newline
    }

    void PrintElapsed(const std::string& name) const {
        auto end = std::chrono::steady_clock::now();
        auto wall_clock_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             end - this->wall_clock_time_start_).count();

        double cpu_time_ms =  1000.0 * (static_cast<double>(clock()) - this->cpu_time_start_) / CLOCKS_PER_SEC;

        double cpu_ratio = static_cast<int>(10000 * (cpu_time_ms / wall_clock_ms)) / 100.0;

        std::cout << name << " " << (wall_clock_ms / 1000.0)  << " seconds. CPU (" << cpu_ratio << "%) " << Timer::GetNow();
    }

 private:
  std::chrono::time_point<std::chrono::steady_clock> wall_clock_time_start_;
  clock_t cpu_time_start_;
};


class Util {
 public:
    template <typename X>
    static inline X Mod(X i, X n) {
        return (i % n + n) % n;
    }

    static uint32_t ByteAlign(uint32_t num_bits) {
        return (num_bits + (8 - ((num_bits) % 8)) % 8);
    }

    static std::string HexStr(const uint8_t* data, size_t len) {
        std::stringstream s;
        s << std::hex;
        for (size_t i=0; i < len; ++i)
            s << std::setw(2) << std::setfill('0') << static_cast<int>(data[i]);
        s << std::dec;
        return s.str();
    }

    static void WriteZeroesHeap(std::ofstream &file, uint32_t num_bytes) {
        uint8_t* buf = new uint8_t[num_bytes];
        memset(buf, 0, num_bytes);
        file.write(reinterpret_cast<char*>(buf), num_bytes);
        delete[] buf;
    }

    static void WriteZeroesStack(std::ofstream &file, uint32_t num_bytes) {
#ifdef _WIN32
        uint8_t *buf = new uint8_t[num_bytes];
        memset(buf, 0, num_bytes);
        file.write(reinterpret_cast<char*>(buf), num_bytes);
        delete[] buf;
#else
        uint8_t buf[num_bytes];
        memset(buf, 0, num_bytes);
        file.write(reinterpret_cast<char*>(buf), num_bytes);
#endif
    }

    static void IntToTwoBytes(uint8_t* result, const uint16_t input) {
        uint16_t r = bswap_16(input);
        memcpy(result, &r, sizeof(r));
    }

    // Used to encode deltas object size
    static void IntToTwoBytesLE(uint8_t* result, const uint16_t input) {
        result[0] = input & 0xff;
        result[1] = input >> 8;
    }

    static uint16_t TwoBytesToInt(const uint8_t *bytes) {
        uint16_t i;
        memcpy(&i, bytes, sizeof(i));
        return bswap_16(i);
    }

    /*
     * Converts a 32 bit int to bytes.
     */
    static void IntToFourBytes(uint8_t* result, const uint32_t input) {
        uint32_t r = bswap_32(input);
        memcpy(result, &r, sizeof(r));
    }

    /*
     * Converts a byte array to a 32 bit int.
     */
    static uint32_t FourBytesToInt(const uint8_t* bytes) {
        uint32_t i;
        memcpy(&i, bytes, sizeof(i));
        return bswap_32(i);
    }

    /*
     * Converts a 64 bit int to bytes.
     */
    static void IntToEightBytes(uint8_t* result, const uint64_t input) {
        uint64_t r = bswap_64(input);
        memcpy(result, &r, sizeof(r));
    }

    /*
     * Converts a byte array to a 64 bit int.
     */
    static uint64_t EightBytesToInt(const uint8_t* bytes) {
        uint64_t i;
        memcpy(&i, bytes, sizeof(i));
        return bswap_64(i);
    }

    /*
     * Retrieves the size of an integer, in Bits.
     */
    static uint8_t GetSizeBits(uint128_t value) {
        uint8_t count = 0;
        while (value) {
            count++;
            value >>= 1;
        }
        return count;
    }

    /* Note: requires start_bit % 8 + num_bits <= 64 */
    inline static uint64_t SliceInt64FromBytes(const uint8_t* bytes,
                                               uint32_t start_bit, const uint32_t num_bits) {
        uint64_t tmp;

        if (start_bit + num_bits > 64) {
            bytes += start_bit / 8;
            start_bit %= 8;
        }

        tmp = Util::EightBytesToInt(bytes);
        tmp <<= start_bit;
        tmp >>= 64 - num_bits;
        return tmp;
    }

    static uint64_t SliceInt64FromBytesFull(const uint8_t *bytes, uint32_t start_bit, uint32_t num_bits) {
        uint32_t last_bit = start_bit + num_bits;
        uint64_t r = SliceInt64FromBytes(bytes, start_bit, num_bits);
        if (start_bit % 8 + num_bits > 64)
            r |= bytes[last_bit / 8] >> (8 - last_bit % 8);
        return r;
    }


    inline static uint128_t SliceInt128FromBytes(const uint8_t* bytes,
                                                 const uint32_t start_bit, const uint32_t num_bits) {
        if (num_bits <= 64)
            return SliceInt64FromBytesFull(bytes, start_bit, num_bits);

        uint32_t num_bits_high = num_bits - 64;
        uint64_t high = SliceInt64FromBytesFull(bytes, start_bit, num_bits_high);
        uint64_t low = SliceInt64FromBytesFull(bytes, start_bit + num_bits_high, 64);
        return ((uint128_t)high << 64) | low;
    }

    static void GetRandomBytes(uint8_t* buf, uint32_t num_bytes) {
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dist(0, 255);
        for (uint32_t i = 0; i < num_bytes; i++) {
            buf[i] = dist(mt);
        }
    }

    static uint64_t find_islands(std::vector<std::pair<uint64_t, uint64_t> > edges) {
        std::map<uint64_t, std::vector<uint64_t> > edge_indeces;
        for (uint64_t edge_index = 0; edge_index < edges.size(); edge_index++) {
            edge_indeces[edges[edge_index].first].push_back(edge_index);
            edge_indeces[edges[edge_index].second].push_back(edge_index);
        }
        std::set<uint64_t> visited_nodes;
        std::queue<uint64_t> nodes_to_visit;
        uint64_t num_islands = 0;
        for (auto new_edge : edges) {
            uint64_t old_size = visited_nodes.size();
            if (visited_nodes.find(new_edge.first) == visited_nodes.end()) {
                visited_nodes.insert(new_edge.first);
                nodes_to_visit.push(new_edge.first);
            }
            if (visited_nodes.find(new_edge.second) == visited_nodes.end()) {
                visited_nodes.insert(new_edge.second);
                nodes_to_visit.push(new_edge.second);
            }
            while (!nodes_to_visit.empty()) {
                uint64_t node = nodes_to_visit.front();
                nodes_to_visit.pop();
                for (uint64_t edge_index : edge_indeces[node]) {
                    std::pair<uint64_t, uint64_t> edge = edges[edge_index];
                    if (visited_nodes.find(edge.first) == visited_nodes.end()) {
                        visited_nodes.insert(edge.first);
                        nodes_to_visit.push(edge.first);
                    }
                    if (visited_nodes.find(edge.second) == visited_nodes.end()) {
                        visited_nodes.insert(edge.second);
                        nodes_to_visit.push(edge.second);
                    }
                }
            }
            if (visited_nodes.size() > old_size) {
                num_islands++;
            }
        }
        return num_islands;
    }
};

#endif  // SRC_CPP_UTIL_HPP_
