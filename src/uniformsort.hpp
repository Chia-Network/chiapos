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

#ifndef SRC_CPP_UNIFORMSORT_HPP_
#define SRC_CPP_UNIFORMSORT_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "./disk.hpp"
#include "./util.hpp"

namespace UniformSort {

    inline int64_t const BUF_SIZE = 262144;

    inline static bool IsPositionEmpty(const uint8_t *memory, uint32_t const entry_len)
    {
        for (uint32_t i = 0; i < entry_len; i++)
            if (memory[i] != 0)
                return false;
        return true;
    }

    inline void SortToMemory(
        FileDisk &input_disk,
        uint64_t const input_disk_begin,
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {
        uint64_t const memory_len = Util::RoundSize(num_entries) * entry_len;
        auto const swap_space = std::make_unique<uint8_t[]>(entry_len);
        auto const buffer = std::make_unique<uint8_t[]>(BUF_SIZE);
        uint64_t bucket_length = 0;
        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
        memset(memory, 0, memory_len);

        uint64_t read_pos = input_disk_begin;
        uint64_t buf_size = 0;
        uint64_t buf_ptr = 0;
        uint64_t swaps = 0;
        for (uint64_t i = 0; i < num_entries; i++) {
            if (buf_size == 0) {
                // If read buffer is empty, read from disk and refill it.
                buf_size = std::min((uint64_t)BUF_SIZE / entry_len, num_entries - i);
                buf_ptr = 0;
                input_disk.Read(read_pos, buffer.get(), buf_size * entry_len);
                read_pos += buf_size * entry_len;
            }
            buf_size--;
            // First unique bits in the entry give the expected position of it in the sorted array.
            // We take 'bucket_length' bits starting with the first unique one.
            uint64_t pos =
                Util::ExtractNum(buffer.get() + buf_ptr, entry_len, bits_begin, bucket_length) *
                entry_len;
            // As long as position is occupied by a previous entry...
            while (!IsPositionEmpty(memory + pos, entry_len) && pos < memory_len) {
                // ...store there the minimum between the two and continue to push the higher one.
                if (Util::MemCmpBits(
                        memory + pos, buffer.get() + buf_ptr, entry_len, bits_begin) > 0) {
                    memcpy(swap_space.get(), memory + pos, entry_len);
                    memcpy(memory + pos, buffer.get() + buf_ptr, entry_len);
                    memcpy(buffer.get() + buf_ptr, swap_space.get(), entry_len);
                    swaps++;
                }
                pos += entry_len;
            }
            // Push the entry in the first free spot.
            memcpy(memory + pos, buffer.get() + buf_ptr, entry_len);
            buf_ptr += entry_len;
        }
        uint64_t entries_written = 0;
        // Search the memory buffer for occupied entries.
        for (uint64_t pos = 0; entries_written < num_entries && pos < memory_len;
             pos += entry_len) {
            if (!IsPositionEmpty(memory + pos, entry_len)) {
                // We've found an entry.
                // write the stored entry itself.
                memcpy(
                    memory + entries_written * entry_len,
                    memory + pos,
                    entry_len);
                entries_written++;
            }
        }

        assert(entries_written == num_entries);
    }

}

#endif  // SRC_CPP_UNIFORMSORT_HPP_
