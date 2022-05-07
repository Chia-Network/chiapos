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

    inline void SortToMemory(
        FileDisk &input_disk,
        uint64_t const input_disk_begin,
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {
        uint64_t const rounded_entries = Util::RoundSize(num_entries);
        auto const swap_space = std::make_unique<uint8_t[]>(entry_len);
        auto const buffer = std::make_unique<uint8_t[]>(BUF_SIZE);
        uint64_t bucket_length = 0;
        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
        bitfield is_used(rounded_entries);

        uint64_t read_pos = input_disk_begin;
        uint64_t buf_size = 0;
        uint64_t buf_ofs = 0;
        for (uint64_t i = 0; i < num_entries; i++) {
            if (buf_size == 0) {
                // If read buffer is empty, read from disk and refill it.
                buf_size = std::min((uint64_t)BUF_SIZE / entry_len, num_entries - i);
                buf_ofs = 0;
                input_disk.Read(read_pos, buffer.get(), buf_size * entry_len);
                read_pos += buf_size * entry_len;
            }
            buf_size--;
            // First unique bits in the entry give the expected position of it in the sorted array.
            // We take 'bucket_length' bits starting with the first unique one.
            uint64_t idx =
                Util::ExtractNum(buffer.get() + buf_ofs, entry_len, bits_begin, bucket_length);
            uint64_t mem_ofs = idx * entry_len;
            // As long as position is occupied by a previous entry...
            while (is_used.get(idx) && idx < rounded_entries) {
                // ...store there the minimum between the two and continue to push the higher one.
                if (Util::MemCmpBits(
                        memory + mem_ofs, buffer.get() + buf_ofs, entry_len, bits_begin) > 0) {
                    memcpy(swap_space.get(), memory + mem_ofs, entry_len);
                    memcpy(memory + mem_ofs, buffer.get() + buf_ofs, entry_len);
                    memcpy(buffer.get() + buf_ofs, swap_space.get(), entry_len);
                }
                idx++;
                mem_ofs += entry_len;
            }
            // Push the entry in the first free spot.
            memcpy(memory + mem_ofs, buffer.get() + buf_ofs, entry_len);
            is_used.set(idx);
            buf_ofs += entry_len;
        }
        uint64_t entries_written = 0;
        uint64_t entries_ofs = 0;
        // Search the memory buffer for occupied entries.
        for (uint64_t idx = 0, mem_ofs = 0; entries_written < num_entries && idx < rounded_entries;
             idx++, mem_ofs += entry_len) {
            if (is_used.get(idx)) {
                // We've found an entry.
                // write the stored entry itself.
                memcpy(
                    memory + entries_ofs,
                    memory + mem_ofs,
                    entry_len);
                entries_written++;
                entries_ofs += entry_len;
            }
        }

        assert(entries_written == num_entries);
    }

}

#endif  // SRC_CPP_UNIFORMSORT_HPP_
