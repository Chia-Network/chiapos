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
#include "./threading.hpp"

#include "thread_pool.hpp"

extern thread_pool pool;

namespace UniformSort {

    inline int64_t constexpr BUF_SIZE = 2 * 1024 * 1024;

    inline static bool IsPositionEmpty(const uint8_t *memory, uint32_t const entry_len)
    {
       // no entry can be larger than 256 bytes (the blake3 output size)
       const static uint8_t zeros[256] = {};
       return memcmp(memory, zeros, entry_len) == 0;
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
        auto buffer = std::make_unique<uint8_t[]>(BUF_SIZE);
        auto next_buffer = std::make_unique<uint8_t[]>(BUF_SIZE);
        uint64_t bucket_length = 0;

        uint64_t buf_size = 0;
        uint64_t next_buf_size = 0;
        uint64_t buf_ptr = 0;
        uint64_t swaps = 0;
        uint64_t read_pos = input_disk_begin;
        const uint64_t full_buffer_size = (uint64_t)BUF_SIZE / entry_len;

        std::future<bool> next_read_job;

        auto read_next = [&input_disk, full_buffer_size, num_entries, entry_len, &read_pos, input_disk_begin, &next_buffer, &next_buf_size, &next_read_job]() {
            next_buf_size = std::min(full_buffer_size, num_entries - ((read_pos - input_disk_begin) / entry_len));
            const uint64_t read_size = next_buf_size * entry_len;
            if (read_size == 0) {
              return;
            }
            next_read_job = pool.submit([&input_disk, read_pos, read_size, buffer = next_buffer.get()]{input_disk.Read(read_pos, buffer, read_size);});
            read_pos += next_buf_size * entry_len;
        };

        // Start reading the first block. Memset will take a while.
        read_next();

        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
        memset(memory, 0, memory_len);

        for (uint64_t i = 0; i < num_entries; i++) {
            if (buf_size == 0) {
                // If read buffer is empty, wait for the reader thread, get the
                // buffer and immediately start reading the next block.
                next_read_job.wait();
                buf_size = next_buf_size;
                buffer.swap(next_buffer);
                buf_ptr = 0;
                read_next();
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
