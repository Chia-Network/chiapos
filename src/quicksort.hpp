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

#ifndef SRC_CPP_QUICKSORT_HPP_
#define SRC_CPP_QUICKSORT_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <chrono>

#include "util.hpp"
#include "thread_pool.hpp"

namespace QuickSort {

    inline static void SortInner(
        uint8_t *memory,
        uint64_t memory_len,
        uint32_t L,
        uint32_t bits_begin,
        uint64_t begin,
        uint64_t end,
        thread_pool& pool)
    {
        auto const _pivot_space = std::make_unique<uint8_t[]>(L);
        auto *pivot_space = _pivot_space.get();
        if (end - begin <= 32) {
            for (uint64_t i = begin + 1; i < end; i++) {
                uint64_t j = i;
                memcpy(pivot_space, memory + i * L, L);
                while (j > begin &&
                       Util::MemCmpBits(memory + (j - 1) * L, pivot_space, L, bits_begin) > 0) {
                    memcpy(memory + j * L, memory + (j - 1) * L, L);
                    j--;
                }
                memcpy(memory + j * L, pivot_space, L);
            }
            return;
        }

        uint64_t lo = begin;
        uint64_t hi = end - 1;

        memcpy(pivot_space, memory + (hi * L), L);
        bool left_side = true;

        while (lo < hi) {
            if (left_side) {
                if (Util::MemCmpBits(memory + lo * L, pivot_space, L, bits_begin) < 0) {
                    ++lo;
                } else {
                    memcpy(memory + hi * L, memory + lo * L, L);
                    --hi;
                    left_side = false;
                }
            } else {
                if (Util::MemCmpBits(memory + hi * L, pivot_space, L, bits_begin) > 0) {
                    --hi;
                } else {
                    memcpy(memory + lo * L, memory + hi * L, L);
                    ++lo;
                    left_side = true;
                }
            }
        }
        memcpy(memory + lo * L, pivot_space, L);
        if (lo - begin <= end - lo) {
            pool.push_task(SortInner, memory, memory_len, L, bits_begin, begin, lo, std::ref(pool));
            pool.push_task(SortInner, memory, memory_len, L, bits_begin, lo + 1, end, std::ref(pool));
        } else {
            pool.push_task(SortInner, memory, memory_len, L, bits_begin, lo + 1, end, std::ref(pool));
            pool.push_task(SortInner, memory, memory_len, L, bits_begin, begin, lo, std::ref(pool));
        }
    }

    inline void Sort(
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {
        uint64_t const memory_len = (uint64_t)entry_len * num_entries;
        static thread_pool pool;
        const auto start = std::chrono::high_resolution_clock::now();
        pool.push_task(SortInner, memory, memory_len, entry_len, bits_begin, 0, num_entries, std::ref(pool));
        pool.wait_for_tasks();
        const auto end = std::chrono::high_resolution_clock::now();
        const auto elapsed = static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count());
        std::cout << "Parallel quick sort took " << elapsed << " mSec" << std::endl;
    }

}

#endif  // SRC_CPP_QUICKSORT_HPP_
