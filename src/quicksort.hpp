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

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

#include "./util.hpp"

class QuickSort {
public:
    inline static void Sort(
        uint8_t *memory,
        uint32_t entry_len,
        uint64_t num_entries,
        uint32_t bits_begin)
    {
        uint64_t memory_len = (uint64_t)entry_len * num_entries;
        uint8_t *pivot_space = new uint8_t[entry_len];
        SortInner(memory, memory_len, entry_len, bits_begin, 0, num_entries, pivot_space);
        delete[] pivot_space;
    }

private:
    /*
     * Like memcmp, but only compares starting at a certain bit.
     */
    inline static int MemCmpBits(
        uint8_t *left_arr,
        uint8_t *right_arr,
        uint32_t len,
        uint32_t bits_begin)
    {
        uint32_t start_byte = bits_begin / 8;
        uint8_t mask = ((1 << (8 - (bits_begin % 8))) - 1);
        if ((left_arr[start_byte] & mask) != (right_arr[start_byte] & mask)) {
            return (left_arr[start_byte] & mask) - (right_arr[start_byte] & mask);
        }

        for (uint32_t i = start_byte + 1; i < len; i++) {
            if (left_arr[i] != right_arr[i])
                return left_arr[i] - right_arr[i];
        }
        return 0;
    }

    inline static void SortInner(
        uint8_t *memory,
        uint64_t memory_len,
        uint32_t L,
        uint32_t bits_begin,
        uint64_t begin,
        uint64_t end,
        uint8_t *pivot_space)
    {
        if (end - begin <= 5) {
            for (uint64_t i = begin + 1; i < end; i++) {
                uint64_t j = i;
                memcpy(pivot_space, memory + i * L, L);
                while (j > begin &&
                       MemCmpBits(memory + (j - 1) * L, pivot_space, L, bits_begin) > 0) {
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
                if (MemCmpBits(memory + lo * L, pivot_space, L, bits_begin) < 0) {
                    ++lo;
                } else {
                    memcpy(memory + hi * L, memory + lo * L, L);
                    --hi;
                    left_side = false;
                }
            } else {
                if (MemCmpBits(memory + hi * L, pivot_space, L, bits_begin) > 0) {
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
            SortInner(memory, memory_len, L, bits_begin, begin, lo, pivot_space);
            SortInner(memory, memory_len, L, bits_begin, lo + 1, end, pivot_space);
        } else {
            SortInner(memory, memory_len, L, bits_begin, lo + 1, end, pivot_space);
            SortInner(memory, memory_len, L, bits_begin, begin, lo, pivot_space);
        }
    }
};

#endif  // SRC_CPP_QUICKSORT_HPP_
