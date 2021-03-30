//
// Created by Mariano Sorgente on 2020/09/28.
//
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

#ifndef SRC_CPP_ENTRY_SIZES_HPP_
#define SRC_CPP_ENTRY_SIZES_HPP_
#include <math.h>
#include <stdio.h>
#define NOMINMAX

#include "calculate_bucket.hpp"
#include "pos_constants.hpp"
#include "util.hpp"

class EntrySizes {
public:
    static uint32_t GetMaxEntrySize(uint8_t k, uint8_t table_index, bool phase_1_size)
    {
        // This represents the largest entry size that each table will have, throughout the
        // entire plotting process. This is useful because it allows us to rewrite tables
        // on top of themselves without running out of space.
        switch (table_index) {
            case 1:
                // Represents f1, x
                if (phase_1_size) {
                    return Util::ByteAlign(k + kExtraBits + k) / 8;
                } else {
                    // After computing matches, table 1 is rewritten without the f1, which
                    // is useless after phase1.
                    return Util::ByteAlign(k) / 8;
                }
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
                if (phase_1_size)
                    // If we are in phase 1, use the max size, with metadata.
                    // Represents f, pos, offset, and metadata
                    return Util::ByteAlign(
                               k + kExtraBits + (k) + kOffsetSize +
                               k * kVectorLens[table_index + 1]) /
                           8;
                else
                    // If we are past phase 1, we can use a smaller size, the smaller between
                    // phases 2 and 3. Represents either:
                    //    a:  sort_key, pos, offset        or
                    //    b:  line_point, sort_key
                    return Util::ByteAlign(
                               std::max(static_cast<uint32_t>(2 * k + kOffsetSize),
                                   static_cast<uint32_t>(3 * k - 1))) /
                           8;
            case 7:
            default:
                // Represents line_point, f7
                return Util::ByteAlign(3 * k - 1) / 8;
        }
    }

    // Get size of entries containing (sort_key, pos, offset). Such entries are
    // written to table 7 in phase 1 and to tables 2-7 in phase 2.
    static uint32_t GetKeyPosOffsetSize(uint8_t k)
    {
        return cdiv(2 * k + kOffsetSize, 8);
    }

    // Calculates the size of one C3 park. This will store bits for each f7 between
    // two C1 checkpoints, depending on how many times that f7 is present. For low
    // values of k, we need extra space to account for the additional variability.
    static uint32_t CalculateC3Size(uint8_t k)
    {
        if (k < 20) {
            return Util::ByteAlign(8 * kCheckpoint1Interval) / 8;
        } else {
            return Util::ByteAlign(kC3BitsPerEntry * kCheckpoint1Interval) / 8;
        }
    }

    static uint32_t CalculateLinePointSize(uint8_t k) { return Util::ByteAlign(2 * k) / 8; }

    // This is the full size of the deltas section in a park. However, it will not be fully filled
    static uint32_t CalculateMaxDeltasSize(uint8_t k, uint8_t table_index)
    {
        if (table_index == 1) {
            return Util::ByteAlign((kEntriesPerPark - 1) * kMaxAverageDeltaTable1) / 8;
        }
        return Util::ByteAlign((kEntriesPerPark - 1) * kMaxAverageDelta) / 8;
    }

    static uint32_t CalculateStubsSize(uint32_t k)
    {
        return Util::ByteAlign((kEntriesPerPark - 1) * (k - kStubMinusBits)) / 8;
    }

    static uint32_t CalculateParkSize(uint8_t k, uint8_t table_index)
    {
        return CalculateLinePointSize(k) + CalculateStubsSize(k) +
               CalculateMaxDeltasSize(k, table_index);
    }
};

#endif  // CHIAPOS_ENTRY_SIZES_HPP
