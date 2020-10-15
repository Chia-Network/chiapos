// Copyright 2020 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>

struct bitfield_index
{
    // cache the number of set bits evey n bits. This is n. For a bitfield of
    // size 2^32, this means a 2 MiB index
    static inline const int64_t kIndexBucket = 16 * 1024;

    bitfield_index(std::vector<bool> const& b) : bitfield_(b)
    {
        uint64_t counter = 0;
        auto it = bitfield_.begin();
        index_.reserve(bitfield_.size() / kIndexBucket);

        for (int64_t idx = 0; idx < int64_t(bitfield_.size()); idx += kIndexBucket) {
            index_.push_back(counter);
            int64_t const left = std::min(int64_t(bitfield_.size()) - idx, kIndexBucket);
            counter += std::count(it, it + left, true);
            it += left;
        }
    }

    std::pair<uint64_t, uint64_t> lookup(uint64_t pos, uint64_t offset) const
    {
        uint64_t const bucket = pos / kIndexBucket;

        assert(bucket < index_.size());
        assert(pos < bitfield_.size());
        assert(pos + offset < bitfield_.size());

        uint64_t const base = index_[bucket];

        uint64_t const diff = std::count(
                bitfield_.begin() + (bucket * kIndexBucket), bitfield_.begin() + pos, true);
        uint64_t const new_offset = std::count(
            bitfield_.begin() + pos
            , bitfield_.begin() + pos + offset, true);

        assert(new_offset <= offset);

        return { base + diff, new_offset };
    }
private:
    std::vector<bool> const& bitfield_;
    std::vector<uint64_t> index_;
};

