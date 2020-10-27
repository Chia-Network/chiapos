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

struct bitfield
{
    bitfield(uint8_t* buffer, int64_t num_bytes)
        : buffer_(reinterpret_cast<uint64_t*>(buffer))
        , size_(num_bytes / 8)
    {
        // we want this buffer to be 8-byte aligned
        // both the pointer and size
        assert((uintptr_t(buffer) & 7) == 0);
        assert((num_bytes % 8) == 0);

        clear();
    }

    void set(int64_t const bit)
    {
        assert(bit / 64 < size_);
        buffer_[bit / 64] |= uint64_t(1) << (bit % 64);
    }

    bool get(int64_t const bit) const
    {
        assert(bit / 64 < size_);
        return (buffer_[bit / 64] & (uint64_t(1) << (bit % 64))) != 0;
    }

    void clear()
    {
        std::memset(buffer_, 0, size_ * 8);
    }

    int64_t size() const { return size_ * 64; }

    void swap(bitfield& rhs)
    {
        using std::swap;
        swap(buffer_, rhs.buffer_);
        swap(size_, rhs.size_);
    }

    int64_t count(int64_t const start_bit, int64_t const end_bit) const
    {
        assert((start_bit % 64) == 0);
        assert(start_bit <= end_bit);

        uint64_t const* start = buffer_ + start_bit / 64;
        uint64_t const* end = buffer_ + end_bit / 64;
        int64_t ret = 0;
        while (start != end) {
#ifdef _MSC_VER
            ret += __popcnt64(*start);
#else
            ret += __builtin_popcountl(*start);
#endif
            ++start;
        }
        int const tail = end_bit % 64;
        if (tail > 0) {
            uint64_t const mask = (uint64_t(1) << tail) - 1;
#ifdef _MSC_VER
            ret += __popcnt64(*end & mask);
#else
            ret += __builtin_popcountl(*end & mask);
#endif
        }
        return ret;
    }
private:
    uint64_t* buffer_;

    // number of 64-bit words
    int64_t size_;
};
