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

#include <memory>

#ifndef _MSC_VER
#ifndef __x86_64__
inline int hasPOPCNT()
{
    return false;
}
#else
int bCheckedPOPCNT;
int bPOPCNT;

inline int hasPOPCNT()
{
    if(bCheckedPOPCNT)
        return bPOPCNT;

    bCheckedPOPCNT = 1;
    int info[4] = {0};
    #if defined(_MSC_VER)
        __cpuid(info, 0x00000001);
    #elif defined(__GNUC__) || defined(__clang__)
        #if defined(ARCH_X86) && defined(__PIC__)
            __asm__ __volatile__ (
                "xchg{l} {%%}ebx, %k1;"
                "cpuid;"
                "xchg{l} {%%}ebx, %k1;"
                : "=a"(info[0]), "=&r"(info[1]), "=c"(info[2]), "=d"(info[3]) : "a"(0x00000001), "c"(0)
            );
        #else
            __asm__ __volatile__ (
                "cpuid" : "=a"(info[0]), "=b"(info[1]), "=c"(info[2]), "=d"(info[3]) : "a"(0x00000001), "c"(0)
            );
        #endif
    #endif

    bPOPCNT = ((info[2] & (1 << 23)) != 0);
    return bPOPCNT;
}

#endif
#endif

struct bitfield
{
    explicit bitfield(int64_t size)
        : buffer_(new uint64_t[(size + 63) / 64])
        , size_((size + 63) / 64)
    {
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
        std::memset(buffer_.get(), 0, size_ * 8);
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

        uint64_t const* start = buffer_.get() + start_bit / 64;
        uint64_t const* end = buffer_.get() + end_bit / 64;
        int64_t ret = 0;

        if(hasPOPCNT())
        {
            while (start != end) {
#ifdef _MSC_VER
                ret += __popcnt64(*start);
#else
                uint64_t x;
                __asm__ volatile("popcntq %0, %1"
                     : "=r" (x)
                     : "0" (*start));
                ret += x;
#endif
                ++start;
            }
        }
        else {
            while (start != end) {
#ifdef _MSC_VER 
                // Need fallback here for MSVC
                ret += __popcnt64(*start);
#else           
                ret += __builtin_popcountl(*start);
#endif
                ++start;
            }
        }
        int const tail = end_bit % 64;
        if (tail > 0) {
            uint64_t const mask = (uint64_t(1) << tail) - 1;
#ifdef _MSC_VER
            ret += __popcnt64(*end & mask);
#else
            if(hasPOPCNT())
            {
                uint64_t x;
                __asm__ volatile("popcntq %0, %1"
                     : "=r" (x)
                     : "0" (*end & mask));
                ret += x;
            } 
            else {
                ret += __builtin_popcountl(*end & mask);
            }
#endif
        }
        return ret;
    }

    void free_memory()
    {
        buffer_.reset();
        size_ = 0;
    }
private:
    std::unique_ptr<uint64_t[]> buffer_;

    // number of 64-bit words
    int64_t size_;
};
