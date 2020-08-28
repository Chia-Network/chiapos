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

#ifndef SRC_CPP_SORT_ON_DISK_HPP_
#define SRC_CPP_SORT_ON_DISK_HPP_

#define BUF_SIZE 262144

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include "./util.hpp"


class SortOnDiskUtils {
 public:
    /*
     * Given an array of bytes, extracts an unsigned 64 bit integer from the given
     * index, to the given index.
     */
    inline static uint64_t ExtractNum(uint8_t* bytes, uint32_t len_bytes, uint32_t begin_bits, uint32_t take_bits) {
        if ((begin_bits + take_bits) / 8 > len_bytes - 1) {
            take_bits = len_bytes * 8 - begin_bits;
        }
        return Util::SliceInt64FromBytes(bytes, len_bytes, begin_bits, take_bits);
    }

    /*
     * Like memcmp, but only compares starting at a certain bit.
     */
    inline static int MemCmpBits(uint8_t* left_arr, uint8_t* right_arr, uint32_t len, uint32_t bits_begin) {
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

    // The number of memory entries required to do the custom SortInMemory algorithm, given the total number of entries to be sorted.
    inline static uint64_t RoundSize(uint64_t size) {
        size *= 2;
        uint64_t result = 1;
        while (result < size)
            result *= 2;
        return result + 50;
    }

    inline static bool IsPositionEmpty(uint8_t* memory, uint32_t entry_len) {
        for (uint32_t i = 0; i < entry_len; i++)
            if (memory[i] != 0)
                return false;
        return true;
    }
};

int g_bits_begin;
int g_entry_len;

int cmpfunc (const void * a, const void * b) {
   return SortOnDiskUtils::MemCmpBits((uint8_t *)a, (uint8_t *)b, g_entry_len, g_bits_begin);
}

class Disk {
 public:
    virtual void Read(uint64_t begin, uint8_t* memcache, uint64_t length) = 0;
    virtual void Write(uint64_t begin, const uint8_t* memcache, uint64_t length) = 0;
    virtual uint8_t *getBuf() = 0;
};

class FileDisk : public Disk {
 public:
    inline explicit FileDisk(const std::string& filename, bool mem = false) {
        filename_ = filename;
        mem_ = mem;

        if(mem_) {
            buf = new uint8_t[1*1024*1024*1024];

            return;
        }

        // Opens the file for reading and writing
        f_=fopen(filename.c_str(), "w+b");
    }

    bool isOpen() {
       if(mem_)
           return true;

       return (f_!=NULL);
    }

    ~FileDisk() {
        if(mem_) {
            delete[] buf;
        }

        if(f_!=NULL)
            fclose(f_);
    }

    uint8_t *getBuf() {
        return buf;
    }

    inline void Read(uint64_t begin, uint8_t* memcache, uint64_t length) override {
        if(mem_) {
            memcpy(memcache,&(buf[begin]),length);
            return;
        }

        // Seek, read, and replace into memcache
        uint64_t amtread;
        do {
            if((!bReading)||(begin!=readPos)) {
#ifdef WIN32
                _fseeki64(f_,begin,SEEK_SET);
#else
                fseek(f_, begin, SEEK_SET);
#endif
                bReading=true;
            }
            amtread = fread(reinterpret_cast<char*>(memcache), sizeof(uint8_t), length, f_);
            readPos=begin + amtread;
            if(amtread != length) {
                std::cout << "Only read " << amtread << " of " << length << " bytes at offset " << begin << " from " << filename_ << "with length " << writeMax << ". Error " << ferror(f_) << ". Retrying in five minutes." << std::endl;
#ifdef WIN32
                Sleep(5 * 60000);
#else
                sleep(5 * 60);
#endif
            }
        } while (amtread != length);
    }

    inline void Write(uint64_t begin, const uint8_t* memcache, uint64_t length) override {
        if(mem_) {
            memcpy(&(buf[begin]),memcache,length);
            return;
        }

        // Seek and write from memcache
        uint64_t amtwritten;
        do {
            if((bReading)||(begin!=writePos)) {
#ifdef WIN32
                _fseeki64(f_,begin,SEEK_SET);
#else
                fseek(f_, begin, SEEK_SET);
#endif
                bReading=false;
            }
            amtwritten = fwrite(reinterpret_cast<const char*>(memcache), sizeof(uint8_t), length, f_);
            writePos=begin+amtwritten;
	    if(writePos > writeMax)
                writeMax = writePos;
            if(amtwritten != length) {
                std::cout << "Only wrote " << amtwritten << " of " << length << " bytes at offset " << begin << " to " << filename_ << "with length " << writeMax << ". Error " << ferror(f_) << ". Retrying in five minutes." << std::endl;
#ifdef WIN32
                Sleep(5 * 60000);
#else
                sleep(5 * 60);
#endif
            }
        } while (amtwritten != length);
    }

    inline std::string GetFileName() const noexcept {
        return filename_;
    }

    inline uint64_t GetWriteMax() const noexcept {
        return writeMax;
    }


 private:
    uint64_t readPos=0;
    uint64_t writePos=0;
    uint64_t writeMax=0;
    bool bReading=true;
    bool mem_;

    std::string filename_;
    FILE *f_;

    uint8_t *buf;

};

// Store values bucketed by their leading bits into an array-like memcache.
// The memcache stores stacks of values, one for each bucket.
// The stacks are broken into segments, where each segment has content
// all from the same bucket, and a 4 bit pointer to its previous segment.
// The most recent segment is the head segment of that bucket.
// Additionally, empty segments form a linked list: 4 bit pointers of
// empty segments point to the next empty segment in the memcache.
// Each segment has size entries_per_seg * entry_len + 4, and consists of:
// [4 bit pointer to segment id] + [entries of length entry_len]*

class BucketStore {
 public:
    inline BucketStore(uint8_t* mem, uint64_t mem_len, uint32_t entry_len,
                       uint32_t bits_begin, uint32_t bucket_log, uint64_t entries_per_seg) {
        mem_ = mem;
        mem_len_ = mem_len;
        entry_len_ = entry_len;
        bits_begin_ = bits_begin;
        bucket_log_ = bucket_log;
        entries_per_seg_ = entries_per_seg;

        for (uint64_t i = 0; i < (1UL << bucket_log); i++) {
            bucket_sizes_.push_back(0);
        }

        seg_size_ = 4 + entry_len_ * entries_per_seg;

        length_ = mem_len / seg_size_;

        // Initially, all the segments are empty, store them as a linked list,
        // where a segment points to the next empty segment.
        for (uint64_t i = 0; i < length_; i++) {
            SetSegmentId(i, i + 1);
        }

        // The head of the empty segments list.
        first_empty_seg_id_ = 0;

        // Initially, all bucket lists contain no segments in it.
        for (uint64_t i = 0; i < bucket_sizes_.size(); i++) {
            bucket_head_ids_.push_back(length_);
            bucket_head_counts_.push_back(0);
        }
    }

    inline void SetSegmentId(uint64_t i, uint64_t v) {
        Util::IntToFourBytes(mem_ + i * seg_size_, v);
    }

    inline uint64_t GetSegmentId(uint64_t i) const {
        return Util::FourBytesToInt(mem_ + i * seg_size_);
    }

    // Get the first empty position from the head segment of bucket b.
    inline uint64_t GetEntryPos(uint64_t b) const {
        return bucket_head_ids_[b] * seg_size_ + 4
               + bucket_head_counts_[b] * entry_len_;
    }

    inline void Audit() const {
        uint64_t count = 0;
        uint64_t pos = first_empty_seg_id_;

        while (pos != length_) {
            ++count;
            pos = GetSegmentId(pos);
        }
        for (uint64_t pos2 : bucket_head_ids_) {
            while (pos2 != length_) {
                ++count;
                pos2 = GetSegmentId(pos2);
            }
        }
        assert(count == length_);
    }

    inline uint64_t NumFree() const {
        uint64_t used = GetSegmentId(first_empty_seg_id_);
        return (bucket_sizes_.size() - used) * entries_per_seg_;
    }

    inline bool IsEmpty() const noexcept {
        for (uint64_t s : bucket_sizes_) {
            if (s > 0) return false;
        }
        return true;
    }

    inline bool IsFull() const noexcept {
        return first_empty_seg_id_ == length_;
    }

    inline void Store(uint8_t* new_val, uint64_t new_val_len) {
        assert(new_val_len == entry_len_);
        assert(first_empty_seg_id_ != length_);
        uint64_t b = SortOnDiskUtils::ExtractNum(new_val, new_val_len, bits_begin_, bucket_log_);
        bucket_sizes_[b] += 1;

        // If bucket b contains no segments, or the head segment of bucket b is full, append a new segment.
        if (bucket_head_ids_[b] == length_ ||
                bucket_head_counts_[b] == entries_per_seg_) {
            uint64_t old_seg_id = bucket_head_ids_[b];
            // Set the head of the bucket b with the first empty segment (thus appending a new segment to the bucket b).
            bucket_head_ids_[b] = first_empty_seg_id_;
            // Move the first empty segment to the next empty one
            // (which is linked with the first empty segment using id, since empty segments
            // form a linked list).
            first_empty_seg_id_ = GetSegmentId(first_empty_seg_id_);
            // Link the head of bucket b to the previous head (in the linked list,
            // the segment that will follow the new head will be the previous head).
            SetSegmentId(bucket_head_ids_[b], old_seg_id);
            bucket_head_counts_[b] = 0;
        }

        // Get the first empty position inside the head segment and write the entry there.
        uint64_t pos = GetEntryPos(b);
        memcpy(mem_ + pos, new_val, entry_len_);
        bucket_head_counts_[b] += 1;
    }

    inline uint64_t MaxBucket() const {
        uint64_t max_bucket_size = bucket_sizes_[0];
        uint64_t max_index = 0;
        for (uint64_t i = 1; i < bucket_sizes_.size(); i++) {
            if (bucket_sizes_[i] > max_bucket_size) {
                max_bucket_size = bucket_sizes_[i];
                max_index = i;
            }
        }
        return max_index;
    }

    inline std::vector<uint64_t> BucketsBySize() const {
        // Lukasz Wiklendt (https://stackoverflow.com/questions/1577475/c-sorting-and-keeping-track-of-indexes)
        std::vector<uint64_t> idx(bucket_sizes_.size());
        iota(idx.begin(), idx.end(), 0);
        sort(idx.begin(), idx.end(),
             [this](uint64_t i1, uint64_t i2) {return bucket_sizes_[i1] > bucket_sizes_[i2];});
        return idx;
    }

    // Similar to how 'Bits' class works, appends an entry to the entries list, such as all entries are stored into 64-bit blocks.
    // Bits class was avoided since it consumes more time than a uint64_t array.
    static void AddBucketEntry(uint8_t* big_endian_bytes, uint64_t num_bytes, uint16_t size_bits, uint64_t* entries, uint64_t& cnt) {
        assert(size_bits / 8 >= num_bytes);
        uint16_t extra_space = size_bits - num_bytes * 8;
        uint64_t init_cnt = cnt;
        uint16_t last_size = 0;
        while (extra_space >= 64) {
            extra_space -= 64;
            entries[cnt++] = 0;
            last_size = 64;
        }
        if (extra_space > 0) {
            entries[cnt++] = 0;
            last_size = extra_space;
        }
        for (uint64_t i = 0; i < num_bytes; i += 8) {
            uint64_t val = 0;
            uint8_t bucket_size = 0;
            for (uint64_t j = i; j < i + 8 && j < num_bytes; j++) {
                val = (val << 8) + big_endian_bytes[j];
                bucket_size += 8;
            }
            if (cnt == init_cnt || last_size == 64) {
                entries[cnt++] = val;
                last_size = bucket_size;
            } else {
                uint8_t free_space = 64 - last_size;
                if (free_space >= bucket_size) {
                    entries[cnt - 1] = (entries[cnt - 1] << bucket_size) + val;
                    last_size += bucket_size;
                } else {
                    uint8_t suffix_size = bucket_size - free_space;
                    uint64_t mask = (static_cast<uint64_t>(1)) << suffix_size;
                    mask--;
                    uint64_t suffix = (val & mask);
                    uint64_t prefix = (val >> suffix_size);
                    entries[cnt - 1] = (entries[cnt - 1] << free_space) + prefix;
                    entries[cnt++] = suffix;
                    last_size = suffix_size;
                }
            }
        }
    }

    // Extracts 'number_of_entries' from bucket b and empties memory of those from BucketStore.
    inline uint64_t* BucketHandle(uint64_t b, uint64_t number_of_entries, uint64_t& final_size) {
        uint32_t L = entry_len_;
        uint32_t entry_size = L / 8;
        if (L % 8)
            ++entry_size;
        uint64_t cnt = 0;
        uint64_t cnt_entries = 0;
        // Entry bytes will be compressed into uint64_t array.
        uint64_t* entries = new uint64_t[number_of_entries * entry_size];

        // As long as we have a head segment in bucket b...
        while (bucket_head_ids_[b] != length_) {
            // ...extract the entries from it.
            uint64_t start_pos = GetEntryPos(b) - L;
            uint64_t end_pos = start_pos - bucket_head_counts_[b] * L;
            for (uint64_t pos = start_pos; pos > end_pos + L; pos -=L) {
                bucket_sizes_[b] -= 1;
                bucket_head_counts_[b] -= 1;
                AddBucketEntry(mem_ + pos, L, L*8, entries, cnt);
                ++cnt_entries;
                if (cnt_entries == number_of_entries) {
                    final_size = cnt;
                    return entries;
                }
            }

            // Move to the next segment from bucket b.
            uint64_t next_full_seg_id = GetSegmentId(bucket_head_ids_[b]);
            // The processed segment becomes now an empty segment.
            SetSegmentId(bucket_head_ids_[b], first_empty_seg_id_);
            first_empty_seg_id_ = bucket_head_ids_[b];
            // Change the head of bucket b.
            bucket_head_ids_[b] = next_full_seg_id;

            if (next_full_seg_id == length_) {
                bucket_head_counts_[b] = 0;
            } else {
                bucket_head_counts_[b] = entries_per_seg_;
            }

            if (start_pos != end_pos) {
                bucket_sizes_[b] -= 1;
                AddBucketEntry(mem_ + end_pos + L, L, L*8, entries, cnt);
                ++cnt_entries;
                if (cnt_entries == number_of_entries) {
                    final_size = cnt;
                    return entries;
                }
            }
        }

        assert(bucket_sizes_[b] == 0);
        final_size = cnt;
        return entries;
    }

 private:
    uint8_t* mem_;
    uint64_t mem_len_;
    uint32_t bits_begin_;
    uint32_t entry_len_;
    uint32_t bucket_log_;
    uint64_t entries_per_seg_;
    std::vector<uint64_t> bucket_sizes_;
    uint64_t seg_size_;
    uint64_t length_;
    uint64_t first_empty_seg_id_;
    std::vector<uint64_t> bucket_head_ids_;
    std::vector<uint64_t> bucket_head_counts_;
};

class Sorting {
 public:
    static void EntryToBytes(uint64_t* entries, uint32_t start_pos, uint32_t end_pos, uint8_t last_size, uint8_t buffer[]) {
        uint8_t shift = Util::ByteAlign(last_size) - last_size;
        uint64_t val = entries[end_pos - 1] << (shift);
        uint16_t cnt = 0;
        uint8_t iterations = last_size / 8;
        if (last_size % 8)
            iterations++;
        for (uint8_t i = 0; i < iterations; i++) {
            buffer[cnt++] = (val & 0xff);
            val >>= 8;
        }

        if (end_pos - start_pos >= 2) {
            for (int32_t i = end_pos - 2; i >= (int32_t) start_pos; i--) {
                uint64_t val = entries[i];
                for (uint8_t j = 0; j < 8; j++) {
                    buffer[cnt++] = (val & 0xff);
                    val >>= 8;
                }
            }
        }
        uint32_t left = 0, right = cnt - 1;
        while (left <= right) {
            std::swap(buffer[left], buffer[right]);
            left++;
            right--;
        }
    }

    inline static uint64_t SortOnDisk(Disk& disk, uint64_t disk_begin, uint64_t spare_begin,
                                      uint32_t entry_len, uint32_t bits_begin, std::vector<uint64_t> bucket_sizes,
                                      uint8_t* mem, uint64_t mem_len, int quicksort = 0) {

        uint64_t length = mem_len / entry_len;
        uint64_t total_size = 0;
        // bucket_sizes[i] represent how many entries start with the prefix i (from 0000 to 1111).
        // i.e. bucket_sizes[10] represents how many entries start with the prefix 1010.
        for (auto& n : bucket_sizes) total_size += n;
        uint64_t N_buckets = bucket_sizes.size();

        assert(disk_begin + total_size * entry_len <= spare_begin);

        if (bits_begin >= entry_len * 8) {
            return 0;
        }

        // If we have enough memory to sort the entries, do it.

        // How much an entry occupies in memory, without the common prefix, in SortInMemory algorithm.
        uint32_t entry_len_memory = entry_len - bits_begin / 8;

        g_entry_len=entry_len;
        g_bits_begin=bits_begin;
        qsort(&((disk.getBuf())[disk_begin]),total_size,entry_len,cmpfunc);
        return 0;
    }
};

#endif  // SRC_CPP_SORT_ON_DISK_HPP_
