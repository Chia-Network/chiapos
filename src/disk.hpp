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

#ifndef SRC_CPP_DISK_HPP_
#define SRC_CPP_DISK_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

// enables disk I/O logging to disk.log
// use tools/disk.gnuplot to generate a plot
#define ENABLE_LOGGING 0

using namespace std::chrono_literals; // for operator""min;

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

#include "../lib/include/filesystem.hpp"

namespace fs = ghc::filesystem;

#include "./bits.hpp"
#include "./util.hpp"

struct Disk {
    virtual void Read(uint64_t begin, uint8_t *memcache, uint64_t length) = 0;

    virtual void Write(uint64_t begin, const uint8_t *memcache, uint64_t length) = 0;

    virtual void Truncate(uint64_t new_size) = 0;

    virtual std::string GetFileName() = 0;

    virtual ~Disk() = default;
};

#if ENABLE_LOGGING
#include <unordered_map>
#include <cinttypes>

enum class op_t { read, write};
void disk_log(fs::path const& filename, op_t const op, uint64_t offset, uint64_t length)
{
    static std::mutex m;
    static std::unordered_map<std::string, int> file_index;
    static auto const start_time = std::chrono::steady_clock::now();
    static int next_file = 0;

    auto const timestamp = std::chrono::steady_clock::now() - start_time;

    int fd = ::open("disk.log", O_WRONLY | O_CREAT | O_APPEND, 0755);

    std::unique_lock<std::mutex> l(m);

    char buffer[512];

    int const index = [&] {
        auto it = file_index.find(filename.string());
        if (it != file_index.end()) return it->second;
        file_index[filename.string()] = next_file;

        int const len = std::snprintf(buffer, sizeof(buffer)
            , "# %d %s\n", next_file, filename.string().c_str());
        ::write(fd, buffer, len);
        return next_file++;
    }();

    // timestamp (ms), start-offset, end-offset, operation (0 = read, 1 = write), file_index
    int const len = std::snprintf(buffer, sizeof(buffer)
        , "%" PRId64 "\t%" PRIu64 "\t%" PRIu64 "\t%d\t%d\n"
        , std::chrono::duration_cast<std::chrono::milliseconds>(timestamp).count()
        , offset
        , offset + length
        , op
        , index);
    ::write(fd, buffer, len);
    ::close(fd);
}
#endif

struct FileDisk : Disk {
    explicit FileDisk(const fs::path &filename)
    {
        filename_ = filename;

        // Opens the file for reading and writing
        f_ = ::fopen(filename.c_str(), "w+b");
        if (f_ == nullptr) {
            throw InvalidValueException(
                "Could not open " + filename.string() + ": " + ::strerror(errno));
        }
    }

    FileDisk(FileDisk &&fd)
    {
        filename_ = fd.filename_;
        f_ = fd.f_;
        fd.f_ = nullptr;
    }

    FileDisk(const FileDisk &) = delete;
    FileDisk &operator=(const FileDisk &) = delete;

    void Close()
    {
        if (f_ == nullptr) return;
        ::fclose(f_);
        f_ = nullptr;
    }

    ~FileDisk() { Close(); }

    void Read(uint64_t begin, uint8_t *memcache, uint64_t length) override
    {
#if ENABLE_LOGGING
        disk_log(filename_, op_t::read, begin, length);
#endif
        // Seek, read, and replace into memcache
        uint64_t amtread;
        do {
            if ((!bReading) || (begin != readPos)) {
#ifdef WIN32
                _fseeki64(f_, begin, SEEK_SET);
#else
                // fseek() takes a long as offset, make sure it's wide enough
                static_assert(sizeof(long) >= sizeof(begin));
                ::fseek(f_, begin, SEEK_SET);
#endif
                bReading = true;
            }
            amtread = ::fread(reinterpret_cast<char *>(memcache), sizeof(uint8_t), length, f_);
            readPos = begin + amtread;
            if (amtread != length) {
                std::cout << "Only read " << amtread << " of " << length << " bytes at offset "
                          << begin << " from " << filename_ << "with length " << writeMax
                          << ". Error " << ferror(f_) << ". Retrying in five minutes." << std::endl;
                std::this_thread::sleep_for(5min);
            }
        } while (amtread != length);
    }

    void Write(uint64_t begin, const uint8_t *memcache, uint64_t length) override
    {
#if ENABLE_LOGGING
        disk_log(filename_, op_t::write, begin, length);
#endif
        // Seek and write from memcache
        uint64_t amtwritten;
        do {
            if ((bReading) || (begin != writePos)) {
#ifdef WIN32
                _fseeki64(f_, begin, SEEK_SET);
#else
                // fseek() takes a long as offset, make sure it's wide enough
                static_assert(sizeof(long) >= sizeof(begin));
                ::fseek(f_, begin, SEEK_SET);
#endif
                bReading = false;
            }
            amtwritten =
                ::fwrite(reinterpret_cast<const char *>(memcache), sizeof(uint8_t), length, f_);
            writePos = begin + amtwritten;
            if (writePos > writeMax)
                writeMax = writePos;
            if (amtwritten != length) {
                std::cout << "Only wrote " << amtwritten << " of " << length << " bytes at offset "
                          << begin << " to " << filename_ << "with length " << writeMax
                          << ". Error " << ferror(f_) << ". Retrying in five minutes." << std::endl;
                std::this_thread::sleep_for(5min);
            }
        } while (amtwritten != length);
    }

    std::string GetFileName() override { return filename_.string(); }

    uint64_t GetWriteMax() const noexcept { return writeMax; }

    void Truncate(uint64_t new_size) override
    {
        Close();
        fs::resize_file(filename_, new_size);
        f_ = ::fopen(filename_.c_str(), "r+b");
        if (f_ == nullptr) {
            throw InvalidValueException(
                "Could not open " + filename_.string() + ": " + ::strerror(errno));
        }
    }

private:

    uint64_t readPos = 0;
    uint64_t writePos = 0;
    uint64_t writeMax = 0;
    bool bReading = true;

    fs::path filename_;
    FILE *f_;
};

struct BufferedDisk : Disk
{
    BufferedDisk(Disk* disk, uint64_t file_size) : disk_(disk), file_size_(file_size) {}

    void Read(uint64_t begin, uint8_t *memcache, uint64_t length) override
    {
        if (read_buffer_start_ <= begin && read_buffer_start_ + read_buffer_size_ >= begin + length) {
            // if the read is entirely inside the buffer, just memcopy it out
            ::memcpy(memcache, read_buffer_ + (begin - read_buffer_start_), length);
        }
        else if (begin >= read_buffer_start_ + read_buffer_size_ || begin == 0) {
            // if the read is beyond the current buffer (i.e.
            // forward-sequential) move the buffer forward and read the next
            // buffer-capacity number of bytes
            read_buffer_start_ = begin;
            uint64_t const amount_to_read = std::min(file_size_ - read_buffer_start_, read_buffer_capacity_);
            disk_->Read(begin, read_buffer_, amount_to_read);
            read_buffer_size_ = amount_to_read;
            ::memcpy(memcache, read_buffer_ + (begin - read_buffer_start_), length);
        }
        else {
            // if we're going backwards, don't wipe out the cache. We assume
            // forward sequential access
            disk_->Read(begin, memcache, length);
        }
    }

    void Write(uint64_t const begin, const uint8_t *memcache, uint64_t const length) override
    {
        if (begin == write_buffer_start_ + write_buffer_size_) {
            if (write_buffer_size_ + length <= write_buffer_capacity_) {
                ::memcpy(write_buffer_ + write_buffer_size_, memcache, length);
                write_buffer_size_ += length;
                return;
            }
            TryFlushWriteCache();
        }

        if (write_buffer_size_ == 0 && write_buffer_capacity_ >= length) {
            write_buffer_start_ = begin;
            ::memcpy(write_buffer_ + write_buffer_size_, memcache, length);
            write_buffer_size_ = length;
            return;
        }

        disk_->Write(begin, memcache, length);
    }

    void Truncate(uint64_t const new_size) override
    {
        TryFlushWriteCache();
        disk_->Truncate(new_size);
        file_size_ = new_size;
    }

    std::string GetFileName() override { return disk_->GetFileName(); }

    void SetReadCache(uint8_t* buffer, uint64_t size)
    {
        read_buffer_ = buffer;
        read_buffer_capacity_ = size;
        read_buffer_start_ = -1;
        read_buffer_size_ = 0;
    }

    void SetWriteCache(uint8_t* buffer, uint64_t size)
    {
        write_buffer_ = buffer;
        write_buffer_capacity_ = size;
        write_buffer_start_ = -1;
        write_buffer_size_ = 0;
    }

private:

    void TryFlushWriteCache()
    {
        if (write_buffer_size_ == 0) return;

        disk_->Write(write_buffer_start_, write_buffer_, write_buffer_size_);
        write_buffer_size_ = 0;
    }

    Disk* disk_;

    uint64_t file_size_;

    // the file offset the read buffer was read from
    uint64_t read_buffer_start_ = -1;
    uint8_t* read_buffer_ = nullptr;
    uint64_t read_buffer_size_ = 0;
    uint64_t read_buffer_capacity_ = 0;

    // the file offset the write buffer should be written back to
    // the write buffer is *only* for contiguous and sequential writes
    uint64_t write_buffer_start_ = -1;
    uint8_t* write_buffer_ = nullptr;
    uint64_t write_buffer_size_ = 0;
    uint64_t write_buffer_capacity_ = 0;
};

#endif  // SRC_CPP_DISK_HPP_
