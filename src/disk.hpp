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

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

#include "../lib/include/filesystem.hh"

namespace fs = ghc::filesystem;

#include "./bits.hpp"
#include "./util.hpp"

class Disk {
public:
    virtual void Read(uint64_t begin, uint8_t *memcache, uint64_t length) = 0;

    virtual void Write(uint64_t begin, const uint8_t *memcache, uint64_t length) = 0;

    virtual void Truncate(uint64_t new_size) = 0;

    virtual std::string GetFileName() = 0;

    virtual ~Disk(){};
};

class FileDisk : public Disk {
public:
    inline explicit FileDisk(const fs::path &filename)
    {
        filename_ = filename;

        // Opens the file for reading and writing
        f_ = fopen(filename.c_str(), "w+b");
    }

    FileDisk(FileDisk &&fd)
    {
        filename_ = fd.filename_;
        f_ = fd.f_;
        fd.f_ = NULL;
    }

    bool isOpen() { return (f_ != NULL); }

    void Close()
    {
        if (f_ != NULL)
            fclose(f_);
    }

    ~FileDisk()
    {
        if (f_ != NULL)
            fclose(f_);
    }

    inline void Read(uint64_t begin, uint8_t *memcache, uint64_t length) override
    {
        // Seek, read, and replace into memcache
        uint64_t amtread;
        do {
            if ((!bReading) || (begin != readPos)) {
#ifdef WIN32
                _fseeki64(f_, begin, SEEK_SET);
#else
                fseek(f_, begin, SEEK_SET);
#endif
                bReading = true;
            }
            amtread = fread(reinterpret_cast<char *>(memcache), sizeof(uint8_t), length, f_);
            readPos = begin + amtread;
            if (amtread != length) {
                std::cout << "Only read " << amtread << " of " << length << " bytes at offset "
                          << begin << " from " << filename_ << "with length " << writeMax
                          << ". Error " << ferror(f_) << ". Retrying in five minutes." << std::endl;
#ifdef WIN32
                Sleep(5 * 60000);
#else
                sleep(5 * 60);
#endif
            }
        } while (amtread != length);
    }

    inline void Write(uint64_t begin, const uint8_t *memcache, uint64_t length) override
    {
        // Seek and write from memcache
        uint64_t amtwritten;
        do {
            if ((bReading) || (begin != writePos)) {
#ifdef WIN32
                _fseeki64(f_, begin, SEEK_SET);
#else
                fseek(f_, begin, SEEK_SET);
#endif
                bReading = false;
            }
            amtwritten =
                fwrite(reinterpret_cast<const char *>(memcache), sizeof(uint8_t), length, f_);
            writePos = begin + amtwritten;
            if (writePos > writeMax)
                writeMax = writePos;
            if (amtwritten != length) {
                std::cout << "Only wrote " << amtwritten << " of " << length << " bytes at offset "
                          << begin << " to " << filename_ << "with length " << writeMax
                          << ". Error " << ferror(f_) << ". Retrying in five minutes." << std::endl;
#ifdef WIN32
                Sleep(5 * 60000);
#else
                sleep(5 * 60);
#endif
            }
        } while (amtwritten != length);
    }

    inline std::string GetFileName() override { return filename_.string(); }

    inline uint64_t GetWriteMax() const noexcept { return writeMax; }

    inline void Truncate(uint64_t new_size) override
    {
        if (f_ != NULL)
            fclose(f_);
        fs::resize_file(filename_, new_size);
        f_ = fopen(filename_.c_str(), "r+b");
    }

private:
    FileDisk(const FileDisk &);

    FileDisk &operator=(const FileDisk &);

    uint64_t readPos = 0;
    uint64_t writePos = 0;
    uint64_t writeMax = 0;
    bool bReading = true;

    fs::path filename_;
    FILE *f_;
};

#endif  // SRC_CPP_DISK_HPP_