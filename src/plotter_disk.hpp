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

#ifndef SRC_CPP_PLOTTER_DISK_HPP_
#define SRC_CPP_PLOTTER_DISK_HPP_

#ifndef _WIN32
#include <semaphore.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <memory>

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

class DiskPlotter {
public:
    // This method creates a plot on disk with the filename. A temporary file, "plotting" +
    // filename, is created and will be larger than the final plot file. This file is deleted at the
    // end of the process.
    void CreatePlotDisk(
        std::string tmp_dirname,
        std::string tmp2_dirname,
        std::string final_dirname,
        std::string filename,
        uint8_t k,
        const uint8_t* memo,
        uint32_t memo_len,
        const uint8_t* id,
        uint32_t id_len,
        uint32_t buf_megabytes_input = 0,
        uint32_t num_buckets_input = 0,
        uint64_t stripe_size_input = 0,
        uint8_t num_threads_input = 0)
    {
    }

};

#endif  // SRC_CPP_PLOTTER_DISK_HPP_
