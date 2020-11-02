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

#ifndef SRC_CPP_PROVER_DISK_HPP_
#define SRC_CPP_PROVER_DISK_HPP_

#ifndef _WIN32
#include <unistd.h>
#endif
#include <stdio.h>

#include <algorithm>  // std::min
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

struct plot_header {
    uint8_t magic[19];
    uint8_t id[32];
    uint8_t k;
    uint8_t fmt_desc_len[2];
    uint8_t fmt_desc[50];
};

// The DiskProver, given a correctly formatted plot file, can efficiently generate valid proofs
// of space, for a given challenge.
class DiskProver {
public:
    // The costructor opens the file, and reads the contents of the file header. The table pointers
    // will be used to find and seek to all seven tables, at the time of proving.
    explicit DiskProver(std::string filename)
    {
    }

};

#endif  // SRC_CPP_PROVER_DISK_HPP_
