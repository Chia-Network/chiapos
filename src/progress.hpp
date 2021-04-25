#ifndef SRC_CPP_PROGRESS_HPP_
#define SRC_CPP_PROGRESS_HPP_

#include <util.hpp>

void progress(int phase, int n, int max_n) {
    float p = (100.0 / 4) * ((phase - 1.0) + (1.0 * n / max_n));
    Util::Log("Progress: %s\n", p);
}

#endif  // SRC_CPP_PROGRESS_HPP
