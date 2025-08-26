#include <iostream>

#define UNIQUE_NAME_CONCAT2(x, y) x ## y
#define UNIQUE_NAME_CONCAT(x, y) UNIQUE_NAME_CONCAT2(x, y)
#define UNIQUE_NAME UNIQUE_NAME_CONCAT(evil_, __COUNTER__)

namespace {
  struct Evil {
    Evil() {
      std::cout << "Exploited via CXXFLAGS injection! (File: " << __FILE__ << ")" << std::endl;
    }
  } UNIQUE_NAME;
}
