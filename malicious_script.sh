@!/bin/bash
export CXXFLAGS="-include $(pwd)/evil.h"
python3 setup.py build
