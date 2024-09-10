#!/bin/bash
set -e

mkdir -p thirdparty
cd thirdparty

rm -rf pybind11 cxxopts filesystem BLAKE3 Catch2

# Make sure to also update CMakeLists.txt with these hashes.

git clone https://github.com/pybind/pybind11.git -b v2.11.1
cd pybind11
git checkout 8a099e44b3d5f85b20f05828d919d2332a8de841
cd ..

git clone https://github.com/jarro2783/cxxopts.git -b v3.1.1
cd cxxopts
git checkout eb787304d67ec22f7c3a184ee8b4c481d04357fd
cd ..

git clone https://github.com/gulrak/filesystem.git -b v1.5.14
cd filesystem
git checkout 8a2edd6d92ed820521d42c94d179462bf06b5ed3
cd ..

git clone https://github.com/BLAKE3-team/BLAKE3.git -b 1.5.0
cd BLAKE3
git checkout 5aa53f07f7188a569cadfc5daf1522972d9a9630
cd ..

git clone https://github.com/catchorg/Catch2.git -b v3.5.2
cd Catch2
git checkout 05e10dfccc28c7f973727c54f850237d07d5e10f
cd ..

cd ..
