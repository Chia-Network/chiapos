#!/bin/sh -x

# Stop in case of error
set -e

#python -m pip install cibuildwheel==1.3.0


# build just python 3.7
export CIBW_BUILD="cp37-win_amd64"
#export CIBW_BEFORE_BUILD_WINDOWS="python -m pip install --upgrade pip"
export CIBW_TEST_REQUIRES="pytest"
export CIBW_TEST_COMMAND="py.test -v {project}/tests"


export PATH=$PATH:"C:\msys64\mingw64\bin\cmake.exe"
echo "PWD is $PWD"

cmake -G "MSYS Makefiles" --build .
make
$PWD/RunTests.exe
#Test failing for windows:
#py.test -v $PWD/tests/
#pip -vv wheel .
