#!/bin/sh -x

# Stop in case of error
set -e

export CMAKE_GENERATOR='MSYS Makefiles'

#export PATH=$PATH:"C:\msys64\mingw64\bin"
echo "PATH is $PATH"

echo "PWD is $PWD"

#python3 -m venv venv
#. venv/bin/activate

#pip wheel . -G "MSYS Makefiles"
cmake -G "MSYS Makefiles" --build .
make

echo "Running RunTests.exe"
$PWD/RunTests.exe

echo "Not Running py.test -v tests/"
#Test failing for windows:
#py.test -v $PWD/tests/

echo "Done because we can't build wheels yet."
#pip -vv wheel .
