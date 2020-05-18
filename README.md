![Build](https://github.com/Chia-Network/chiapos/workflows/Build/badge.svg)
![PyPI](https://img.shields.io/pypi/v/chiapos?logo=pypi)
![PyPI - Format](https://img.shields.io/pypi/format/chiapos?logo=pypi)
![GitHub](https://img.shields.io/github/license/Chia-Network/chiapos?logo=Github)

[![Total alerts](https://img.shields.io/lgtm/alerts/g/Chia-Network/chiapos.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/Chia-Network/chiapos/alerts/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/Chia-Network/chiapos.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/Chia-Network/chiapos/context:python)
[![Language grade: C/C++](https://img.shields.io/lgtm/grade/cpp/g/Chia-Network/chiapos.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/Chia-Network/chiapos/context:cpp)
# Chia Proof of Space

Chia's proof of space, written in C++. Includes a plotter, prover, and verifier.
Only runs on 64 bit architectures with AES-NI support. Read the
[Proof of Space document](https://www.chia.net/assets/proof_of_space.pdf) to
learn about what proof of space is and how it works.

Expect significant changes by June 30, 2020 that will break the plot file
format, move to chacha8, and otherwise improve plotting performance.

## C++ Usage Instructions

### Compile

```bash
git submodule update --init --recursive

mkdir -p build && cd build
cmake ../
cmake --build . -- -j 6
```

### Run tests

```bash
./RunTests
```

### CLI usage

```bash
./ProofOfSpace -k 25 -f "plot.dat" -m "0x1234" generate
./ProofOfSpace -k 25 -f "final-plot.dat" -m "0x4567" -t TMPDIR -2 SECOND_TMPDIR generate
./ProofOfSpace -f "plot.dat" prove <32 byte hex challenge>
./ProofOfSpace -k 25 verify <hex proof> <32 byte hex challenge>
./ProofOfSpace -f "plot.dat" check <iterations>
```

### Benchmark

```bash
time ./ProofOfSpace -k 25 generate
```


### Hellman Attacks usage

There is an experimental implementation which implements some of the Hellman
Attacks that can provide significant space savings for the final file.


```bash
./HellmanAttacks -k 18 -f "plot.dat" -m "0x1234" generate
./HellmanAttacks -f "plot.dat" check <iterations>
```

## Python

Finally, python bindings are provided in the python-bindings directory.

### Install

```bash
git submodule update --init --recursive

python3 -m venv .venv
. .venv/bin/activate
pip3 install .
```

### Run python tests

Testings uses pytest. Linting uses flake8 and mypy.

```bash
py.test ./tests -s -v
```

## ci Building
The primary build process for this repository is to use GitHub Actions to
build binary wheels for MacOS, Linux, and Windows and publish them with
a source wheel on PyPi. See `.github/workflows/build.yml`. setup.py adds
a dependency on [pybind11](https://github.com/pybind/pybind11) by invoking git
to check out the pybind submodules. Building is then managed by
[cibuildwheel](https://github.com/joerick/cibuildwheel). Further installation
is then available via `pip install chiapos` e.g.

## Contributing and workflow
Contributions are welcome and more details are available in chia-blockchain's
[CONTRIBUTING.md](https://github.com/Chia-Network/chia-blockchain/blob/master/CONTRIBUTING.md).

The master branch is the currently released latest version on PyPI. Note that
at times chiapos will be ahead of the release version that chia-blockchain
requires in it's master/release version in preparation for a new chia-blockchain
release. Please branch or fork master and then create a pull request to the
master branch. Linear merging is enforced on master and merging requires a
completed review. PRs will kick off a ci build and analysis of chiapos at
[lgtm.com](https://lgtm.com/projects/g/Chia-Network/chiapos/?mode=list). Please
make sure your build is passing and that it does not increase alerts at lgtm.
