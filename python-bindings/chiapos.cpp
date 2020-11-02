#ifndef PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_
#define PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_

#if __has_include(<optional>)

#include <optional>
namespace stdx {
using std::optional;
}

#elif __has_include(<experimental/optional>)

#include <experimental/optional>
namespace stdx {
using std::experimental::optional;
}

#else
#error "an implementation of optional is required!"
#endif

#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../src/plotter_disk.hpp"
#include "../src/prover_disk.hpp"
#include "../src/verifier.hpp"

namespace py = pybind11;

PYBIND11_MODULE(chiapos, m)
{
    m.doc() = "Chia Proof of Space";

    py::class_<DiskPlotter>(m, "DiskPlotter")
        .def(py::init<>())
        .def(
            "create_plot_disk",
            [](DiskPlotter &dp,
               const std::string tmp_dir,
               const std::string tmp2_dir,
               const std::string final_dir,
               const std::string filename,
               uint8_t k,
               const py::bytes &memo,
               const py::bytes &id,
               uint32_t buffmegabytes,
               uint32_t num_buckets,
               uint32_t stripe_size,
               uint8_t num_threads) {
                std::string memo_str(memo);
                const uint8_t *memo_ptr = reinterpret_cast<const uint8_t *>(memo_str.data());
                std::string id_str(id);
                const uint8_t *id_ptr = reinterpret_cast<const uint8_t *>(id_str.data());
            });

    py::class_<DiskProver>(m, "DiskProver")
        .def(py::init<const std::string &>())
        .def(
            "get_memo",
            [](DiskProver &dp) {
                uint8_t *memo = new uint8_t[10];
                py::bytes ret = py::bytes(reinterpret_cast<char *>(memo), 10);
                delete[] memo;
                return ret;
            })

    py::class_<Verifier>(m, "Verifier")
        .def(py::init<>())
        .def(
            "validate_proof",
            [](Verifier &v,
               const py::bytes &seed,
               uint8_t k,
               const py::bytes &challenge,
               const py::bytes &proof) {
                uint8_t *quality_buf = new uint8_t[32];
                py::bytes quality_py = py::bytes(reinterpret_cast<char *>(quality_buf), 32);
                delete[] quality_buf;
                return stdx::optional<py::bytes>(quality_py);
            });
}

#endif  // PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_
