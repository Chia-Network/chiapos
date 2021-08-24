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
               uint8_t num_threads,
               bool nobitfield) {
                std::string memo_str(memo);
                const uint8_t *memo_ptr = reinterpret_cast<const uint8_t *>(memo_str.data());
                std::string id_str(id);
                const uint8_t *id_ptr = reinterpret_cast<const uint8_t *>(id_str.data());
                try {
                    dp.CreatePlotDisk(tmp_dir,
                                      tmp2_dir,
                                      final_dir,
                                      filename,
                                      k,
                                      memo_ptr,
                                      len(memo),
                                      id_ptr,
                                      len(id),
                                      buffmegabytes,
                                      num_buckets,
                                      stripe_size,
                                      num_threads,
                                      nobitfield ? 0 : ENABLE_BITFIELD);
                } catch (const std::exception &e) {
                    std::cout << "Caught plotting error: " << e.what() << std::endl;
                    throw e;
                }
            });

    py::class_<DiskProver>(m, "DiskProver")
        .def(py::init<const std::string &>())
        .def("is_valid", [](const DiskProver &dp) { return dp.IsValid(); })
        .def(py::pickle(
            [](const DiskProver& dp) { // __getstate__
                return py::make_tuple(dp.GetFilename(),
                                      dp.GetSize(),
                                      dp.GetMemo(),
                                      dp.GetId(),
                                      dp.GetTableBeginPointers(),
                                      dp.GetC2());
                },
            [](const py::tuple& t) { // __setstate__
                if (t.size() != 6)
                    throw std::runtime_error("Invalid state!");

                auto filename = t[0].cast<std::string>();
                auto k = t[1].cast<uint8_t>();
                auto memo = t[2].cast<std::vector<uint8_t>>();
                auto id = t[3].cast<std::vector<uint8_t>>();
                auto table_begin_pointers = t[4].cast<std::vector<uint64_t>>();
                auto C2 = t[5].cast<std::vector<uint64_t>>();
                return DiskProver(filename, memo, id, k,
                                  table_begin_pointers, C2);
            }))
        .def(
            "get_memo",
            [](DiskProver &dp) {
                const std::vector<uint8_t>& memo = dp.GetMemo();
                return py::bytes(reinterpret_cast<const char*>(memo.data()), memo.size());
            })
        .def(
            "get_id",
            [](DiskProver &dp) {
                const std::vector<uint8_t>& id = dp.GetId();
                return py::bytes(reinterpret_cast<const char*>(id.data()), id.size());
            })
        .def("get_size", [](DiskProver &dp) { return dp.GetSize(); })
        .def("get_filename", [](DiskProver &dp) { return dp.GetFilename(); })
        .def(
            "get_qualities_for_challenge",
            [](DiskProver &dp, const py::bytes &challenge) {
                if (len(challenge) != 32) {
                    throw std::invalid_argument("Challenge must be exactly 32 bytes");
                }
                std::string challenge_str(challenge);
                const uint8_t *challenge_ptr =
                    reinterpret_cast<const uint8_t *>(challenge_str.data());
                py::gil_scoped_release release;
                std::vector<LargeBits> qualities = dp.GetQualitiesForChallenge(challenge_ptr);
                py::gil_scoped_acquire acquire;
                std::vector<py::bytes> ret;
                uint8_t *quality_buf = new uint8_t[32];
                for (LargeBits quality : qualities) {
                    quality.ToBytes(quality_buf);
                    py::bytes quality_py = py::bytes(reinterpret_cast<char *>(quality_buf), 32);
                    ret.push_back(quality_py);
                }
                delete[] quality_buf;
                return ret;
            })
        .def("get_full_proof", [](DiskProver &dp, const py::bytes &challenge, uint32_t index, bool parallel_read) {
            std::string challenge_str(challenge);
            const uint8_t *challenge_ptr = reinterpret_cast<const uint8_t *>(challenge_str.data());
            py::gil_scoped_release release;
            LargeBits proof = dp.GetFullProof(challenge_ptr, index, parallel_read);
            py::gil_scoped_acquire acquire;
            uint8_t *proof_buf = new uint8_t[Util::ByteAlign(64 * dp.GetSize()) / 8];
            proof.ToBytes(proof_buf);
            py::bytes ret = py::bytes(
                reinterpret_cast<char *>(proof_buf), Util::ByteAlign(64 * dp.GetSize()) / 8);
            delete[] proof_buf;
            return ret;
        },py::arg("challenge"), py::arg("index"), py::arg("parallel_read") = true);

    py::class_<Verifier>(m, "Verifier")
        .def(py::init<>())
        .def(
            "validate_proof",
            [](Verifier &v,
               const py::bytes &seed,
               uint8_t k,
               const py::bytes &challenge,
               const py::bytes &proof) {
                std::string seed_str(seed);
                const uint8_t *seed_ptr = reinterpret_cast<const uint8_t *>(seed_str.data());

                std::string challenge_str(challenge);
                const uint8_t *challenge_ptr =
                    reinterpret_cast<const uint8_t *>(challenge_str.data());

                std::string proof_str(proof);
                const uint8_t *proof_ptr = reinterpret_cast<const uint8_t *>(proof_str.data());

                LargeBits quality =
                    v.ValidateProof(seed_ptr, k, challenge_ptr, proof_ptr, len(proof));
                if (quality.GetSize() == 0) {
                    return stdx::optional<py::bytes>();
                }
                uint8_t *quality_buf = new uint8_t[32];
                quality.ToBytes(quality_buf);
                py::bytes quality_py = py::bytes(reinterpret_cast<char *>(quality_buf), 32);
                delete[] quality_buf;
                return stdx::optional<py::bytes>(quality_py);
            });
}

#endif  // PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_
