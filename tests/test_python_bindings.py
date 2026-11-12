import unittest
from chiapos import DiskProver, DiskPlotter, Verifier
from hashlib import sha256
from pathlib import Path
from secrets import token_bytes


class TestPythonBindings(unittest.TestCase):
    def test_k_21(self):
        plot_seed: bytes = bytes(
            [
                5,
                104,
                52,
                4,
                51,
                55,
                23,
                84,
                91,
                10,
                111,
                12,
                13,
                222,
                151,
                16,
                228,
                211,
                254,
                45,
                92,
                198,
                204,
                10,
                9,
                10,
                11,
                129,
                139,
                171,
                15,
                23,
            ]
        )

        pl = DiskPlotter()
        pl.create_plot_disk(
            ".",
            ".",
            ".",
            "myplot.dat",
            21,
            bytes([1, 2, 3, 4, 5]),
            plot_seed,
            300,
            32,
            8192,
            8,
            False,
        )
        pl = None

        pr = DiskProver(str(Path("myplot.dat")))

        total_proofs: int = 0
        total_proofs2: int = 0
        iterations: int = 5000

        v = Verifier()
        for i in range(iterations):
            if i % 100 == 0:
                print(i)
            challenge = sha256(i.to_bytes(4, "big")).digest()
            for index, quality in enumerate(pr.get_qualities_for_challenge(challenge)):
                proof = pr.get_full_proof(challenge, index)
                assert len(proof) == 8 * pr.get_size()
                computed_quality = v.validate_proof(
                    plot_seed, pr.get_size(), challenge, proof
                )
                assert computed_quality == quality
                total_proofs += 1
            for index, quality in enumerate(pr.get_qualities_for_challenge(challenge)):
                proof = pr.get_full_proof(challenge, index, parallel_read=False)
                assert len(proof) == 8 * pr.get_size()
                computed_quality = v.validate_proof(
                    plot_seed, pr.get_size(), challenge, proof
                )
                assert computed_quality == quality
                total_proofs2 += 1

        print(
            f"total proofs {total_proofs} out of {iterations}\
            {total_proofs / iterations}"
        )
        print(
            f"total proofs (sequential reads) {total_proofs2} out of {iterations}\
            {total_proofs2 / iterations}"
        )

        assert total_proofs2 == total_proofs
        assert total_proofs > 4000
        assert total_proofs < 6000
        pr = None
        sha256_plot_hash = sha256()
        with open("myplot.dat", "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_plot_hash.update(byte_block)
            plot_hash = str(sha256_plot_hash.hexdigest())
        assert (
            plot_hash
            == "80e32f560f3a4347760d6baae8d16fbaf484948088bff05c51bdcc24b7bc40d9"
        )
        print(f"\nPlotfile asserted sha256: {plot_hash}\n")

    def test_faulty_plot_doesnt_crash(self):
        if Path("myplot.dat").exists():
            Path("myplot.dat").unlink()
        if Path("myplotbad.dat").exists():
            Path("myplotbad.dat").unlink()

        plot_id: bytes = bytes([i for i in range(32, 64)])
        pl = DiskPlotter()
        pl.create_plot_disk(
            ".",
            ".",
            ".",
            "myplot.dat",
            21,
            bytes([1, 2, 3, 4, 5]),
            plot_id,
            300,
            32,
            8192,
            8,
            False,
        )
        f = open("myplot.dat", "rb")
        all_data = bytearray(f.read())
        f.close()
        assert len(all_data) > 20000000
        all_data_bad = (
            all_data[:20000000] + bytearray(token_bytes(10000)) + all_data[20100000:]
        )
        f_bad = open("myplotbad.dat", "wb")
        f_bad.write(all_data_bad)
        f_bad.close()

        pr = DiskProver(str(Path("myplotbad.dat")))

        iterations: int = 50000
        v = Verifier()
        successes = 0
        failures = 0
        for i in range(iterations):
            if i % 100 == 0:
                print(i)
            challenge = sha256(i.to_bytes(4, "big")).digest()
            try:
                for index, quality in enumerate(
                    pr.get_qualities_for_challenge(challenge)
                ):
                    proof = pr.get_full_proof(challenge, index)
                    computed_quality = v.validate_proof(
                        plot_id, pr.get_size(), challenge, proof
                    )
                    if computed_quality == quality:
                        successes += 1
                    else:
                        print("Did not validate")
                        failures += 1
            except Exception as e:
                print(f"Exception: {e}")
                failures += 1
        print(f"Successes: {successes}")
        print(f"Failures: {failures}")

    def test_disk_prover_byte_conversions(self):
        plot_path = Path("byte_conversion_plot.dat")
        if plot_path.exists():
            plot_path.unlink()
        challenge = sha256(b"\x00").digest()
        pl = DiskPlotter()
        pl.create_plot_disk(
            ".", ".", ".", str(plot_path), 21, bytes([1, 2, 3, 4, 5]), bytes(b'\0' * 32), 300, 32, 8192, 8, False
        )
        pr = DiskProver(str(plot_path))
        serialized = bytes(pr)
        pr_recovered = DiskProver.from_bytes(serialized)
        assert bytes(pr_recovered) == serialized
        assert pr.get_memo() == pr_recovered.get_memo()
        assert pr.get_id() == pr_recovered.get_id()
        assert pr.get_size() == pr_recovered.get_size()
        assert pr.get_filename() == pr_recovered.get_filename()
        assert pr.get_size() == pr_recovered.get_size()
        assert pr.get_qualities_for_challenge(challenge) == pr_recovered.get_qualities_for_challenge(challenge)
        with self.assertRaises(ValueError):
            DiskProver.from_bytes(bytes())
        with self.assertRaises(ValueError):
            DiskProver.from_bytes(serialized[0:int(len(serialized)/2)])

    def test_validate_proof_rejects_wrong_length_inputs(self):
        v = Verifier()
        good = bytes(32)
        proof = bytes(21 * 8)
        # Well-formed input is handled without raising (the proof is simply invalid).
        v.validate_proof(good, 21, good, proof)
        for bad in (b"", bytes(4), bytes(31), bytes(33)):
            with self.assertRaises(ValueError):
                v.validate_proof(bad, 21, good, proof)
            with self.assertRaises(ValueError):
                v.validate_proof(good, 21, bad, proof)

    def test_disk_prover_rejects_k_out_of_range_in_header(self):
        plot_path = Path("k_range_plot.dat")
        bad_path = Path("k_range_plot_bad.dat")
        for p in (plot_path, bad_path):
            if p.exists():
                p.unlink()
        pl = DiskPlotter()
        pl.create_plot_disk(
            ".", ".", ".", str(plot_path), 21, bytes([1, 2, 3, 4, 5]), bytes(b"\0" * 32), 300, 32, 8192, 8, False
        )
        data = bytearray(plot_path.read_bytes())
        # Header: 19 bytes magic, 32 bytes plot id, then the 1-byte k.
        k_offset = 19 + 32
        assert data[k_offset] == 21
        # A valid header still loads.
        DiskProver(str(plot_path))
        # k outside [18, 50] must be rejected with an exception, not crash the process
        # (k == 0 used to cause a division by zero in the constructor).
        for bad_k in (0, 1, 17, 51, 255):
            data[k_offset] = bad_k
            bad_path.write_bytes(bytes(data))
            with self.assertRaises(ValueError):
                DiskProver(str(bad_path))
        for p in (plot_path, bad_path):
            p.unlink()


if __name__ == "__main__":
    unittest.main()
