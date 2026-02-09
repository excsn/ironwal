# Storage Benchmark Suite

**Test Machine:** Macbook M4 Pro

## Results

## Append Operations

### `Append_Operations/append_single_strict`

* **Time:** 9.2916 ms - 9.3838 ms - 9.4737 ms

### `Append_Operations/append_single_async`

* **Time:** 5.7344 ms - 5.8385 ms - 5.9435 ms

### `Append_Operations/append_batch_100_strict`

* **Time:** 9.6809 ms - 9.8025 ms - 9.9255 ms

### `Append_Operations/append_batch_100_async`

* **Time:** 5.5162 ms - 5.6068 ms - 5.7006 ms

---

## Read Operations

### `Read_Operations/read_random_get_std_io`

* **Time:** 18.455 ms - 20.849 ms - 23.289 ms

### `Read_Operations/read_random_get_mmap`

* **Time:** 457.40 µs - 472.04 µs - 487.03 µs

### `Read_Operations/read_random_get_mmap_cached`

* **Time:** 306.53 µs - 393.77 µs - 526.52 µs

### `Read_Operations/read_sequential_iter_std_io`

* **Time:** 1.3844 ms - 1.4165 ms - 1.4548 ms

### `Read_Operations/read_sequential_iter_mmap`

* **Time:** 1.0685 ms - 1.0769 ms - 1.0859 ms

---

## Stress Benchmarks

### `Stress/CacheThrashing/write_round_robin_thrashing`

* **Time:** 572.84 ms - 577.56 ms - 582.23 ms

---

## Stress — Concurrency

### `Stress/Concurrency/1`

* **Time:** 7.4704 ms - 7.6078 ms - 7.7438 ms
* **Throughput:** 12.913 Kelem/s - 13.144 Kelem/s - 13.386 Kelem/s

### `Stress/Concurrency/4`

* **Time:** 16.943 ms - 17.418 ms - 17.897 ms
* **Throughput:** 22.351 Kelem/s - 22.965 Kelem/s - 23.608 Kelem/s

### `Stress/Concurrency/8`

* **Time:** 28.291 ms - 28.964 ms - 29.632 ms
* **Throughput:** 26.998 Kelem/s - 27.621 Kelem/s - 28.278 Kelem/s

### `Stress/Concurrency/16`

* **Time:** 53.700 ms - 55.451 ms - 57.228 ms
* **Throughput:** 27.958 Kelem/s - 28.854 Kelem/s - 29.795 Kelem/s

---

## Stress — Recovery

### `Stress/Recovery/recover_10k_entries_1k_segments`

* **Time:** 710.93 µs - 712.50 µs - 714.21 µs