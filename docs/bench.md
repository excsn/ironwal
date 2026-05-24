# Storage Benchmark Suite

**Test Machine:** Macbook M4 Pro

## Results

## Append Operations

### `Append_Operations/append_single_strict`

* **Time:** 10.541 ms - 10.638 ms - 10.741 ms

### `Append_Operations/append_single_async`

* **Time:** 6.3475 ms - 6.4548 ms - 6.5647 ms

### `Append_Operations/append_batch_100_strict`

* **Time:** 10.784 ms - 10.895 ms - 11.004 ms

### `Append_Operations/append_batch_100_async`

* **Time:** 6.4485 ms - 6.5493 ms - 6.6518 ms

---

## Read Operations

### `Read_Operations/read_random_get_std_io`

* **Time:** 14.054 ms - 15.932 ms - 17.840 ms

### `Read_Operations/read_random_get_mmap`

* **Time:** 236.08 µs - 245.23 µs - 255.03 µs

### `Read_Operations/read_random_get_mmap_cached`

* **Time:** 367.69 µs - 416.65 µs - 475.42 µs

### `Read_Operations/read_sequential_iter_std_io`

* **Time:** 1.1298 ms - 1.1361 ms - 1.1428 ms

### `Read_Operations/read_sequential_iter_mmap`

* **Time:** 926.83 µs - 933.70 µs - 941.30 µs

---

## Stress Benchmarks

### `Stress/CacheThrashing/write_round_robin_thrashing`

* **Time:** 532.00 ms - 535.29 ms - 538.57 ms

---

## Stress — Concurrency

### `Stress/Concurrency/1`

* **Time:** 7.2361 ms - 7.3698 ms - 7.5040 ms
* **Throughput:** 13.326 Kelem/s - 13.569 Kelem/s - 13.820 Kelem/s

### `Stress/Concurrency/4`

* **Time:** 15.557 ms - 15.911 ms - 16.259 ms
* **Throughput:** 24.602 Kelem/s - 25.139 Kelem/s - 25.711 Kelem/s

### `Stress/Concurrency/8`

* **Time:** 27.230 ms - 27.927 ms - 28.645 ms
* **Throughput:** 27.928 Kelem/s - 28.646 Kelem/s - 29.380 Kelem/s

### `Stress/Concurrency/16`

* **Time:** 49.170 ms - 50.903 ms - 52.670 ms
* **Throughput:** 30.378 Kelem/s - 31.432 Kelem/s - 32.540 Kelem/s

---

## Stress — Recovery

### `Stress/Recovery/recover_10k_entries_1k_segments`

* **Time:** 923.94 µs - 925.86 µs - 927.94 µs

---

## Sharded WAL Operations

### `ShardedWal/Append/single_async/4`

* **Time:** 3.0557 µs - 3.0906 µs - 3.1342 µs

### `ShardedWal/Append/single_async/16`

* **Time:** 3.3058 µs - 3.3519 µs - 3.4152 µs

### `ShardedWal/Append/single_async/64`

* **Time:** 4.1715 µs - 4.2643 µs - 4.3721 µs

### `ShardedWal/Batch/batch_100/4`

* **Time:** 27.362 µs - 27.501 µs - 27.663 µs
* **Throughput:** 3.6150 Melem/s - 3.6362 Melem/s - 3.6548 Melem/s

### `ShardedWal/Batch/batch_100/16`

* **Time:** 77.103 µs - 77.931 µs - 78.997 µs
* **Throughput:** 1.2659 Melem/s - 1.2832 Melem/s - 1.2970 Melem/s

### `ShardedWal/Batch/batch_100/64`

* **Time:** 269.70 µs - 275.15 µs - 281.43 µs
* **Throughput:** 355.33 Kelem/s - 363.43 Kelem/s - 370.78 Kelem/s

### `ShardedWal/Routing/core_wal_baseline`

* **Time:** 3.2050 µs - 3.2471 µs - 3.3128 µs

### `ShardedWal/Routing/sharded_16`

* **Time:** 3.2804 µs - 3.3100 µs - 3.3472 µs

### `ShardedWal/Checkpoint/Create/create/4`

* **Time:** 13.419 ms - 13.585 ms - 13.750 ms

### `ShardedWal/Checkpoint/Create/create/16`

* **Time:** 13.554 ms - 13.708 ms - 13.873 ms

### `ShardedWal/Checkpoint/Create/create/64`

* **Time:** 13.393 ms - 13.508 ms - 13.623 ms

### `ShardedWal/Checkpoint/Load/load/4`

* **Time:** 7.9481 µs - 7.9808 µs - 8.0183 µs

### `ShardedWal/Checkpoint/Load/load/16`

* **Time:** 8.6235 µs - 8.7087 µs - 8.8120 µs

### `ShardedWal/Checkpoint/Load/load/64`

* **Time:** 10.224 µs - 10.427 µs - 10.650 µs

### `ShardedWal/Checkpoint/Scale/load_with_count/10`

* **Time:** 8.1019 µs - 8.2081 µs - 8.4020 µs

### `ShardedWal/Checkpoint/Scale/load_with_count/100`

* **Time:** 8.2893 µs - 8.3479 µs - 8.4884 µs

### `ShardedWal/Checkpoint/Scale/load_with_count/500`

* **Time:** 8.2342 µs - 8.3875 µs - 8.7032 µs

### `ShardedWal/Concurrency/Writes/threads/1`

* **Time:** 6.3460 ms - 7.1171 ms - 7.8163 ms
* **Throughput:** 12.794 Kelem/s - 14.051 Kelem/s - 15.758 Kelem/s

### `ShardedWal/Concurrency/Writes/threads/4`

* **Time:** 6.9623 ms - 7.7347 ms - 8.4990 ms
* **Throughput:** 47.064 Kelem/s - 51.715 Kelem/s - 57.452 Kelem/s

### `ShardedWal/Concurrency/Writes/threads/8`

* **Time:** 9.2769 ms - 10.226 ms - 11.124 ms
* **Throughput:** 71.915 Kelem/s - 78.236 Kelem/s - 86.235 Kelem/s

### `ShardedWal/Concurrency/Writes/threads/16`

* **Time:** 10.998 ms - 11.913 ms - 12.780 ms
* **Throughput:** 125.20 Kelem/s - 134.30 Kelem/s - 145.48 Kelem/s

### `ShardedWal/Concurrency/Checkpoint/write_and_checkpoint`

* **Time:** 146.03 ms - 147.71 ms - 149.50 ms