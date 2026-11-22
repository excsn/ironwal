# Storage Benchmark Suite

**Test Machine:** Macbook M4 Pro

## Results

## Append Operations

### `Append_Operations/append_single_strict`

* **Time:** 10.564 ms - 10.662 ms - 10.761 ms

### `Append_Operations/append_single_async`

* **Time:** 963.76 µs - 973.86 µs - 988.37 µs

### `Append_Operations/append_batch_100_strict`

* **Time:** 10.853 ms - 10.930 ms - 11.008 ms

### `Append_Operations/append_batch_100_async`

* **Time:** 988.15 µs - 994.32 µs - 999.69 µs

---

## Read Operations

### `Read_Operations/read_random_get_std_io`

* **Time:** 302.69 µs - 319.15 µs - 335.81 µs

### `Read_Operations/read_random_get_mmap`

* **Time:** 212.60 µs - 220.60 µs - 229.64 µs

### `Read_Operations/read_random_get_mmap_cached`

* **Time:** 321.32 µs - 330.72 µs - 340.61 µs

### `Read_Operations/read_sequential_iter_std_io`

* **Time:** 1.1812 ms - 1.1899 ms - 1.1994 ms

### `Read_Operations/read_sequential_iter_mmap`

* **Time:** 913.91 µs - 921.20 µs - 929.05 µs

---

## Stress Benchmarks

### `Stress/CacheThrashing/write_round_robin_thrashing`

* **Time:** 550.35 ms - 554.49 ms - 558.44 ms

---

## Stress — Concurrency

### `Stress/Concurrency/1`

* **Time:** 7.8261 ms - 8.5601 ms - 9.4336 ms
* **Throughput:** 10.600 Kelem/s - 11.682 Kelem/s - 12.778 Kelem/s

### `Stress/Concurrency/4`

* **Time:** 13.202 ms - 13.667 ms - 14.127 ms
* **Throughput:** 28.314 Kelem/s - 29.267 Kelem/s - 30.298 Kelem/s

### `Stress/Concurrency/8`

* **Time:** 22.024 ms - 22.452 ms - 22.883 ms
* **Throughput:** 34.960 Kelem/s - 35.632 Kelem/s - 36.325 Kelem/s

### `Stress/Concurrency/16`

* **Time:** 32.650 ms - 33.330 ms - 34.022 ms
* **Throughput:** 47.028 Kelem/s - 48.005 Kelem/s - 49.005 Kelem/s

---

## Stress — Recovery

### `Stress/Recovery/recover_10k_entries_1k_segments`

* **Time:** 1.1220 ms - 1.1276 ms - 1.1341 ms

---

## Sharded WAL Operations

### `ShardedWal/Append/single_async/4`

* **Time:** 3.0718 µs - 3.1042 µs - 3.1497 µs

### `ShardedWal/Append/single_async/16`

* **Time:** 3.1823 µs - 3.2107 µs - 3.2411 µs

### `ShardedWal/Append/single_async/64`

* **Time:** 3.3203 µs - 3.3782 µs - 3.4448 µs

### `ShardedWal/Batch/batch_100/4`

* **Time:** 35.454 µs - 35.845 µs - 36.391 µs
* **Throughput:** 2.7479 Melem/s - 2.7898 Melem/s - 2.8206 Melem/s

### `ShardedWal/Batch/batch_100/16`

* **Time:** 100.72 µs - 101.99 µs - 103.30 µs
* **Throughput:** 968.05 Kelem/s - 980.45 Kelem/s - 992.83 Kelem/s

### `ShardedWal/Batch/batch_100/64`

* **Time:** 282.51 µs - 287.65 µs - 293.91 µs
* **Throughput:** 340.24 Kelem/s - 347.64 Kelem/s - 353.97 Kelem/s

### `ShardedWal/Batch/batch_100_buffered/4`

* **Time:** 17.270 µs - 17.612 µs - 18.032 µs
* **Throughput:** 5.5457 Melem/s - 5.6781 Melem/s - 5.7905 Melem/s

### `ShardedWal/Batch/batch_100_buffered/16`

* **Time:** 38.422 µs - 38.908 µs - 39.556 µs
* **Throughput:** 2.5281 Melem/s - 2.5701 Melem/s - 2.6027 Melem/s

### `ShardedWal/Batch/batch_100_buffered/64`

* **Time:** 108.81 µs - 121.23 µs - 136.42 µs
* **Throughput:** 733.00 Kelem/s - 824.90 Kelem/s - 919.00 Kelem/s

### `ShardedWal/Routing/core_wal_baseline`

* **Time:** 3.0495 µs - 3.0889 µs - 3.1368 µs

### `ShardedWal/Routing/sharded_16`

* **Time:** 3.1667 µs - 3.1929 µs - 3.2220 µs

### `ShardedWal/Checkpoint/Create/create/4`

* **Time:** 13.627 ms - 13.773 ms - 13.921 ms

### `ShardedWal/Checkpoint/Create/create/16`

* **Time:** 13.553 ms - 13.690 ms - 13.836 ms

### `ShardedWal/Checkpoint/Create/create/64`

* **Time:** 13.673 ms - 13.800 ms - 13.927 ms

### `ShardedWal/Checkpoint/Load/load/4`

* **Time:** 9.0546 µs - 9.1043 µs - 9.1629 µs

### `ShardedWal/Checkpoint/Load/load/16`

* **Time:** 9.7539 µs - 9.8382 µs - 9.9410 µs

### `ShardedWal/Checkpoint/Load/load/64`

* **Time:** 10.909 µs - 11.166 µs - 11.448 µs

### `ShardedWal/Checkpoint/Scale/load_with_count/10`

* **Time:** 9.2478 µs - 9.3177 µs - 9.4060 µs

### `ShardedWal/Checkpoint/Scale/load_with_count/100`

* **Time:** 9.2445 µs - 9.3232 µs - 9.4846 µs

### `ShardedWal/Checkpoint/Scale/load_with_count/500`

* **Time:** 9.2347 µs - 9.5416 µs - 10.183 µs

### `ShardedWal/Concurrency/Writes/threads/1`

* **Time:** 6.8026 ms - 7.3672 ms - 7.8474 ms
* **Throughput:** 12.743 Kelem/s - 13.574 Kelem/s - 14.700 Kelem/s

### `ShardedWal/Concurrency/Writes/threads/4`

* **Time:** 8.1274 ms - 8.5700 ms - 9.0042 ms
* **Throughput:** 44.424 Kelem/s - 46.674 Kelem/s - 49.216 Kelem/s

### `ShardedWal/Concurrency/Writes/threads/8`

* **Time:** 9.5532 ms - 10.051 ms - 10.576 ms
* **Throughput:** 75.643 Kelem/s - 79.595 Kelem/s - 83.742 Kelem/s

### `ShardedWal/Concurrency/Writes/threads/16`

* **Time:** 10.902 ms - 11.492 ms - 12.130 ms
* **Throughput:** 131.90 Kelem/s - 139.23 Kelem/s - 146.76 Kelem/s

### `ShardedWal/Concurrency/Checkpoint/write_and_checkpoint`

* **Time:** 148.30 ms - 149.83 ms - 151.51 ms
