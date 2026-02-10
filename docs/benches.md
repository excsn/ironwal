# IronWal Benchmark Guide

## Running Benchmarks
```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench sharded_append

# Run with features
cargo bench --features sharded
```

## Benchmark Methodology

### Warmup Strategy

All sharded benchmarks use a warmup phase to:
1. Create all shard directories
2. Initialize file handles
3. Avoid one-time costs in measurements

### WAL Instance Reuse

Benchmarks reuse WAL instances across iterations to measure steady-state performance,
not initialization overhead. This reflects real-world usage where a WAL is created
once and used for many operations.

## Tuning for Benchmarks

### For High Shard Counts

If benchmarking with >32 shards:
```rust
let mut opts = WalOptions::default();
opts.max_open_segments = shard_count * 2; // Prevent thrashing
```

### For Read-Heavy Workloads
```rust
opts.read_strategy = ReadStrategy::Mmap;
opts.block_cache_size = Some(100 * 1024 * 1024); // 100MB
```

### For Write-Heavy Workloads
```rust
opts.sync_mode = SyncMode::BatchOnly;
opts.write_buffer_size = 256 * 1024; // 256KB
```