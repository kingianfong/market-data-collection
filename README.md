# Market Data Collection

High-performance cryptocurrency market data capture system for systematic trading research, designed for tick-level precision and data integrity.

## Motivation

Infrastructure for collecting real-time tick data to support microstructure research and strategy development while avoiding commercial data vendor costs. Focuses on capture completeness and sub-millisecond processing latency.

## System Architecture

**WebSocket Layer**
- Asynchronous I/O using Boost.Cobalt coroutines over Boost.Beast
- Non-blocking message reception and parsing
- Exchange-specific protocol handling (Binance WebSocket API)

**Lock-Free SPSC Queue**
- Cache-optimized circular buffer for variable-length messages
- Zero runtime heap allocations via fixed-size backing store
- Acquire/Release memory ordering ensures correctness without CAS overhead
- Verified thread-safety with ThreadSanitizer

**Writer Thread**
- Dedicated persistence with batched disk writes
- Minimizes syscall overhead

## Performance

**Hardware:** Apple M1 Max (10-core: 8P+2E)

**Queue Throughput Benchmark:**
- **43.7 million msg/sec** (7.3 GB/s sustained)
- Per-message latency: ~23 ns (amortized)
- Test configuration: 10M messages × 180 bytes, 16 MB queue capacity

**Note:** Benchmark measures queue-only throughput in isolation. End-to-end system latency includes WebSocket reception (~100μs) and batched disk persistence (~1ms).

## Build

Requires C++23 compiler and vcpkg:
```bash
git clone --recursive https://github.com/kingianfong/market-data-collection.git
cd market-data-collection
cmake --preset=release
cmake --build --preset=release
```

## Continuous Integration

Automated CI via GitHub Actions validates both debug and release builds on each commit.

## Future Work

- Add data validation metrics (sequence gap detection, timestamp monotonicity)
- Implement orderbook reconstruction from L2 deltas
- Extend to additional exchanges and asset classes
