# Market Data Collection

Market data capture system optimizing for zero-allocation coroutine execution and sub-millisecond processing latency.

## Motivation

Infrastructure for collecting real-time tick data to support microstructure research and strategy development while avoiding commercial data vendor costs. Focuses on capture completeness and minimizing allocation overhead in the hot path.

## System Architecture

**Memory Management Strategy**
- Custom `std::pmr` allocator hierarchy eliminates dynamic allocations for coroutine frames
- Monotonic buffer resource pre-allocated at startup
- Unsynchronized pool resource layered on top for efficient reuse
- Logging wrappers at each tier detect unexpected allocations during development
- Coroutine frames allocated from PMR pool via `cobalt::this_thread::set_default_resource()`

**WebSocket Layer**
- Asynchronous I/O using Boost.Cobalt coroutines over Boost.Beast
- Non-blocking message reception with pre-reserved buffers
- All coroutine state (local variables, suspend points) backed by PMR allocator
- Exchange-specific endpoint configuration (Binance WebSocket API)

**Lock-Free SPSC Queue**
- Cache-optimized circular buffer for variable-length messages
- Zero runtime heap allocations via fixed-size backing store
- Acquire/Release memory ordering ensures correctness without CAS overhead
- Verified thread-safety with ThreadSanitizer

**Writer Thread**
- Dedicated persistence with batched disk writes
- Minimizes syscall overhead

## PMR Implementation Details

The coroutine allocation strategy uses a three-tier memory resource hierarchy defined in `PmrContext`:

1. **Base Layer**: `LoggingMemoryResource` wrapping `std::pmr::new_delete_resource()` - catches unexpected fallback to heap (logs at ERROR level)
2. **Middle Layer**: `std::pmr::monotonic_buffer_resource` with 32 MB pre-allocated buffer - provides fast bump-pointer allocation
3. **Top Layer**: `std::pmr::unsynchronized_pool_resource` - enables efficient reuse of fixed-size blocks for coroutine frames

In `collect_ws.cc`, the PMR context is set as the default allocator for the Cobalt executor thread:
```cpp
cobalt::this_thread::set_default_resource(run_ctx.pmr_ctx->memory_resource());
```

This ensures all coroutine frames spawned via `cobalt::spawn()` allocate from the PMR pool rather than the global heap. The `RunWebsocket` coroutine's frame (local variables + suspend state) is allocated once from the pool when spawned and reused across all suspension points, avoiding per-message allocation overhead.

The logging wrappers provide allocation visibility during development:
- DEBUG: Pool allocations (expected for coroutine frames)
- WARN: Monotonic buffer allocations (shouldn't occur after warmup)
- ERROR: Heap allocations (indicates PMR capacity exhaustion)

## Performance

**Hardware:** Apple M1 Max (10-core: 8P+2E)

**Queue Throughput Benchmark:**
- **43.7 million msg/sec** (7.3 GB/s sustained)
- Per-message latency: ~23 ns (amortized)
- Test configuration: 10M messages × 180 bytes, 16 MB queue capacity

**Note:** Benchmark measures queue-only throughput in isolation. End-to-end system latency includes WebSocket reception and disk persistence.

## Build

Requires C++23 compiler and vcpkg:
```bash
git clone --recursive https://github.com/kingianfong/market-data-collection.git
cd market-data-collection
cmake --preset release
cmake --build --preset release
```

## Continuous Integration

Automated CI via GitHub Actions validates both debug and release builds on each commit.

## Future Work

- Add data validation metrics (sequence gap detection, timestamp monotonicity)
- Implement orderbook reconstruction from L2 deltas
- Extend to additional exchanges and asset classes