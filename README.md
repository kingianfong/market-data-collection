# Market Data Collection

This project is an exploration of modern C++23, focusing on low-latency design in the context of market data collection.

## Highlights

*   **Asynchronous C++20 Coroutines**: Using Boost.Cobalt coroutines for non-blocking asynchronous control flow on top of Boost.Beast WebSocket communication.
*   **Zero-allocation SPSC Queue**: The `SpscBuffers` buffers variable-length messages between the network and writer threads without runtime heap allocations. Uses **Acquire/Release memory semantics** to guarantee ordering while minimising producer latency.
*   **Continuous Integration**: Automated CI via GitHub Actions for robust builds and testing for debug and release configurations.
