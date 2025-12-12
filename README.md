# Market Data Collection

This project is a C++23 application for collecting market data. It leverages modern C++ features to provide efficient and reliable data acquisition.

## Highlights

*   **Modern C++**: Developed with C++23 for contemporary language features.
*   **Dependency Management**: Employs vcpkg to manage third-party libraries, ensuring reproducible builds.
*   **Asynchronous Networking**: Built on Boost.Beast for efficient WebSocket communication and Boost.Cobalt for asynchronous operations using C++20 coroutines.
*   **Data Serialization**: Uses Glaze for fast JSON parsing and serialization of market data messages.
*   **Logging**: Integrates Spdlog for fast, flexible, and feature-rich logging.
*   **Testing**: Includes unit tests with Catch2 to ensure code correctness and reliability.
*   **Continuous Integration**: Leverages GitHub Actions for automated builds and tests across debug and release configurations, ensuring code quality and stability.
*   **Efficient Message Buffering**: Implements a highly optimized Single-Producer, Single-Consumer (SPSC) lock-free ring buffer (`SpscBuffers`) using `boost::container::static_vector`, atomic operations with specific memory orders, and cache-line alignment to minimize latency for market data messages.
