# YCSB Client Design

## REVIEW COMMENTS:

*   **Retain `std::variant` encapsulation for `GeneralServer`**: The current design utilizing `std::variant` for `LeanStoreServer`, `WiredTigerServer`, and `RocksDbServer` will be kept due to its performance benefits (lower overhead than virtual functions) and the fixed, limited number of target backends.
*   **Retain `KvSpace` + `Server` abstraction**: The existing `GeneralServer` (managing backend worker threads/task executors) and `GeneralKvSpace` (handling specific `put`, `get`, `update` operations) abstraction will be maintained.
*   **`--clients=0` embedded mode**: The design for the embedded execution mode for `--clients=0` is approved.

---

This document outlines the design of a flexible and extensible YCSB (Yahoo! Cloud Serving Benchmark) client for LeanStore and other database backends. The primary goal is to create a modular architecture that supports different benchmarking scenarios while leveraging existing optimized components.

## 1. Core Components

The benchmark tool is composed of several key components:

-   **`YcsbOptions`**: Handles parsing of command-line arguments, such as the target database, workload parameters, number of threads, and the number of clients (`--clients`).

-   **`YcsbExecutor`**: The main orchestrator of the benchmark. It initializes the environment, sets up the database instance (via `GeneralServer`), and manages the execution of the specified workload (`load` or `run`). It is responsible for selecting the execution mode based on the `--clients` parameter.

-   **`GeneralServer`**: This class acts as the primary interface to the selected database backend. It encapsulates the specifics of initializing and managing a database server, including its associated worker threads or task executors. It uses `std::variant` to wrap one of the specific server implementations: `LeanStoreServer`, `RocksDbServer`, or `WiredTigerServer`.
    *   It provides methods for:
        *   Creating and managing the database instance (e.g., `LeanStore::Open`, RocksDB `DB::Open`).
        *   Submitting asynchronous tasks to the database's internal worker threads (e.g., `AsyncExecOn`).
        *   Creating the initial `KvSpace` for benchmark operations.

-   **`GeneralKvSpace`**: This class provides a unified interface for key-value operations (`Get`, `Put`, `Update`, `Delete`, `Scan`) across different database backends. It is typically obtained from `GeneralServer` and is responsible for translating generic KV operations into the specific calls of the wrapped database (e.g., LeanStore's `BasicKV` or `TransactionKV`, RocksDB's `DB` operations). Like `GeneralServer`, it uses `std::variant` to dispatch calls to the actual backend KV space.

-   **Backend Server Implementations (`LeanStoreServer`, `RocksDbServer`, `WiredTigerServer`)**: These are concrete classes that manage the lifecycle and interaction with a specific database system. They are wrapped by `GeneralServer`'s `std::variant`. Each provides methods for:
    *   `Create(const YcsbOptions& options)`: Static factory method to initialize the specific database.
    *   `AsyncExecOn(uint64_t wid, Func&& func)`: Submits a lambda function to be executed on a specific worker thread (if applicable).
    *   `GetKvSpace()`: Returns a `std::unique_ptr<GeneralKvSpace>` configured for this backend.

-   **`BenchClient`**: Represents a single client thread. This component is only used when the number of clients (`--clients=N`) is greater than zero. Each `BenchClient` instance runs in its own thread, generates a workload, and sends requests to the database via the `GeneralServer` (specifically, using its `AsyncExecOn` or `ExecOn` methods). It also maintains its own statistics.

-   **`YcsbBenchStats`**: A class for collecting and reporting performance statistics, such as throughput (ops/sec) and latency. In client-server mode, each `BenchClient` will have its own `YcsbBenchStats` instance.

## 2. Execution Flow

The benchmark supports two distinct execution modes, determined by the `--clients` command-line argument.

### Mode 1: Embedded Mode (`--clients=0`)

This mode is designed to measure the raw performance of the database engine by eliminating any client-server communication overhead.

1.  The `YcsbExecutor` initializes and creates a `GeneralServer` instance.
2.  The executor starts a number of long-running tasks, equal to the number of worker threads (`--threads`).
3.  These tasks are submitted to the `GeneralServer` (via `AsyncExecOn`) and run directly on the database's internal worker threads (e.g., LeanStore's coroutine scheduler, RocksDB's internal threads).
4.  Inside each task, a loop generates YCSB requests (read, update, etc.) and obtains a `GeneralKvSpace` (via `GeneralServer::GetKvSpace()`) to perform the actual key-value operations.
5.  Statistics are collected within these tasks (potentially using thread-local stats objects) and later aggregated by the `YcsbExecutor`.

### Mode 2: Client-Server Mode (`--clients=N`, where N > 0)

This mode simulates a more realistic scenario where `N` clients concurrently send requests to the database server.

1.  The `YcsbExecutor` initializes and creates a single shared `GeneralServer` instance.
2.  The executor creates and launches `N` `BenchClient` instances, each running in its own `std::thread`.
3.  Each `BenchClient` is assigned a unique client ID, an instance of `GeneralServer` (likely a reference or pointer to the shared instance), and its own `YcsbBenchStats` object.
4.  Inside its thread, each `BenchClient` generates YCSB requests and submits them to the shared `GeneralServer` instance using methods like `AsyncExecOn` or `ExecOn`. The lambda function submitted will contain the logic to get a `GeneralKvSpace` from the `GeneralServer` and perform the actual KV operation.
5.  The `GeneralServer` implementation is responsible for thread-safely handling these concurrent requests and dispatching them to its internal worker threads as needed. For example, `LeanStoreServer` would submit the client's request as a lambda function to its coroutine scheduler.
6.  Each `BenchClient` tracks its own performance metrics in its `YcsbBenchStats` object.

## 3. Statistics Reporting

-   The `YcsbExecutor` starts a dedicated reporter thread that periodically wakes up.
-   In **Embedded Mode**, it aggregates statistics directly from the worker tasks (e.g., from thread-local `YcsbBenchStats` instances).
-   In **Client-Server Mode**, it iterates through the `N` `BenchClient` instances, accesses their individual `YcsbBenchStats` objects, and aggregates the results (e.g., summing up total committed and aborted transactions, combining latency histograms).
-   The reporter thread then prints a consolidated summary of the overall performance to the console. This ensures that statistics reporting is centralized and consistent across both modes.