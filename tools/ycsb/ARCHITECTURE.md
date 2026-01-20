# YCSB Benchmark Tool Architecture

This document describes the architecture of the YCSB (Yahoo! Cloud Serving Benchmark) tool implemented in this repository.

## Overview

The `tools/ycsb` directory contains a standalone executable for benchmarking various storage backends using the YCSB workloads. It is designed to be extensible and supports multiple storage engines.

## Client-Server Model

The tool operates as a single process that spawns multiple threads to simulate clients and backend workers. It does not use a network-based client-server model but rather embeds the storage engine directly into the benchmark process (embedded database model).

### Components

1.  **YcsbExecutor (`ycsb_executor.cpp/hpp`)**:
    -   The core orchestrator of the benchmark.
    -   Manages the lifecycle of the benchmark (initialization, loading, running, reporting).
    -   Spawns `client` threads (loaders or runners) and a `reporter` thread.
    -   Collects and aggregates statistics (TPS, Abort Rate) from all client threads.

2.  **YcsbSession**:
    -   Represents a session for a client thread.
    -   Abstracts the thread-local state required by the backend.

3.  **YcsbKvSpace**:
    -   Represents a namespace or table (Key-Value Space) where data is stored.
    -   Provides the data access interface (`Put`, `Get`, `Update`).

## Backends

The tool currently supports the following backends, defined in `ycsb_options.hpp`:

*   **LeanStore**:
    *   `basickv`: Uses LeanStore's `BasicKV` (Atomic, non-transactional).
    *   `transactionkv`: Uses LeanStore's `TransactionKV` (Transactional).
*   **RocksDB**: `rocksdb`
*   **WiredTiger**: `wiredtiger`

## Backend Interface

To add a new backend, one must implement the wrapper classes that adapt the specific storage engine's API to the YCSB interface. These wrappers are managed via `std::variant` in `ycsb_backend.hpp` for static polymorphism / performance.

### Required Classes

1.  **Db Wrapper** (e.g., `YcsbLeanDb`):
    *   `Create(options)`: Factory method to initialize the database.
    *   `NewSession()`: Creates a thread-local session.

2.  **Session Wrapper** (e.g., `YcsbLeanSession`):
    *   `CreateKvSpace(name)`: Creates a new KV store/table.
    *   `GetKvSpace(name)`: Retrieves a handle to an existing KV store.

3.  **KvSpace Wrapper** (e.g., `YcsbLeanAtomicKvSpace`):
    *   `Put(key, value)`: Insert a new record.
    *   `Update(key, value)`: Update an existing record.
    *   `Get(key, value_out)`: Read a record.
    *   *Note: Scan interface is defined in workloads but currently not implemented in the executor loop for A/B/C.*

## Workloads

The tool supports the standard YCSB workloads, defined in `ycsb_workload_spec.hpp`.
*Current implementation in `YcsbExecutor::HandleCmdRun` only supports A, B, and C.*

*   **Workload A**: 50% Read, 50% Update
*   **Workload B**: 95% Read, 5% Update
*   **Workload C**: 100% Read
*   **Workload D**: 95% Read, 5% Insert (Not currently supported in Executor)
*   **Workload E**: 95% Scan, 5% Insert (Not currently supported in Executor)
*   **Workload F**: 50% Read, 50% Read-Modify-Write (Not currently supported in Executor)

## Key Distribution (Zipfian)

The benchmark uses a **Scrambled Zipfian** distribution for key selection to simulate hot spots, which is typical for YCSB.

*   **Implementation**: `src/utils/zipfian_generator.cpp`
*   **Classes**:
    *   `ZipfianGenerator`: Generates numbers according to the Zipfian distribution. Uses an optimized algorithm (Jim Gray's) for performance, computing the Zeta distribution approximation.
    *   `ScrambledZipfianGenerator`: Wraps `ZipfianGenerator`. It generates a number from a larger range and hashes it to the target range `[0, N)`. This "scrambles" the distribution to ensure that the "hot" items are not just the small integers (0, 1, 2...) but are scattered throughout the key space, while maintaining the same skew characteristics.

## Usage

The tool is invoked via the command line.

### Command Format
```bash
./ycsb_bench --backend <backend_name> [options]
```

### Options

| Flag | Description | Default |
| :--- | :--- | :--- |
| `--backend` | Target backend (`basickv`, `transactionkv`, `rocksdb`, `wiredtiger`) | **Required** |
| `--action` | `load` (populate DB) or `run` (execute workload) | `run` |
| `--workload` | YCSB workload (`a`, `b`, `c`, `d`, `e`, `f`) | `a` |
| `--clients` | Number of client/loader threads | `8` |
| `--workers` | Number of backend worker threads | `4` |
| `--dram` | DRAM limit in GB | `1` |
| `--duration` | Execution time in seconds | `30` |
| `--dir` | Data directory | `/tmp/leanstore/ycsb` |
| `--rows` | Number of records (rows) | `10000` |
| `--key_size` | Size of the key in bytes | `16` |
| `--val_size` | Size of the value in bytes | `120` |
| `--zipf_factor` | Zipfian skew factor (0.0 = uniform, 0.99 = highly skewed) | `0.99` |

### Example

1.  **Load Data** (BasicKV):
    ```bash
    ./ycsb_bench --backend basickv --action load --rows 1000000 --clients 10
    ```

2.  **Run Workload A** (BasicKV):
    ```bash
    ./ycsb_bench --backend basickv --action run --workload a --rows 1000000 --clients 10 --duration 60
    ```
