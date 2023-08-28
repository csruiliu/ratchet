# Ratchet

Ratchet is a prototype system for efficient fine-grained data analytic suspending and resuming

Ratchet implementation is modified from DuckDB.

## Prerequisite

### Third-party

It is highly recommended to add third-party libs whose whole source code is in a single header file. Then, you can add them by,

1. copying the header file of the third-party lib to `third_party` folder
2. adding `include_directories(third_party/xxx)` after `include_directories(src/include)` in the `CMakeLists.txt` at the root directory. You may have to recompile the source code if needed, using `make` in root folder.
3. if you are working on Python client, you also need to update `third_party_includes()` in `scripts/package_build.py`. You may have to reinstall python client to reflect the change, using `pip3 install .` in `/tools/pythonpkg`.

### JSON for Modern C++

We import the `nlohmann/json` to serialize and deserialize JSON. Github: https://github.com/nlohmann/json

### Python Client Modification

When you want to add a new Python API or modify an existing one for DuckDB especially for virtual environments, you need to:
1. Install `mypy` python library in the virtual environment
2. Modify the source code in `tools/pythonpkg/src` to reflect to API change
3. Run `scripts/regenerate_python_stubs.sh` at the **root directory of DuckDB**, making sure `<Ratchet-DuckDB>/tools/pythonpkg/duckdb-stubs/__init__.pyi` already reflect the API change
4. Install the modified DuckDB again using `python setup.py install` in `<Ratchet-DuckDB>/tools/pythonpkg`
5. If you still cannot apply the change you made for Python Client APIs, please repeat 3,4 for mutiple times, you should be fine.

## Source Code Compilation

### Main source code [C++]

The main codebase is written in C++, so it is common to use `cmake` to compile the source code. Namely, using `make` command in root folder.

### Python Client Installation

Install pybind11 using `pip3 install pybind11` (system-wide or virtual environment)

`pip3 show pybind11` will tell you where is the pybind11, for example, /home/{user_path}/{venv}/lib/python3.7/site-packages

Then, `</path/to/pybind11>` is, for example, `/home/{user_path}/{venv}/lib/python3.7/site-packages/pybind11`

If you are using CLion IDE for development, and make sure CLion can link all the source code, you may need to add `-DBUILD_PYTHON_PKG=TRUE -DCMAKE_PREFIX_PATH=</path/to/pybind11>` in `Settings | Build, Execution, Deployment | CMake | CMake Options`. This will tell CLion where to find pybind11.

Ratchet-DuckDB can be used and tested by a python client. It is recommended to install the python client in a python virtual environment.

```bash
source <path/to/python-virtual-environment/bin/activate>
cd <Ratchet>/tools/pythonpkg 
pip3 install . 
# or python setup.py install
```


## Dataset

### TPC-H

First, you need to generate the original tables using TPC-H tools and then move them to `dataset/tpch/tbl` folder. Simply running `duckdb_tpch_data.py` can convert the table files to `parquet` or `csv` format using, for example, the following command,
```bash
python3 duckdb_tpch_data.py -d dataset/tbl/sf1 -f parquet -rgs 100000
```
You can move the converted data to any folder you want.

We have several datasets:

+ TCP-H SF30: `tpch/dataset/tbl/sf10`, `tpch/dataset/parquet/sf10`
+ TCP-H SF10: `tpch/dataset/tbl/sf10`, `tpch/dataset/parquet/sf10`
+ TCP-H SF1: `tpch/dataset/tbl/sf1`, `tpch/dataset/parquet/sf1`
+ TCP-H Small (SF-0.1): `tpch/dataset/tbl/small`, `tpch/dataset/parquet/small`
+ TCP-H Tiny (SF-0.01): `tpch/dataset/tbl/tiny`, `tpch/dataset/parquet/tiny`

## Benchmark

### Vanilla

We provide some demo queries in `demo/queries` for suspend and resume, which can be triggered by the following commands

```bash
# run q1 based on demo.db and the dataset from parquet-tiny using 2 threads
python3 demo.py -q q1 -d demo.db -df ../dataset/tpch/parquet-tiny -td 2

# run q1 with suspension and serialize into single file
python3 demo.py -q q1 -d demo.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl /home/ruiliu/Develop/ratchet-duckdb/ratchet/demo/demo.ratchet 
# run q1 with suspension and serialize into multiple files (will generate part-*.ratchet in demo folder)
python3 demo.py -q q1 -d demo.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl /home/ruiliu/Develop/ratchet-duckdb/ratchet/demo -psr

# run q1 with resumption using a single file
python3 demo.py -q q1 -d demo.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl /home/ruiliu/Develop/ratchet-duckdb/ratchet/demo/demo.ratchet
# run q1 with resumption using multiple files (will use all part-*.ratchet in the demo folder)
python3 demo.py -q q1 -d demo.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl /home/ruiliu/Develop/ratchet-duckdb/ratchet/demo -psr
```

### TPC-H

`duckdb_tpch_perf` will trigger the original TPC-H queries from q1 to q22 (stored in ). For example,
```bash
python3 tpch_perf.py -q q1 -d dataset/parquet/sf1 -td 1
```
The above command will run `q1` in TPC-H based on the data from `dataset/parquet/sf1` using `1` thread.

The TPC-H benchmark is mostly used for functionality test.

### TPC-DS

`duckdb_tpcds_perf`

### CRIU

We also exploit `CRIU` to benchmark the performance of suspending and resuming queries at the process level. More details can be found [here](criu/README.md).

## Source Code Modification

`Sink()`, `Finalize()`, and `GetData()` are the key functions for query suspension and resumption. Usually, query suspension should happen in `Finalize()`, while query resumption should happen in the Sink(). However, it is still case-by-case due to implementation or performance reason, for example, resumption for aggregation may happen in `GetData()`.

1. Adding suspension and resumption APIs in `pyconnection.cpp` and `pyconnection.hp`
2. Checking finished pipelines when resumption in `pipeline.cpp`
3. Suspending and resuming ungrouped aggregation in `physical_ungrouped_aggregate.cpp`
4. Suspending and resuming in-memory hash join in `physical_hash_join.cpp` and `perfect_hash_join_executor.cpp`
5. Suspending and resuming external hash join in `physical_hash_join.cpp`
6. Suspending and resuming grouped aggregation in `physical_hash_aggregate.cpp`

### List of Modification

1. tools/pythonpkg/src/pyconnection.cpp
2. tools/pythonpkg/include/duckdb_python/pyconnection/pyconnection.hpp
3. src/include/duckdb/common/constants.hpp
4. src/include/duckdb/common/types/data_chunk.hpp
5. src/include/duckdb/common/vector_operations/aggregate_executor.hpp
6. src/include/duckdb/execution/operator/join/perfect_hash_join_executor.hpp
7. src/include/duckdb/execution/executor.hpp
8. src/include/duckdb/main/client_config.hpp
9. src/include/duckdb/parallel/pipeline.hpp
10. src/common/constants.cpp 
11. src/main/settings/settings.cpp
12. src/execution/operator/aggregate/physical_hash_aggregate.cpp
13. src/execution/operator/aggregate/physical_ungrouped_aggregate.cpp
14. src/execution/operator/join/perfect_hash_join_executor.cpp
15. src/execution/operator/join/physical_hash_join.cpp
16. src/execution/operator/join/physical_range_join.cpp
17. src/execution/operator/order/physical_order.cpp
18. src/execution/operator/scan/physical_table_scan.cpp
19. src/execution/join_hashtable.cpp
20. src/parallel/executor.cpp
21. src/parallel/pipeline.cpp
22. src/parallel/pipeline_executor.cpp

## DuckDB
DuckDB is a high-performance analytical database system. It is designed to be fast, reliable and easy to use. DuckDB provides a rich SQL dialect, with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs), and more. For more information on the goals of DuckDB, please refer to [the Why DuckDB page on our website](https://duckdb.org/why_duckdb).

### Installation
If you want to install and use DuckDB, please see [our website](https://www.duckdb.org) for installation and usage instructions.

### Data Import
For CSV files and Parquet files, data import is as simple as referencing the file in the FROM clause:

```sql
SELECT * FROM 'myfile.csv';
SELECT * FROM 'myfile.parquet';
```

Refer to our [Data Import](https://duckdb.org/docs/data/overview) section for more information.

### SQL Reference
The [website](https://duckdb.org/docs/sql/introduction) contains a reference of functions and SQL constructs available in DuckDB.

### Development
For development, DuckDB requires [CMake](https://cmake.org), Python3 and a `C++11` compliant compiler. Run `make` in the root directory to compile the sources. For development, use `make debug` to build a non-optimized debug version. You should run `make unit` and `make allunit` to verify that your version works properly after making changes. To test performance, you can run `BUILD_BENCHMARK=1 BUILD_TPCH=1 make` and then perform several standard benchmarks from the root directory by executing `./build/release/benchmark/benchmark_runner`. The detail of benchmarks is in our [Benchmark Guide](benchmark/README.md).

Please also refer to our [Contribution Guide](CONTRIBUTING.md).
