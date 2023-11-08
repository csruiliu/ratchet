# Ratchet

Ratchet is a prototype system for pipeline-based query suspending and resuming

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

## Source Code Modification

`Sink()` and `Finalize()` are the main functions for query suspension and resumption. Usually, query suspension should happen in `Finalize()`, while query resumption should happen in the Sink(). However, it is still case-by-case due to implementation or performance reason, for example, resumption for aggregation may happen in `GetData()`.

The modifications are across the entire source code base, we list some representative source code modifications:

1. Suspension and resumption Client APIs `tools/pythonpkg`, such as, `pyconnection.cpp`, `pyconnection.hp`, etc.
2. Pipeline dependency management is in `src/parallel`, such as, `pipeline.cpp`, `executor.cpp`, `pipeline_executor.cpp`, etc.
3. Suspending and resuming ungrouped aggregation in `src/execution/operator/aggregate`, such as, `physical_ungrouped_aggregate.cpp`, etc.
4. Suspending and resuming groupby aggregation in `src/execution/operator/aggregate`, such as, `physical_hash_aggregate`, etc. 
5. Suspending and resuming join in `src/execution/join`, such as, `physical_hash_join.cpp`, `physical_range_join.cpp`, `perfect_hash_join_executor.cpp`, etc.
6. Suspending and resuming order in `src/execution/operator/order`, such as, `physical_order.cpp`, etc. 
