# Dynamic Extension Framework
This is a C++20, header-only, library for automatically building data
structures with support for concurrent updates from static data structures.

## Dependencies
This project requires libboost, libgsl, and libcheck (for unit tests). It also
uses a 128bit compare-and-swap atomic, which may not be available on
particularly ancient CPUs. It currently builds using gcc.

## Building Tests and Benchmarks
The project is built using cmake. CMakeLists contains several flags that can 
be used to customize the build. In particular, setting `debug` to true will
build without optimization, and with asan and ubsan enabled. The `tests`
and `benchmarks` flag can be used to toggle the construction of unit tests
and benchmarks respectively. The benchmarks specific to the VLDB 2024 
submission are built using the `vldb_bench` flag.

After configuring the build to your liking, make sure that you initialized the
git sub-modules first if you've just cloned the repository.
```
% git submodule update --init --recursive
```

and then build the project using,
```
% mkdir build
% cd build
% cmake ..
% make -j
```

The unit test binaries will appear in `bin/tests/` and the benchmarks in
`bin/benchmarks/`.

## Available Benchmarks
The benchmarks relevant to the 2024 VLDB submission are located in
`benchmarks/vldb`, and various other benchmarks are in `benchmarks`. The
majority of the benchmark suite used for creating the figures in the VLDB
submission can be run using the `bin/scripts/run_benchmarks.sh` script, which
should be updated first with the locations of the desired data and query files.
The main benchmark not automatically run by this script is
`bin/benchmarks/ts_parmsweep`, which should be updated (by editing
`benchmarks/vldb/ts_parmsweep`) to reflect the parameter values to test, and
then run manually as desired.

Most of the benchmarks will display a help message, showing what arguments they
expect, if you run them without any arguments, or with the wrong number of
arguments.

## Unit Tests
While test coverage is not perfect yet, there are some basic unit tests in
`tests/`. These tests are built using the [check
unit](https://libcheck.github.io/check/) test framework.

## Documentation to Come
This is still an active research project, and so the interfaces are not yet
stable. Proper documentation will come when these interfaces stabilize
a little. In the meantime, the code will have to serve as the documentation.

Some of the most important files are `include/framework/DynamicExtension`,
which contains the external interface of the framework, and
`include/framework/interface/*`, which contains the necessary C++20
concepts/interfaces for developing your own shards and queries. Any class
implementing the appropriate interfaces can be used as template arguments to
the DynamicExtension itself.

