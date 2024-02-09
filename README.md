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
and benchmarks respectively. `old_bench` should be ignored--most of these won't
build anymore anyway. Eventually that will be removed.

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
`bin/benchmarks`.

## Available Benchmarks
At present there are several benchmarks available in `benchmarks`. Not every
benchmark has been updated to build with the current version; these can be
found in `benchmarks/old-bench`. Most of the benchmarks will display a help
message, showing what arguments they expect, if you run them without any
arguments, or with the wrong number of arguments.

## Unit Tests
While test coverage is not perfect yet, there are some basic unit tests in
`tests`. These tests are built using the [check
unit](https://libcheck.github.io/check/) test framework.

## Documentation to Come
This is still an active research project, and so the interfaces are not yet
stable. Proper documentation will come when these interfaces stabilize
a little. In the meantime, the code will have to serve as the documentation.
There are also a lot of comments in most of the files, though several are a bit
light in that regard.

