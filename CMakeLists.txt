cmake_minimum_required(VERSION 3.13)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(namespace "de")
project("Practical Dynamic Extension" VERSION 0.1.0)

set(debug false)
set(tests True)
set(bench True)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

add_compile_options(-Iinclude -Iexternal/PLEX/include)

if (debug) 
    add_compile_options(-g -O0)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
else()
    add_compile_options(-O3)
endif()

# Test build instructions
if (tests) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/tests")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/tests/data")

    add_executable(pagedfile_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pagedfile_tests.cpp)
    target_link_libraries(pagedfile_tests PUBLIC gsl check subunit pthread)
    target_include_directories(pagedfile_tests PRIVATE include)

    add_executable(wirs_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/wirs_tests.cpp)
    target_link_libraries(wirs_tests PUBLIC gsl check subunit pthread)
    target_include_directories(wirs_tests PRIVATE include)

    add_executable(internal_level_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/internal_level_tests.cpp)
    target_link_libraries(internal_level_tests PUBLIC gsl check subunit pthread)
    target_include_directories(internal_level_tests PRIVATE include)

    add_executable(mutable_buffer_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/mutable_buffer_tests.cpp)
    target_link_libraries(mutable_buffer_tests PUBLIC gsl check subunit pthread)
    target_include_directories(mutable_buffer_tests PRIVATE include)

    add_executable(dynamic_extension_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/dynamic_extension_tests.cpp)
    target_link_libraries(dynamic_extension_tests PUBLIC gsl check subunit pthread)
    target_include_directories(dynamic_extension_tests PRIVATE include)
    
    add_executable(memisam_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/memisam_tests.cpp)
    target_link_libraries(memisam_tests PUBLIC gsl check subunit pthread)
    target_include_directories(memisam_tests PRIVATE include)

    add_executable(wss_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/wss_tests.cpp)
    target_link_libraries(wss_tests PUBLIC gsl check subunit pthread)
    target_include_directories(wss_tests PRIVATE include)

    add_executable(triespline_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/triespline_tests.cpp)
    target_link_libraries(triespline_tests PUBLIC gsl check subunit pthread)
    target_include_directories(triespline_tests PRIVATE include external/PLEX/include)

    add_executable(pgm_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pgm_tests.cpp)
    target_link_libraries(pgm_tests PUBLIC gsl check subunit pthread)
    target_include_directories(pgm_tests PRIVATE include external/PGM-index/include)
endif()

# Benchmark build instructions
if (bench)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/benchmarks")

    add_executable(sampling_tput ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/sampling_tput.cpp)
    target_link_libraries(sampling_tput PUBLIC gsl pthread)
    target_include_directories(sampling_tput PRIVATE include)


endif()
