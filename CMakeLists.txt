cmake_minimum_required(VERSION 3.13)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(namespace "de")
project("Practical Dynamic Extension" VERSION 0.1.0)

set(debug true)
set(tests True)
set(bench false)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

add_compile_options(-Iinclude -Iexternal/PLEX/include -Iexternal)

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

    add_executable(wirs_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/wirs_tests.cpp)
    target_link_libraries(wirs_tests PUBLIC gsl check subunit pthread)
    target_include_directories(wirs_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(internal_level_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/internal_level_tests.cpp)
    target_link_libraries(internal_level_tests PUBLIC gsl check subunit pthread)
    target_include_directories(internal_level_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(mutable_buffer_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/mutable_buffer_tests.cpp)
    target_link_libraries(mutable_buffer_tests PUBLIC gsl check subunit pthread)
    target_include_directories(mutable_buffer_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(vptree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/vptree_tests.cpp)
    target_link_libraries(vptree_tests PUBLIC gsl check subunit pthread)
    target_include_directories(vptree_tests PRIVATE include external/vptree external/psudb-common/cpp/include)

    #add_executable(dynamic_extension_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/dynamic_extension_tests.cpp)
    #target_link_libraries(dynamic_extension_tests PUBLIC gsl check subunit pthread)
    #target_include_directories(dynamic_extension_tests PRIVATE include)

    add_executable(de_tier_tag ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_tier_tag.cpp)
    target_link_libraries(de_tier_tag PUBLIC gsl check subunit pthread)
    target_include_directories(de_tier_tag PRIVATE include external/psudb-common/cpp/include external)

    add_executable(de_tier_tomb ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_tier_tomb.cpp)
    target_link_libraries(de_tier_tomb PUBLIC gsl check subunit pthread)
    target_include_directories(de_tier_tomb PRIVATE include external/psudb-common/cpp/include external)

    add_executable(de_level_tag ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_level_tag.cpp)
    target_link_libraries(de_level_tag PUBLIC gsl check subunit pthread)
    target_include_directories(de_level_tag PRIVATE include external/psudb-common/cpp/include external)

    add_executable(de_level_tomb ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_level_tomb.cpp)
    target_link_libraries(de_level_tomb PUBLIC gsl check subunit pthread)
    target_include_directories(de_level_tomb PRIVATE include external/ctpl external/psudb-common/cpp/include external)
    
    add_executable(memisam_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/memisam_tests.cpp)
    target_link_libraries(memisam_tests PUBLIC gsl check subunit pthread)
    target_include_directories(memisam_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(wss_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/wss_tests.cpp)
    target_link_libraries(wss_tests PUBLIC gsl check subunit pthread)
    target_include_directories(wss_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(triespline_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/triespline_tests.cpp)
    target_link_libraries(triespline_tests PUBLIC gsl check subunit pthread)
    target_include_directories(triespline_tests PRIVATE include external/PLEX/include external/psudb-common/cpp/include)

    add_executable(pgm_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pgm_tests.cpp)
    target_link_libraries(pgm_tests PUBLIC gsl check subunit pthread gomp)
    target_include_directories(pgm_tests PRIVATE include external/PGM-index/include external/psudb-common/cpp/include)
    target_compile_options(pgm_tests PUBLIC -fopenmp)
endif()

# Benchmark build instructions
if (bench)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/benchmarks")

    add_executable(alias_wss_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/alias_wss_bench.cpp)
    target_link_libraries(alias_wss_bench PUBLIC gsl pthread gomp)
    target_include_directories(alias_wss_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(alias_wss_bench PUBLIC -fopenmp)

    add_executable(triespline_rq_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/triespline_rq_bench.cpp)
    target_link_libraries(triespline_rq_bench PUBLIC gsl pthread gomp)
    target_include_directories(triespline_rq_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(triespline_rq_bench PUBLIC -fopenmp)

    add_executable(pgm_rq_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/pgm_rq_bench.cpp)
    target_link_libraries(pgm_rq_bench PUBLIC gsl pthread gomp)
    target_include_directories(pgm_rq_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(pgm_rq_bench PUBLIC -fopenmp)

    add_executable(pgm_pl_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/pgm_pl_bench.cpp)
    target_link_libraries(pgm_pl_bench PUBLIC gsl pthread gomp)
    target_include_directories(pgm_pl_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(pgm_pl_bench PUBLIC -fopenmp)

    add_executable(upgm_rq_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/upgm_rq_bench.cpp)
    target_link_libraries(upgm_rq_bench PUBLIC gsl pthread gomp)
    target_include_directories(upgm_rq_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(upgm_rq_bench PUBLIC -fopenmp)
    set_property(TARGET upgm_rq_bench PROPERTY CXX_STANDARD 17)

    add_executable(upgm_pl_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/upgm_pl_bench.cpp)
    target_link_libraries(upgm_pl_bench PUBLIC gsl pthread gomp)
    target_include_directories(upgm_pl_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(upgm_pl_bench PUBLIC -fopenmp)
    set_property(TARGET upgm_pl_bench PROPERTY CXX_STANDARD 17)

    add_executable(isam_irs_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/isam_irs_bench.cpp)
    target_link_libraries(isam_irs_bench PUBLIC gsl pthread gomp)
    target_include_directories(isam_irs_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(isam_irs_bench PUBLIC -fopenmp)

    add_executable(isam_rq_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/isam_rq_bench.cpp)
    target_link_libraries(isam_rq_bench PUBLIC gsl pthread gomp)
    target_include_directories(isam_rq_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(isam_rq_bench PUBLIC -fopenmp)

    add_executable(btree_irs_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/btree_irs_bench.cpp)
    target_link_libraries(btree_irs_bench PUBLIC gsl pthread gomp)
    target_include_directories(btree_irs_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(btree_irs_bench PUBLIC -fopenmp)

    add_executable(btree_rq_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/btree_rq_bench.cpp)
    target_link_libraries(btree_rq_bench PUBLIC gsl pthread gomp)
    target_include_directories(btree_rq_bench PRIVATE include external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(btree_rq_bench PUBLIC -fopenmp)

    add_executable(alex_rq_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/alex_rq_bench.cpp)
    target_link_libraries(alex_rq_bench PUBLIC gsl pthread gomp)
    target_include_directories(alex_rq_bench PRIVATE include external/alex/src/core external/m-tree/cpp external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(alex_rq_bench PUBLIC -fopenmp -march=native)
    set_property(TARGET alex_rq_bench PROPERTY CXX_STANDARD 14)

    add_executable(vptree_knn_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vptree_knn_bench.cpp)
    target_link_libraries(vptree_knn_bench PUBLIC gsl pthread gomp)
    target_include_directories(vptree_knn_bench PRIVATE include external/m-tree/cpp external/vptree external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(vptree_knn_bench PUBLIC -fopenmp)

    add_executable(mtree_knn_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/mtree_knn_bench.cpp)
    target_link_libraries(mtree_knn_bench PUBLIC gsl pthread gomp)
    target_include_directories(mtree_knn_bench PRIVATE include external/m-tree/cpp external/vptree external/PGM-index/include external/PLEX/include bench/include)
    target_compile_options(mtree_knn_bench PUBLIC -fopenmp -fpermissive)

endif()
