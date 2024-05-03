cmake_minimum_required(VERSION 3.13)

#set(CMAKE_C_COMPILER clang)
#set(CMAKE_CXX_COMPILER clang++)

set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_CXX_STANDARD_REQUIRED True)

set(namespace "de")
project("Practical Dynamic Extension" VERSION 0.1.0)

set(debug false)
set(tests True)
set(bench false)
set(vldb_bench true)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")
set(CMAKE_CXX_FLAGS=-latomic -mcx16)

add_compile_options(-Iinclude -Iexternal/PLEX/include -Iexternal -mcx16 -march=native) # -fconcepts-diagnostics-depth=3)

if (BSD) 
    add_link_options(-L/usr/local/lib)
    add_compile_options(-I/usr/local/include)
    set(CMAKE_CXX_EXTENSIONS)
endif()

if (debug) 
    add_compile_options(-g -O0)
    if (!BSD)
        add_compile_options(-fsanitize=address)
        add_link_options(-fsanitize=address)
        add_compile_options(-fsanitize=undefined)
        add_link_options(-fsanitize=undefined)
    endif()
else()
    add_compile_options(-O3 -g)
endif()

# Test build instructions
if (tests) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/tests")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/tests/data")

    add_executable(augbtree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/augbtree_tests.cpp)
    target_link_libraries(augbtree_tests PUBLIC gsl check subunit pthread atomic)
    target_link_options(augbtree_tests PUBLIC -mcx16)
    target_include_directories(augbtree_tests PRIVATE include external/psudb-common/cpp/include external/ctpl)

    add_executable(internal_level_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/internal_level_tests.cpp)
    target_link_libraries(internal_level_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(internal_level_tests PUBLIC -mcx16)
    target_include_directories(internal_level_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(mutable_buffer_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/mutable_buffer_tests.cpp)
    target_link_libraries(mutable_buffer_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(mutable_buffer_tests PUBLIC -mcx16)
    target_include_directories(mutable_buffer_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(rangequery_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/rangequery_tests.cpp)
    target_link_libraries(rangequery_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(rangequery_tests PUBLIC -mcx16)
    target_include_directories(rangequery_tests PRIVATE include external/psudb-common/cpp/include)


    add_executable(rangecount_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/rangecount_tests.cpp)
    target_link_libraries(rangecount_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(rangecount_tests PUBLIC -mcx16)
    target_include_directories(rangecount_tests PRIVATE include external/psudb-common/cpp/include)


    add_executable(vptree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/vptree_tests.cpp)
    target_link_libraries(vptree_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(vptree_tests PUBLIC -mcx16)
    target_include_directories(vptree_tests PRIVATE include external/vptree external/psudb-common/cpp/include)
    
    add_executable(de_tier_tag ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_tier_tag.cpp)
    target_link_libraries(de_tier_tag PUBLIC gsl check subunit  pthread atomic)
    target_link_options(de_tier_tag PUBLIC -mcx16)
    target_include_directories(de_tier_tag PRIVATE include external/psudb-common/cpp/include external)

    add_executable(de_tier_tomb ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_tier_tomb.cpp)
    target_link_libraries(de_tier_tomb PUBLIC gsl check subunit  pthread atomic)
    target_link_options(de_tier_tomb PUBLIC -mcx16)
    target_include_directories(de_tier_tomb PRIVATE include external/PLEX/include external/psudb-common/cpp/include external)

    add_executable(de_level_tag ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_level_tag.cpp)
    target_link_libraries(de_level_tag PUBLIC gsl check subunit  pthread atomic)
    target_link_options(de_level_tag PUBLIC -mcx16)
    target_include_directories(de_level_tag PRIVATE include external/psudb-common/cpp/include external)

    add_executable(de_level_tomb ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_level_tomb.cpp)
    target_link_libraries(de_level_tomb PUBLIC gsl check subunit  pthread atomic)
    target_link_options(de_level_tomb PUBLIC -mcx16)
    target_include_directories(de_level_tomb PRIVATE include external/ctpl external/PLEX/include external/psudb-common/cpp/include external)

    add_executable(de_bsm_tomb ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_bsm_tomb.cpp)
    target_link_libraries(de_bsm_tomb PUBLIC gsl check subunit  pthread atomic)
    target_link_options(de_bsm_tomb PUBLIC -mcx16)
    target_include_directories(de_bsm_tomb PRIVATE include external/PLEX/include external/psudb-common/cpp/include external)

    add_executable(de_level_concurrent ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_level_concurrent.cpp)
    target_link_libraries(de_level_concurrent PUBLIC gsl check subunit  pthread atomic)
    target_link_options(de_level_concurrent PUBLIC -mcx16)
    target_include_directories(de_level_concurrent PRIVATE include external/ctpl external/PLEX/include external/psudb-common/cpp/include external)

    add_executable(de_tier_concurrent ${CMAKE_CURRENT_SOURCE_DIR}/tests/de_tier_concurrent.cpp)
    target_link_libraries(de_tier_concurrent PUBLIC gsl check subunit  pthread atomic)
    target_link_options(de_tier_concurrent PUBLIC -mcx16)
    target_include_directories(de_tier_concurrent PRIVATE include external/ctpl external/PLEX/include external/psudb-common/cpp/include external)
    
    add_executable(memisam_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/memisam_tests.cpp)
    target_link_libraries(memisam_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(memisam_tests PUBLIC -mcx16)
    target_include_directories(memisam_tests PRIVATE include external/psudb-common/cpp/include)

    add_executable(triespline_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/triespline_tests.cpp)
    target_link_libraries(triespline_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(triespline_tests PUBLIC -mcx16)
    target_include_directories(triespline_tests PRIVATE include external/psudb-common/cpp/include external/PLEX/include)

    add_executable(alias_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/alias_tests.cpp)
    target_link_libraries(alias_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(alias_tests PUBLIC -mcx16)
    target_include_directories(alias_tests PRIVATE include external/psudb-common/cpp/include)

    # OpenBSD doesn't have OpenMP support, so don't build the PGM code on that
    # platform.
    add_executable(pgm_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pgm_tests.cpp)
    target_link_libraries(pgm_tests PUBLIC gsl check subunit  pthread gomp atomic)
    target_include_directories(pgm_tests PRIVATE include external/PGM-index/include external/psudb-common/cpp/include)
    target_link_options(pgm_tests PUBLIC -mcx16)
    target_compile_options(pgm_tests PUBLIC -fopenmp)

    # Triespline code doesn't build under OpenBSD either due to ambiguous function call;
    # this is likely a difference between gcc and clang, rather than an OS thing 
    add_executable(triespline_debug ${CMAKE_CURRENT_SOURCE_DIR}/tests/triespline_debug.cpp)
    target_link_libraries(triespline_debug PUBLIC gsl check subunit  pthread atomic)
    target_link_options(triespline_debug PUBLIC -mcx16)
    target_include_directories(triespline_debug PRIVATE include external/psudb-common/cpp/include external/PLEX/include)


    add_executable(fst_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/fst_tests.cpp)
    target_link_libraries(fst_tests PUBLIC gsl check subunit  pthread atomic)
    target_link_options(fst_tests PUBLIC -mcx16)
    target_include_directories(fst_tests PRIVATE include external/psudb-common/cpp/include external/PLEX/include external/fast_succinct_trie/include external/louds-patricia)

    #add_executable(louds_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/louds_tests.cpp)
    #target_link_libraries(louds_tests PUBLIC gsl check subunit  pthread atomic)
    #target_link_options(louds_tests PUBLIC -mcx16)
    #target_include_directories(louds_tests PRIVATE include external/psudb-common/cpp/include external/PLEX/include external/fast_succinct_trie/include external/louds-patricia)
endif()

if (vldb_bench)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/benchmarks")

    add_executable(irs_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/irs_bench.cpp)
    target_link_libraries(irs_bench PUBLIC gsl pthread atomic)
    target_include_directories(irs_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(irs_bench PUBLIC -mcx16)
    target_compile_options(irs_bench PUBLIC -fopenmp)

    add_executable(vptree_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/vptree_bench.cpp)
    target_link_libraries(vptree_bench PUBLIC gsl pthread atomic)
    target_include_directories(vptree_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(vptree_bench PUBLIC -mcx16)
    target_compile_options(vptree_bench PUBLIC -fopenmp)

    add_executable(vptree_parmsweep ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/vptree_parmsweep.cpp)
    target_link_libraries(vptree_parmsweep PUBLIC gsl pthread atomic)
    target_include_directories(vptree_parmsweep PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(vptree_parmsweep PUBLIC -mcx16)
    target_compile_options(vptree_parmsweep PUBLIC -fopenmp)

    add_executable(vptree_bsm_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/vptree_bsm_bench.cpp)
    target_link_libraries(vptree_bsm_bench PUBLIC gsl pthread atomic)
    target_include_directories(vptree_bsm_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(vptree_bsm_bench PUBLIC -mcx16)
    target_compile_options(vptree_bsm_bench PUBLIC -fopenmp)

    add_executable(fst_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/fst_bench.cpp)
    target_link_libraries(fst_bench PUBLIC gsl pthread atomic)
    target_include_directories(fst_bench PRIVATE include external external/fast_succinct_trie/include external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(fst_bench PUBLIC -mcx16)
    target_compile_options(fst_bench PUBLIC -fopenmp)

    add_executable(fst_bsm_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/fst_bsm_bench.cpp)
    target_link_libraries(fst_bsm_bench PUBLIC gsl pthread atomic)
    target_include_directories(fst_bsm_bench PRIVATE include external external/fast_succinct_trie/include external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(fst_bsm_bench PUBLIC -mcx16)
    target_compile_options(fst_bsm_bench PUBLIC -fopenmp)

    add_executable(ts_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/ts_bench.cpp)
    target_link_libraries(ts_bench PUBLIC gsl pthread atomic)
    target_include_directories(ts_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(ts_bench PUBLIC -mcx16)
    target_compile_options(ts_bench PUBLIC -fopenmp)

    add_executable(ts_parmsweep ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/ts_parmsweep.cpp)
    target_link_libraries(ts_parmsweep PUBLIC gsl pthread atomic)
    target_include_directories(ts_parmsweep PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(ts_parmsweep PUBLIC -mcx16)
    target_compile_options(ts_parmsweep PUBLIC -fopenmp)

    add_executable(ts_bsm_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/ts_bsm_bench.cpp)
    target_link_libraries(ts_bsm_bench PUBLIC gsl pthread atomic)
    target_include_directories(ts_bsm_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(ts_bsm_bench PUBLIC -mcx16)
    target_compile_options(ts_bsm_bench PUBLIC -fopenmp)

    #add_executable(ts_mdsp_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/ts_mdsp_bench.cpp)
    #target_link_libraries(ts_mdsp_bench PUBLIC gsl pthread atomic)
    #target_include_directories(ts_mdsp_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    #target_link_options(ts_mdsp_bench PUBLIC -mcx16)
    #target_compile_options(ts_mdsp_bench PUBLIC -fopenmp)
    
    add_executable(pgm_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/pgm_bench.cpp)
    target_link_libraries(pgm_bench PUBLIC gsl pthread atomic gomp)
    target_include_directories(pgm_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(pgm_bench PUBLIC -mcx16)
    target_compile_options(pgm_bench PUBLIC -fopenmp)

    add_executable(dynamic_pgm_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/dynamic_pgm_bench.cpp)
    target_link_libraries(dynamic_pgm_bench PUBLIC gsl pthread atomic gomp)
    target_include_directories(dynamic_pgm_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(dynamic_pgm_bench PUBLIC -mcx16)
    target_compile_options(dynamic_pgm_bench PUBLIC -fopenmp)

    add_executable(btree_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/btree_bench.cpp)
    target_link_libraries(btree_bench PUBLIC gsl pthread atomic gomp)
    target_include_directories(btree_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(btree_bench PUBLIC -mcx16)
    target_compile_options(btree_bench PUBLIC -fopenmp)

    add_executable(alex_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/alex_bench.cpp)
    target_link_libraries(alex_bench PUBLIC gsl )
    target_include_directories(alex_bench PRIVATE external/psudb-common/cpp/include external/alex/src/core/ benchmarks/include)
    target_compile_options(alex_bench PUBLIC -fopenmp)
    set_property(TARGET alex_bench PROPERTY CXX_STANDARD 14)

    add_executable(mtree_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/vldb/mtree_bench.cpp)
    target_link_libraries(mtree_bench PUBLIC gsl pthread atomic gomp)
    target_include_directories(mtree_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(mtree_bench PUBLIC -mcx16)
    target_compile_options(mtree_bench PUBLIC -fopenmp)
endif()



if (bench) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/benchmarks")

    add_executable(reconstruction_interference ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/reconstruction_interference.cpp)
    target_link_libraries(reconstruction_interference PUBLIC gsl pthread atomic)
    target_link_options(reconstruction_interference PUBLIC -mcx16)
    target_include_directories(reconstruction_interference PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)

    add_executable(insertion_tput ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/insertion_tput.cpp)
    target_link_libraries(insertion_tput PUBLIC gsl pthread atomic)
    target_include_directories(insertion_tput PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(insertion_tput PUBLIC -mcx16)


    add_executable(string_insertion_tput ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/string_insertion_tput.cpp)
    target_link_libraries(string_insertion_tput PUBLIC gsl pthread atomic)
    target_include_directories(string_insertion_tput PRIVATE include external external/fast_succinct_trie/include external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(string_insertion_tput PUBLIC -mcx16)


    add_executable(louds_insertion_tput ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/louds_insertion_tput.cpp)
    target_link_libraries(louds_insertion_tput PUBLIC gsl pthread atomic)
    target_include_directories(louds_insertion_tput PRIVATE include external external/fast_succinct_trie/include external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include external/louds-patricia)
    target_link_options(louds_insertion_tput PUBLIC -mcx16)


    add_executable(query_workload_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/query_workload_bench.cpp)
    target_link_libraries(query_workload_bench PUBLIC gsl pthread atomic)
    target_include_directories(query_workload_bench PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(query_workload_bench PUBLIC -mcx16)

    add_executable(insert_query_tput ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/insert_query_tput.cpp)
    target_link_libraries(insert_query_tput PUBLIC gsl pthread atomic)
    target_include_directories(insert_query_tput PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(insert_query_tput PUBLIC -mcx16)

    add_executable(poplar_trie ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/poplar_trie.cpp)
    target_link_libraries(poplar_trie PUBLIC gsl pthread atomic)
    target_include_directories(poplar_trie PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include external/poplar-trie/include)
    target_link_options(poplar_trie PUBLIC -mcx16)

    add_executable(hat_trie ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/hat_trie.cpp)
    target_link_libraries(hat_trie PUBLIC gsl pthread atomic)
    target_include_directories(hat_trie PRIVATE include external
    external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include external/hat-trie/include/tsl)
    target_link_options(hat_trie PUBLIC -mcx16)

    add_executable(cedar_trie ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/cedar_trie.cpp) 
    target_link_libraries(cedar_trie PUBLIC gsl pthread atomic)
    target_include_directories(cedar_trie PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include external/hat-trie/include/tsl)
    target_link_options(cedar_trie PUBLIC -mcx16)

    #add_executable(btree_insert_query_tput ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/btree_insert_query_tput.cpp)
    #target_link_libraries(btree_insert_query_tput PUBLIC gsl cblas pthread atomic)
    #target_include_directories(btree_insert_query_tput PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    #target_link_options(btree_insert_query_tput PUBLIC -mcx16)

    add_executable(watermark_testing ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/watermark_testing.cpp)
    target_link_libraries(watermark_testing PUBLIC gsl pthread atomic)
    target_include_directories(watermark_testing PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(watermark_testing PUBLIC -mcx16)

    #add_executable(static_dynamic_comp ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/static_dynamic_comp.cpp)
    #target_link_libraries(static_dynamic_comp PUBLIC gsl cblas pthread atomic)
    #target_include_directories(static_dynamic_comp PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    #target_link_options(static_dynamic_comp PUBLIC -mcx16)

    add_executable(insert_tail_latency ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/insert_tail_latency.cpp)
    target_link_libraries(insert_tail_latency PUBLIC gsl pthread atomic)
    target_include_directories(insert_tail_latency PRIVATE include external external/m-tree/cpp external/PGM-index/include external/PLEX/include benchmarks/include external/psudb-common/cpp/include)
    target_link_options(insert_tail_latency PUBLIC -mcx16)
endif()
