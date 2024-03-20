/*
 * tests/isam_tests.cpp
 *
 * Unit tests for ISAM Tree shard
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */

#include "shard/FSTrie.h"
#include "include/testing.h"
#include <check.h>

using namespace de;

typedef StringRec R;
typedef FSTrie<R> Shard;

#include "include/shard_string.h"
//#include "include/rangequery.h"

Suite *unit_testing()
{
    Suite *unit = suite_create("Fast-succinct Trie Shard Unit Testing");

 //   inject_rangequery_tests(unit);
    inject_shard_tests(unit);

    return unit;
}


int shard_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_shardner = srunner_create(unit);

    srunner_run_all(unit_shardner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_shardner);
    srunner_free(unit_shardner);

    return failed;
}


int main() 
{
    int unit_failed = shard_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
