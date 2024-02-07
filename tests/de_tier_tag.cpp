/*
 * tests/de_tier_tag.cpp
 *
 * Unit tests for Dynamic Extension Framework
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#include <set>
#include <random>
#include <algorithm>

#include "include/testing.h"
#include "framework/DynamicExtension.h"
#include "framework/scheduling/SerialScheduler.h"
#include "shard/ISAMTree.h"
#include "query/rangequery.h"

#include <check.h>
using namespace de;

typedef DynamicExtension<Rec, ISAMTree<Rec>, rq::Query<Rec, ISAMTree<Rec>>, LayoutPolicy::TEIRING, DeletePolicy::TAGGING, SerialScheduler> DE;

#include "include/dynamic_extension.h"


Suite *unit_testing()
{
    Suite *unit = suite_create("DynamicExtension: Tagged Tiering Testing");
    inject_dynamic_extension_tests(unit);

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
