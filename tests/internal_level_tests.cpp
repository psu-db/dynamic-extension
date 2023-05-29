/*
 * tests/internal_level_tests.cpp
 *
 * Unit tests for InternalLevel
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#include "shard/WIRS.h"
#include "framework/InternalLevel.h"
#include "framework/RecordInterface.h"
#include "framework/QueryInterface.h"
#include "framework/ShardInterface.h"

#include "testing.h"

#include <check.h>

using namespace de;

typedef InternalLevel<WrappedRecord<WRec>, WIRS<WrappedRecord<WRec>>, WIRSQuery<WrappedRecord<WRec>>> ILevel;

START_TEST(t_memlevel_merge)
{
    auto tbl1 = create_test_mbuffer<WrappedWRec>(100);
    auto tbl2 = create_test_mbuffer<WrappedWRec>(100);

    auto base_level = new ILevel(1, 1);
    base_level->append_buffer(tbl1);
    ck_assert_int_eq(base_level->get_record_cnt(), 100);

    auto merging_level = new ILevel(0, 1);
    merging_level->append_buffer(tbl2);
    ck_assert_int_eq(merging_level->get_record_cnt(), 100);

    auto old_level = base_level;
    base_level = ILevel::merge_levels(old_level, merging_level);

    delete old_level;
    delete merging_level;
    ck_assert_int_eq(base_level->get_record_cnt(), 200);

    delete base_level;
    delete tbl1;
    delete tbl2;
}


ILevel *create_test_memlevel(size_t reccnt) {
    auto tbl1 = create_test_mbuffer<WrappedWRec>(reccnt/2);
    auto tbl2 = create_test_mbuffer<WrappedWRec>(reccnt/2);

    auto base_level = new ILevel(1, 2);
    base_level->append_buffer(tbl1); 
    base_level->append_buffer(tbl2);

    delete tbl1;
    delete tbl2;

    return base_level;
}

Suite *unit_testing()
{
    Suite *unit = suite_create("InternalLevel Unit Testing");

    TCase *merge = tcase_create("de::InternalLevel::merge_level Testing");
    tcase_add_test(merge, t_memlevel_merge);
    suite_add_tcase(unit, merge);

    return unit;
}

int run_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_runner = srunner_create(unit);

    srunner_run_all(unit_runner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_runner);
    srunner_free(unit_runner);

    return failed;
}


int main() 
{
    int unit_failed = run_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
