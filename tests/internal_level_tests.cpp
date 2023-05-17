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
#include "util/bf_config.h"
#include "testing.h"

#include <check.h>

using namespace de;

START_TEST(t_memlevel_merge)
{
    auto tbl1 = create_test_mbuffer<WRec>(100);
    auto tbl2 = create_test_mbuffer<WRec>(100);

    auto base_level = new InternalLevel<WRec, WIRS<WRec>>(1, 1);
    base_level->append_mem_table(tbl1, g_rng);
    ck_assert_int_eq(base_level->get_record_cnt(), 100);

    auto merging_level = new InternalLevel<WRec, WIRS<WRec>>(0, 1);
    merging_level->append_mem_table(tbl2, g_rng);
    ck_assert_int_eq(merging_level->get_record_cnt(), 100);

    auto old_level = base_level;
    base_level = InternalLevel<WRec, WIRS<WRec>>::merge_levels(old_level, merging_level, g_rng);

    delete old_level;
    delete merging_level;
    ck_assert_int_eq(base_level->get_record_cnt(), 200);

    delete base_level;
    delete tbl1;
    delete tbl2;
}


InternalLevel<WRec, WIRS<WRec>> *create_test_memlevel(size_t reccnt) {
    auto tbl1 = create_test_mbuffer<WRec>(reccnt/2);
    auto tbl2 = create_test_mbuffer<WRec>(reccnt/2);

    auto base_level = new InternalLevel<WRec, WIRS<WRec>>(1, 2);
    base_level->append_mem_table(tbl1, g_rng);
    base_level->append_mem_table(tbl2, g_rng);

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
