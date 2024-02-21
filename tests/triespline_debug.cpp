/*
 * tests/internal_level_tests.cpp
 *
 * Unit tests for InternalLevel
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */

#include <functional>
#include "ts/builder.h"

#include <check.h>


START_TEST(t_sequential_integers)
{
    size_t n = 4097;
    auto bldr = ts::Builder<size_t>(436712, 440808, 1024);

    for (size_t i=436712; i<n + 436712; i++) {
        bldr.AddKey(i);
    }

    auto ts = bldr.Finalize();
}


Suite *unit_testing()
{
    Suite *unit = suite_create("InternalLevel Unit Testing");

    TCase *ts = tcase_create("TrieSpline::debugging");
    tcase_add_test(ts, t_sequential_integers);
    tcase_set_timeout(ts, 1000);
    suite_add_tcase(unit, ts);

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
