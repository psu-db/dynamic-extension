
#include "shard/MemISAM.h"
#include "framework/InternalLevel.h"
#include "util/bf_config.h"
#include "testing.h"

#include <check.h>

using namespace de;

typedef MemISAM<Rec> M_ISAM;

START_TEST(t_memtable_init)
{
    auto buffer = new MutableBuffer<Rec>(1024, true, 512, g_rng);
    for (uint64_t i = 512; i > 0; i--) {
        buffer->append(Rec {i, (uint32_t)i});
    }
    
    Rec r;
    r.set_tombstone();
    for (uint64_t i = 1; i <= 256; ++i) {
        r.key = i;
        r.value = i;
        buffer->append(r);
    }

    for (uint64_t i = 257; i <= 512; ++i) {
        buffer->append({i, (uint32_t) i+1});
    }

    BloomFilter* bf = new BloomFilter(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS, g_rng);
    M_ISAM* run = new M_ISAM(buffer, bf, false);
    ck_assert_uint_eq(run->get_record_count(), 512);

    delete bf;
    delete buffer;
    delete run;
}

START_TEST(t_inmemrun_init)
{
    size_t n = 512;
    auto memtable1 = create_test_mbuffer<Rec>(n);
    auto memtable2 = create_test_mbuffer<Rec>(n);
    auto memtable3 = create_test_mbuffer<Rec>(n);

    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf2 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf3 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    auto run1 = new M_ISAM(memtable1, bf1, false);
    auto run2 = new M_ISAM(memtable2, bf2, false);
    auto run3 = new M_ISAM(memtable3, bf3, false);

    BloomFilter* bf4 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    M_ISAM* runs[3] = {run1, run2, run3};
    auto run4 = new M_ISAM(runs, 3, bf4, false);

    ck_assert_int_eq(run4->get_record_count(), n * 3);
    ck_assert_int_eq(run4->get_tombstone_count(), 0);

    size_t total_cnt = 0;
    size_t run1_idx = 0;
    size_t run2_idx = 0;
    size_t run3_idx = 0;

    for (size_t i = 0; i < run4->get_record_count(); ++i) {
        auto rec1 = run1->get_record_at(run1_idx);
        auto rec2 = run2->get_record_at(run2_idx);
        auto rec3 = run3->get_record_at(run3_idx);

        auto cur_rec = run4->get_record_at(i);

        if (run1_idx < n && cur_rec == rec1) {
            ++run1_idx;
        } else if (run2_idx < n && cur_rec == rec2) {
            ++run2_idx;
        } else if (run3_idx < n && cur_rec == rec3) {
            ++run3_idx;
        } else {
           assert(false);
        }
    }

    delete memtable1;
    delete memtable2;
    delete memtable3;

    delete bf1;
    delete run1;
    delete bf2;
    delete run2;
    delete bf3;
    delete run3;
    delete bf4;
    delete run4;
}

START_TEST(t_get_lower_bound_index)
{
    size_t n = 10000;
    auto memtable = create_double_seq_mbuffer<Rec>(n);

    ck_assert_ptr_nonnull(memtable);
    BloomFilter* bf = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    M_ISAM* run = new M_ISAM(memtable, bf, false);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);

    auto tbl_records = memtable->sorted_output();
    for (size_t i=0; i<n; i++) {
        const auto *tbl_rec = memtable->get_record_at(i);
        auto pos = run->get_lower_bound(tbl_rec->key);
        ck_assert_int_eq(run->get_record_at(pos)->key, tbl_rec->key);
        ck_assert_int_le(pos, i);
    }

    delete memtable;
    delete bf;
    delete run;
}

START_TEST(t_get_upper_bound_index)
{
    size_t n = 10000;
    auto memtable = create_double_seq_mbuffer<Rec>(n);

    ck_assert_ptr_nonnull(memtable);
    BloomFilter* bf = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    M_ISAM* run = new M_ISAM(memtable, bf, false);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);

    auto tbl_records = memtable->sorted_output();
    for (size_t i=0; i<n; i++) {
        const auto *tbl_rec = memtable->get_record_at(i);
        auto pos = run->get_upper_bound(tbl_rec->key);
        ck_assert(pos == run->get_record_count() ||
                  run->get_record_at(pos)->key > tbl_rec->key);
        ck_assert_int_ge(pos, i);
    }

    delete memtable;
    delete bf;
    delete run;
}


START_TEST(t_full_cancelation)
{
    size_t n = 100;
    auto mtable = create_double_seq_mbuffer<Rec>(n, false);
    auto mtable_ts = create_double_seq_mbuffer<Rec>(n, true);
    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf2 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf3 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);

    M_ISAM* run = new M_ISAM(mtable, bf1, false);
    M_ISAM* run_ts = new M_ISAM(mtable_ts, bf2, false);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);
    ck_assert_int_eq(run_ts->get_record_count(), n);
    ck_assert_int_eq(run_ts->get_tombstone_count(), n);

    M_ISAM* runs[] = {run, run_ts};

    M_ISAM* merged = new M_ISAM(runs, 2, bf3, false);

    ck_assert_int_eq(merged->get_tombstone_count(), 0);
    ck_assert_int_eq(merged->get_record_count(), 0);

    delete mtable;
    delete mtable_ts;
    delete bf1;
    delete bf2;
    delete bf3;
    delete run;
    delete run_ts;
    delete merged;
}
END_TEST

Suite *unit_testing()
{
    Suite *unit = suite_create("M_ISAM Unit Testing");

    TCase *create = tcase_create("lsm::M_ISAM constructor Testing");
    tcase_add_test(create, t_memtable_init);
    tcase_add_test(create, t_inmemrun_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);

    TCase *bounds = tcase_create("lsm::M_ISAM::get_{lower,upper}_bound Testing");
    tcase_add_test(bounds, t_get_lower_bound_index);
    tcase_add_test(bounds, t_get_upper_bound_index);
    tcase_set_timeout(bounds, 100);   
    suite_add_tcase(unit, bounds);

    TCase *tombstone = tcase_create("lsm::M_ISAM::tombstone cancellation Testing");
    tcase_add_test(tombstone, t_full_cancelation);
    suite_add_tcase(unit, tombstone);

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
