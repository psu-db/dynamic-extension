
#include "shard/WIRS.h"
#include "framework/InternalLevel.h"
#include "util/bf_config.h"
#include "testing.h"

#include <check.h>

using namespace de;

typedef WIRS<uint64_t, uint32_t, uint64_t> Shard;

START_TEST(t_mbuffer_init)
{
    auto mem_table = new WeightedMBuffer(1024, true, 1024, g_rng);
    for (uint64_t i = 512; i > 0; i--) {
        uint32_t v = i;
        mem_table->append(i, v);
    }
    
    for (uint64_t i = 1; i <= 256; ++i) {
        uint32_t v = i;
        mem_table->append(i, v, 1.0, true);
    }

    for (uint64_t i = 257; i <= 512; ++i) {
        uint32_t v = i + 1;
        mem_table->append(i, v);
    }

    BloomFilter* bf = new BloomFilter(BF_FPR, mem_table->get_tombstone_count(), BF_HASH_FUNCS, g_rng);
    Shard* run = new Shard(mem_table, bf, false);
    ck_assert_uint_eq(run->get_record_count(), 512);

    delete bf;
    delete mem_table;
    delete run;
}

START_TEST(t_wirs_init)
{
    size_t n = 512;
    auto mbuffer1 = create_test_mbuffer(n);
    auto mbuffer2 = create_test_mbuffer(n);
    auto mbuffer3 = create_test_mbuffer(n);

    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf2 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf3 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    auto run1 = new Shard(mbuffer1, bf1, false);
    auto run2 = new Shard(mbuffer2, bf2, false);
    auto run3 = new Shard(mbuffer3, bf3, false);

    BloomFilter* bf4 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    Shard* runs[3] = {run1, run2, run3};
    auto run4 = new Shard(runs, 3, bf4, false);

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

        if (run1_idx < n && cur_rec->match(rec1)) {
            ++run1_idx;
        } else if (run2_idx < n && cur_rec->match(rec2)) {
            ++run2_idx;
        } else if (run3_idx < n && cur_rec->match(rec3)) {
            ++run3_idx;
        } else {
           assert(false);
        }
    }

    delete mbuffer1;
    delete mbuffer2;
    delete mbuffer3;

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
    auto mbuffer = create_double_seq_mbuffer(n);

    ck_assert_ptr_nonnull(mbuffer);
    BloomFilter* bf = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    Shard* run = new Shard(mbuffer, bf, false);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);

    auto tbl_records = mbuffer->sorted_output();
    for (size_t i=0; i<n; i++) {
        const WeightedRec *tbl_rec = mbuffer->get_record_at(i);
        auto pos = run->get_lower_bound(tbl_rec->key);
        ck_assert_int_eq(run->get_record_at(pos)->key, tbl_rec->key);
        ck_assert_int_le(pos, i);
    }

    delete mbuffer;
    delete bf;
    delete run;
}


START_TEST(t_full_cancelation)
{
    size_t n = 100;
    auto buffer = create_double_seq_mbuffer(n, false);
    auto buffer_ts = create_double_seq_mbuffer(n, true);
    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf2 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf3 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);

    Shard* run = new Shard(buffer, bf1, false);
    Shard* run_ts = new Shard(buffer_ts, bf2, false);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);
    ck_assert_int_eq(run_ts->get_record_count(), n);
    ck_assert_int_eq(run_ts->get_tombstone_count(), n);

    Shard* runs[] = {run, run_ts};

    Shard* merged = new Shard(runs, 2, bf3, false);

    ck_assert_int_eq(merged->get_tombstone_count(), 0);
    ck_assert_int_eq(merged->get_record_count(), 0);

    delete buffer;
    delete buffer_ts;
    delete bf1;
    delete bf2;
    delete bf3;
    delete run;
    delete run_ts;
    delete merged;
}
END_TEST


START_TEST(t_weighted_sampling)
{
    size_t n=1000;
    auto buffer = create_weighted_mbuffer(n);

    BloomFilter* bf = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    Shard* run = new Shard(buffer, bf, false);

    uint64_t lower_key = 0;
    uint64_t upper_key = 5;

    size_t k = 1000;

    std::vector<WeightedRec> results;
    results.reserve(k);
    size_t cnt[3] = {0};
    for (size_t i=0; i<1000; i++) {
        auto state = run->get_sample_run_state(lower_key, upper_key);
        
        run->get_samples(state, results, lower_key, upper_key, k, g_rng);

        for (size_t j=0; j<k; j++) {
            cnt[results[j].key - 1]++;
        }

        WIRS<uint64_t, uint32_t, uint64_t>::delete_state(state);
    }

    ck_assert(roughly_equal(cnt[0] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[1] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[2] / 1000, (double) k/2.0, k, .05));

    delete run;
    delete bf;
    delete buffer;
}
END_TEST


START_TEST(t_tombstone_check)
{
    size_t cnt = 1024;
    size_t ts_cnt = 256;
    auto buffer = new WeightedMBuffer(cnt + ts_cnt, true, ts_cnt, g_rng);

    std::vector<std::pair<uint64_t, uint32_t>> tombstones;

    uint64_t key = 1000;
    uint32_t val = 101;
    for (size_t i = 0; i < cnt; i++) {
        buffer->append(key, val);
        key++;
        val++;
    }

    // ensure that the key range doesn't overlap, so nothing
    // gets cancelled.
    for (size_t i=0; i<ts_cnt; i++) {
        tombstones.push_back({i, i});
    }

    for (size_t i=0; i<ts_cnt; i++) {
        buffer->append(tombstones[i].first, tombstones[i].second, 1.0, true);
    }

    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    auto run = new Shard(buffer, bf1, false);

    for (size_t i=0; i<tombstones.size(); i++) {
        ck_assert(run->check_tombstone(tombstones[i].first, tombstones[i].second));
        ck_assert_int_eq(run->get_rejection_count(), i+1);
    }

    delete run;
    delete buffer;
    delete bf1;
}
END_TEST

Suite *unit_testing()
{
    Suite *unit = suite_create("WIRS Shard Unit Testing");

    TCase *create = tcase_create("de::WIRS constructor Testing");
    tcase_add_test(create, t_mbuffer_init);
    tcase_add_test(create, t_wirs_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);


    TCase *bounds = tcase_create("de:WIRS::get_{lower,upper}_bound Testing");
    tcase_add_test(bounds, t_get_lower_bound_index);
    tcase_set_timeout(bounds, 100);   
    suite_add_tcase(unit, bounds);


    TCase *tombstone = tcase_create("de:WIRS::tombstone cancellation Testing");
    tcase_add_test(tombstone, t_full_cancelation);
    suite_add_tcase(unit, tombstone);


    TCase *sampling = tcase_create("de:WIRS::sampling Testing");
    tcase_add_test(sampling, t_weighted_sampling);
    suite_add_tcase(unit, sampling);


    TCase *check_ts = tcase_create("de::WIRS::check_tombstone Testing");
    tcase_add_test(check_ts, t_tombstone_check);
    suite_add_tcase(unit, check_ts);

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
