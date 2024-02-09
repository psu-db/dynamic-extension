/*
 * include/framework/DynamicExtension.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <atomic>
#include <cstdio>
#include <vector>
#include <set>
#include <mutex>

#include "framework/interface/Scheduler.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "framework/scheduling/SerialScheduler.h"

#include "framework/structure/MutableBuffer.h"
#include "framework/interface/Record.h"
#include "framework/structure/ExtensionStructure.h"

#include "framework/util/Configuration.h"
#include "framework/scheduling/Epoch.h"



namespace de {

template <RecordInterface R, ShardInterface<R> S, QueryInterface<R, S> Q, LayoutPolicy L=LayoutPolicy::TEIRING, 
          DeletePolicy D=DeletePolicy::TAGGING, SchedulerInterface SCHED=SerialScheduler>
class DynamicExtension {
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;
    typedef ExtensionStructure<R, S, Q, L> Structure;
    typedef Epoch<R, S, Q, L> _Epoch;
    typedef BufferView<R> BufView;

    static constexpr size_t QUERY = 1;
    static constexpr size_t RECONSTRUCTION = 2;

    struct epoch_ptr {
        _Epoch *epoch;
        size_t refcnt;
    };

public:
    DynamicExtension(size_t buffer_lwm, size_t buffer_hwm, size_t scale_factor, size_t memory_budget=0, 
                     size_t thread_cnt=16)
        : m_scale_factor(scale_factor)
        , m_max_delete_prop(1)
        , m_sched(memory_budget, thread_cnt)
        , m_buffer(new Buffer(buffer_lwm, buffer_hwm))
        , m_core_cnt(thread_cnt)
        , m_next_core(0)
        , m_epoch_cnt(0)
    {
        auto vers = new Structure(buffer_hwm, m_scale_factor, m_max_delete_prop);
        m_current_epoch.store({new _Epoch(0, vers, m_buffer, 0), 0});
        m_previous_epoch.store({nullptr, 0});
        m_next_epoch.store({nullptr, 0});

        m_versions.insert(vers);
    }

    ~DynamicExtension() {

        /* let any in-flight epoch transition finish */
        await_next_epoch();

        /* shutdown the scheduler */
        m_sched.shutdown();

        /* delete all held resources */
        delete m_next_epoch.load().epoch;
        delete m_current_epoch.load().epoch;
        delete m_previous_epoch.load().epoch;

        delete m_buffer;

        for (auto e : m_versions) {
            delete e;
        }
    }

    /*
     * Insert the record `rec` into the index. If the buffer is full and
     * the framework is blocking on an epoch transition, this call may fail
     * and return 0. In this case, retry the call again later. If
     * successful, 1 will be returned. The record will be immediately
     * visible in the buffer upon the successful return of this function.
     */
    int insert(const R &rec) {
        return internal_append(rec, false);
    }

    /*
     * Erase the record `rec` from the index. It is assumed that `rec`
     * currently exists--no special checks are made for correctness here.
     * The behavior if this function will differ depending on if tombstone
     * or tagged deletes are used.
     *
     * Tombstone deletes - inserts a tombstone record for `rec`. This *may*
     * return 0 and fail if the buffer is full and the framework is
     * blocking on an epoch transition. In this case, repeat the call
     * later. 1 will be returned when the tombstone is successfully
     * inserted.
     *
     * Tagging deletes - Does a point lookup for the record across the
     * entire structure, and sets its delete bit when found. Returns 1 if
     * the record is found and marked, and 0 if it was not (i.e., if it
     * isn't present in the index). 
     */
    int erase(const R &rec) {
        // FIXME: delete tagging will require a lot of extra work to get
        //        operating "correctly" in a concurrent environment.
 
        /* 
         * Get a view on the buffer *first*. This will ensure a stronger 
         * ordering than simply accessing the buffer directly, but is
         * not *strictly* necessary.
         */
        if constexpr (D == DeletePolicy::TAGGING) {
            auto view = m_buffer->get_buffer_view();
            static_assert(std::same_as<SCHED, SerialScheduler>, "Tagging is only supported in single-threaded operation");
            if (get_active_epoch()->get_structure()->tagged_delete(rec)) {
                return 1;
            }

            /*
             * the buffer will take the longest amount of time, and 
             * probably has the lowest probability of having the record,
             * so we'll check it last.
             */
            return view.delete_record(rec);
        }

        /*
         * If tagging isn't used, then delete using a tombstone
         */
        return internal_append(rec, true);
    }

    /*
     * Execute the query with parameters `parms` and return a future. This
     * future can be used to access a vector containing the results of the
     * query.
     *
     * The behavior of this function is undefined if `parms` is not a
     * pointer to a valid query parameter object for the query type used as
     * a template parameter to construct the framework.
     */
    std::future<std::vector<R>> query(void *parms) {
        return schedule_query(parms);
    }

    /*
     * Returns the number of records (included tagged records and
     * tombstones) currently within the framework.
     */
    size_t get_record_count() {
        auto epoch = get_active_epoch();
        auto t =  epoch->get_buffer().get_record_count() + epoch->get_structure()->get_record_count();
        end_job(epoch);

        return t;
    }

    /*
     * Returns the number of tombstone records currently within the
     * framework. This function can be called when tagged deletes are used,
     * but will always return 0 in that case.
     */
    size_t get_tombstone_count() {
        auto epoch = get_active_epoch();
        auto t = epoch->get_buffer().get_tombstone_count() + epoch->get_structure()->get_tombstone_count();
        end_job(epoch);

        return t;
    }

    /*
     * Get the number of levels within the framework. This count will
     * include any empty levels, but will not include the buffer. Note that
     * this is *not* the same as the number of shards when tiering is used,
     * as each level can contain multiple shards in that case.
     */
    size_t get_height() {
        auto epoch = get_active_epoch();
        auto t = epoch->get_structure()->get_height();
        end_job(epoch);

        return t;
    }

    /*
     * Get the number of bytes of memory allocated across the framework for
     * storing records and associated index information (i.e., internal
     * ISAM tree nodes). This includes memory that is allocated but
     * currently unused in the buffer, or in shards themselves
     * (overallocation due to delete cancellation, etc.).
     */
    size_t get_memory_usage() {
        auto epoch = get_active_epoch();
        auto t= epoch->get_buffer().get_memory_usage() + epoch->get_structure()->get_memory_usage();
        end_job(epoch);

        return t;
    }

    /*
     * Get the number of bytes of memory allocated across the framework for
     * auxiliary structures. This can include bloom filters, aux
     * hashtables, etc. 
     */
    size_t get_aux_memory_usage() {
        auto epoch = get_active_epoch();
        auto t = epoch->get_buffer().get_aux_memory_usage() + epoch->get_structure()->get_aux_memory_usage();
        end_job(epoch);

        return t;
    }

    /*
     * Returns the maximum physical capacity of the buffer, measured in
     * records.
     */
    size_t get_buffer_capacity() {
        return m_buffer->get_capacity();
    }
    
    /*
     * Create a new single Shard object containing all of the records
     * within the framework (buffer and shards). The optional parameter can
     * be used to specify whether the Shard should be constructed with the
     * currently active state of the framework (false), or if shard
     * construction should wait until any ongoing reconstructions have
     * finished and use that new version (true).
     */
    Shard *create_static_structure(bool await_reconstruction_completion=false) {
        if (await_reconstruction_completion) {
            await_next_epoch();
        }

        auto epoch = get_active_epoch();
        auto vers = epoch->get_structure();
        std::vector<Shard *> shards;


        if (vers->get_levels().size() > 0) {
            for (int i=vers->get_levels().size() - 1; i>= 0; i--) {
                if (vers->get_levels()[i] && vers->get_levels()[i]->get_record_count() > 0) {
                    shards.emplace_back(vers->get_levels()[i]->get_combined_shard());
                }
            }
        }

        /* 
         * construct a shard from the buffer view. We'll hold the view
         * for as short a time as possible: once the records are exfiltrated
         * from the buffer, there's no reason to retain a hold on the view's
         * head pointer any longer
         */
        {
            auto bv = epoch->get_buffer();
            if (bv.get_record_count() > 0) {
                shards.emplace_back(new S(std::move(bv)));
            }
        }

        Shard *flattened = new S(shards);

        for (auto shard : shards) {
            delete shard;
        }

        end_job(epoch);
        return flattened;
    }

    /*
     * If the current epoch is *not* the newest one, then wait for
     * the newest one to become available. Otherwise, returns immediately.
     */
    void await_next_epoch() {
        while (m_next_epoch.load().epoch != nullptr) {
            std::unique_lock<std::mutex> lk(m_epoch_cv_lk);
            m_epoch_cv.wait(lk);
        }
    }

    /*
     * Mostly exposed for unit-testing purposes. Verifies that the current
     * active version of the ExtensionStructure doesn't violate the maximum
     * tombstone proportion invariant. 
     */
    bool validate_tombstone_proportion() {
        auto epoch = get_active_epoch();
        auto t = epoch->get_structure()->validate_tombstone_proportion();
        end_job(epoch);
        return t;
    }


    void print_scheduler_statistics() {
        m_sched.print_statistics();
    }

private:
    SCHED m_sched;

    Buffer *m_buffer;

    std::mutex m_struct_lock;
    std::set<Structure *> m_versions;

    alignas(64) std::atomic<bool> m_reconstruction_scheduled;

    std::atomic<epoch_ptr> m_next_epoch;
    std::atomic<epoch_ptr> m_current_epoch;
    std::atomic<epoch_ptr> m_previous_epoch;

    std::condition_variable m_epoch_cv;
    std::mutex m_epoch_cv_lk;

    std::atomic<size_t> m_epoch_cnt;

    size_t m_scale_factor;
    double m_max_delete_prop;

    std::atomic<int> m_next_core;
    size_t m_core_cnt;

    void enforce_delete_invariant(_Epoch *epoch) {
        auto structure = epoch->get_structure(); 
        auto compactions = structure->get_compaction_tasks();

        while (compactions.size() > 0) {

            /* schedule a compaction */
            ReconstructionArgs<R, S, Q, L> *args = new ReconstructionArgs<R, S, Q, L>();
            args->epoch = epoch;
            args->merges = compactions;
            args->extension = this;
            args->compaction = true;
            /* NOTE: args is deleted by the reconstruction job, so shouldn't be freed here */

            auto wait = args->result.get_future();

            m_sched.schedule_job(reconstruction, 0, args, RECONSTRUCTION);

            /* wait for compaction completion */
            wait.get();

            /* get a new batch of compactions to perform, if needed */
            compactions = structure->get_compaction_tasks();
        }
    }

    _Epoch *get_active_epoch() {
        epoch_ptr old, new_ptr;

        do {
            /* 
             * during an epoch transition, a nullptr will installed in the
             * current_epoch. At this moment, the "new" current epoch will
             * soon be installed, but the "current" current epoch has been
             * moved back to m_previous_epoch.
             */
            if (m_current_epoch.load().epoch == nullptr) {
                old = m_previous_epoch;
                new_ptr = {old.epoch, old.refcnt+1};
                if (old.epoch != nullptr && m_previous_epoch.compare_exchange_strong(old, new_ptr)) {
                    break;
                }
            } else {
                old = m_current_epoch;
                new_ptr = {old.epoch, old.refcnt+1};
                if (old.epoch != nullptr && m_current_epoch.compare_exchange_strong(old, new_ptr)) {
                    break;
                }
            }
        } while (true);

        assert(new_ptr.refcnt > 0);

        return new_ptr.epoch;
    }

    void advance_epoch(size_t buffer_head) {

        retire_epoch(m_previous_epoch.load().epoch);

        epoch_ptr tmp = {nullptr, 0};
        epoch_ptr cur;
        do {
            cur = m_current_epoch;
        } while(!m_current_epoch.compare_exchange_strong(cur, tmp));

        m_previous_epoch.store(cur);

        // FIXME: this may currently block because there isn't any
        // query preemption yet. At this point, we'd need to either
        // 1) wait for all queries on the old_head to finish
        // 2) kill all queries on the old_head
        // 3) somehow migrate all queries on the old_head to the new
        //    version
        while (!m_next_epoch.load().epoch->advance_buffer_head(buffer_head)) {
            _mm_pause();
        }


        m_current_epoch.store(m_next_epoch);
        m_next_epoch.store({nullptr, 0});


        /* notify any blocking threads that the new epoch is available */
        m_epoch_cv_lk.lock();
        m_epoch_cv.notify_all();
        m_epoch_cv_lk.unlock();
    }

    /*
     * Creates a new epoch by copying the currently active one. The new epoch's
     * structure will be a shallow copy of the old one's. 
     */
    _Epoch *create_new_epoch() {
        /*
         * This epoch access is _not_ protected under the assumption that
         * only one reconstruction will be able to trigger at a time. If that condition
         * is violated, it is possible that this code will clone a retired
         * epoch.
         */
        assert(m_next_epoch.load().epoch == nullptr);
        auto current_epoch = get_active_epoch();

        m_epoch_cnt.fetch_add(1);
        m_next_epoch.store({current_epoch->clone(m_epoch_cnt.load()), 0});

        end_job(current_epoch);

        std::unique_lock<std::mutex> m_struct_lock;
        m_versions.insert(m_next_epoch.load().epoch->get_structure());
        m_struct_lock.release();


        return m_next_epoch.load().epoch;
    }

    void retire_epoch(_Epoch *epoch) {
        /*
         * Epochs with currently active jobs cannot
         * be retired. By the time retire_epoch is called,
         * it is assumed that a new epoch is active, meaning
         * that the epoch to be retired should no longer
         * accumulate new active jobs. Eventually, this
         * number will hit zero and the function will
         * proceed.
         */

        if (epoch == nullptr) {
            return;
        }

        epoch_ptr old, new_ptr;
        new_ptr = {nullptr, 0};
        do {
            old = m_previous_epoch.load();

            if (old.epoch == epoch && old.refcnt == 0 &&
                m_previous_epoch.compare_exchange_strong(old, new_ptr)) {
                break;
            }
            usleep(1);
	    
        } while(true);

        delete epoch;

        /*
         * Following the epoch's destruction, any buffers
         * or structures with no remaining references can
         * be safely freed.
         */
        std::unique_lock<std::mutex> lock(m_struct_lock);

        for (auto itr = m_versions.begin(); itr != m_versions.end();) {
            if ((*itr)->get_reference_count() == 0) {
                auto tmp = *itr;
                itr = m_versions.erase(itr);
                delete tmp;
            } else {
                itr++;
            }
        }
    }

    static void reconstruction(void *arguments) {
        auto args = (ReconstructionArgs<R, S, Q, L> *) arguments;

        ((DynamicExtension *) args->extension)->SetThreadAffinity();
        Structure *vers = args->epoch->get_structure();

        for (ssize_t i=0; i<args->merges.size(); i++) {
            vers->reconstruction(args->merges[i].second, args->merges[i].first);
        }

        /*
         * we'll grab the buffer AFTER doing the internal reconstruction, so we
         * can flush as many records as possible in one go. The reconstruction
         * was done so as to make room for the full buffer anyway, so there's 
         * no real benefit to doing this first.
         */
        auto buffer_view = args->epoch->get_buffer();
        size_t new_head = buffer_view.get_tail();

        /*
         * if performing a compaction, don't flush the buffer, as
         * there is no guarantee that any necessary reconstructions
         * will free sufficient space in L0 to support a flush
         */
        if (!args->compaction) {
            vers->flush_buffer(std::move(buffer_view));
        }

        args->result.set_value(true);

        /*
         * Compactions occur on an epoch _before_ it becomes active,
         * and as a result the active epoch should _not_ be advanced as
         * part of a compaction 
         */
        if (!args->compaction) {
            ((DynamicExtension *) args->extension)->advance_epoch(new_head);
        }

        ((DynamicExtension *) args->extension)->m_reconstruction_scheduled.store(false);
        
        delete args;
    }
    
    static void async_query(void *arguments) {
        QueryArgs<R, S, Q, L> *args = (QueryArgs<R, S, Q, L> *) arguments;

        auto epoch = ((DynamicExtension *) args->extension)->get_active_epoch();

        auto ptr1 = ((DynamicExtension *) args->extension)->m_previous_epoch.load().epoch;
        auto ptr2 = ((DynamicExtension *) args->extension)->m_current_epoch.load().epoch;
        auto ptr3 = ((DynamicExtension *) args->extension)->m_next_epoch.load().epoch;


        auto buffer = epoch->get_buffer();
        auto vers = epoch->get_structure();
        void *parms = args->query_parms;

        /* Get the buffer query states */
        void *buffer_state = Q::get_buffer_query_state(&buffer, parms);

        /* Get the shard query states */
        std::vector<std::pair<ShardID, Shard*>> shards;
        std::vector<void *> states = vers->get_query_states(shards, parms);

        Q::process_query_states(parms, states, buffer_state);

        std::vector<std::vector<Wrapped<R>>> query_results(shards.size() + 1);
        for (size_t i=0; i<query_results.size(); i++) {
            std::vector<Wrapped<R>> local_results;
            ShardID shid;

            if (i == 0) { /* process the buffer first */
                local_results = Q::buffer_query(buffer_state, parms);
                shid = INVALID_SHID;
            } else {
                local_results = Q::query(shards[i - 1].second, states[i - 1], parms);
                shid = shards[i - 1].first; 
            }

            query_results[i] = std::move(filter_deletes(local_results, shid, vers, &buffer)); 

            if constexpr (Q::EARLY_ABORT) {
                if (query_results[i].size() > 0) break;
            }
        }

        auto result = Q::merge(query_results, parms);
        args->result_set.set_value(std::move(result));

        ((DynamicExtension *) args->extension)->end_job(epoch);

        Q::delete_buffer_query_state(buffer_state);
        for (size_t i=0; i<states.size(); i++) {
            Q::delete_query_state(states[i]);
        }

        delete args;
    }

    void schedule_reconstruction() {
        auto epoch = create_new_epoch();
        /* 
         * the reconstruction process calls end_job(), 
         * so we must start one before calling it
         */

        ReconstructionArgs<R, S, Q, L> *args = new ReconstructionArgs<R, S, Q, L>();
        args->epoch = epoch;
        args->merges = epoch->get_structure()->get_reconstruction_tasks(m_buffer->get_high_watermark());
        args->extension = this;
        args->compaction = false;
        /* NOTE: args is deleted by the reconstruction job, so shouldn't be freed here */

        m_sched.schedule_job(reconstruction, 0, args, RECONSTRUCTION);
    }

    std::future<std::vector<R>> schedule_query(void *query_parms) {
        QueryArgs<R, S, Q, L> *args = new QueryArgs<R, S, Q, L>();
        args->extension = this;
        args->query_parms = query_parms;
        auto result = args->result_set.get_future();

        m_sched.schedule_job(async_query, 0, args, QUERY);

        return result;
    }

    int internal_append(const R &rec, bool ts) {
        if (m_buffer->is_at_low_watermark()) {
            auto old = false;

            if (m_reconstruction_scheduled.compare_exchange_strong(old, true)) {
                schedule_reconstruction();
            }
        }

        /* this will fail if the HWM is reached and return 0 */
        return m_buffer->append(rec, ts);
    }

    static std::vector<Wrapped<R>> filter_deletes(std::vector<Wrapped<R>> &records, ShardID shid, Structure *vers, BufView *bview) {
        if constexpr (Q::SKIP_DELETE_FILTER) {
            return records;
        }

        std::vector<Wrapped<R>> processed_records;
        processed_records.reserve(records.size());

        /* 
         * For delete tagging, we just need to check the delete bit 
         * on each record. 
         */
        if constexpr (D == DeletePolicy::TAGGING) {
            for (auto &rec : records) {
                if (rec.is_deleted()) {
                    continue;
                }

                processed_records.emplace_back(rec);
            }

            return processed_records;
        }

        /*
         * For tombstone deletes, we need to search for the corresponding 
         * tombstone for each record.
         */
        for (auto &rec : records) {
           if (rec.is_tombstone()) {
                continue;
            } 

            // FIXME: need to figure out how best to re-enable the buffer tombstone
            // check in the correct manner.
            //if (buffview.check_tombstone(rec.rec)) {
                //continue;
            //}

            for (size_t i=0; i<bview->get_record_count(); i++) {
                if (bview->get(i)->is_tombstone() && bview->get(i)->rec == rec.rec) {
                    continue;
                }
            }

            if (shid != INVALID_SHID) {
                for (size_t lvl=0; lvl<=shid.level_idx; lvl++) {
                    if (vers->get_levels()[lvl]->check_tombstone(0, rec.rec)) {
                        continue;
                    }
                }

                if (vers->get_levels()[shid.level_idx]->check_tombstone(shid.shard_idx + 1, rec.rec)) {
                    continue;
                }
            }

            processed_records.emplace_back(rec);
        }

        return processed_records;
    }

    void SetThreadAffinity() {
        int core = m_next_core.fetch_add(1) % m_core_cnt;
        cpu_set_t mask;
        CPU_ZERO(&mask);

        switch (core % 2) {
        case 0:
          // 0 |-> 0
          // 2 |-> 2
          // 4 |-> 4
          core = core;
          break;
        case 1:
          // 1 |-> 28
          // 3 |-> 30
          // 5 |-> 32
          core = (core - 1) + m_core_cnt;
          break;
        }
        CPU_SET(core, &mask);
        ::sched_setaffinity(0, sizeof(mask), &mask);
    }


    void end_job(_Epoch *epoch) {
        epoch_ptr old, new_ptr;

        do {
            if (m_previous_epoch.load().epoch == epoch) {
                old = m_previous_epoch;
                /* 
                 * This could happen if we get into the system during a
                 * transition. In this case, we can just back out and retry
                 */
                if (old.epoch == nullptr) {
                    continue;
                }

                assert(old.refcnt > 0);

                new_ptr = {old.epoch, old.refcnt - 1};
                if (m_previous_epoch.compare_exchange_strong(old, new_ptr)) {
                    break;
                }
            } else {
                old = m_current_epoch;
                /* 
                 * This could happen if we get into the system during a
                 * transition. In this case, we can just back out and retry
                 */
                if (old.epoch == nullptr) {
                    continue;
                }

                assert(old.refcnt > 0);

                new_ptr = {old.epoch, old.refcnt - 1};
                if (m_current_epoch.compare_exchange_strong(old, new_ptr)) {
                    break;
                }
            }
        } while (true);
    }

};
}

