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
#include <shared_mutex>
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

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L=LayoutPolicy::TEIRING, 
          DeletePolicy D=DeletePolicy::TAGGING, SchedulerInterface SCHED=SerialScheduler>
class DynamicExtension {
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;
    typedef ExtensionStructure<R, S, Q, L> Structure;
    typedef Epoch<R, S, Q, L> _Epoch;
    typedef BufferView<R> BufView;

    static constexpr size_t QUERY = 1;
    static constexpr size_t RECONSTRUCTION = 2;


public:
    DynamicExtension(size_t buffer_lwm, size_t buffer_hwm, size_t scale_factor, size_t memory_budget=0, 
                     size_t thread_cnt=16)
        : m_scale_factor(scale_factor)
        , m_max_delete_prop(1)
        , m_sched(memory_budget, thread_cnt)
        , m_buffer(new Buffer(buffer_lwm, buffer_hwm))
        , m_core_cnt(thread_cnt)
        , m_next_core(0)
    {
        auto vers = new Structure(buffer_hwm, m_scale_factor, m_max_delete_prop);
        auto epoch = new _Epoch(0, vers, m_buffer, 0);

        m_versions.insert(vers);
        m_epochs.insert({0, epoch});
    }

    ~DynamicExtension() {

        /* let any in-flight epoch transition finish */
        await_next_epoch();

        /* deactivate the active epoch */
        get_active_epoch()->set_inactive();

        /* shutdown the scheduler */
        m_sched.shutdown();

        /* delete all held resources */
        for (auto e : m_epochs) {
            delete e.second;
        }

        delete m_buffer;

        for (auto e : m_versions) {
            delete e;
        }
    }

    int insert(const R &rec) {
        return internal_append(rec, false);
    }

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

    std::future<std::vector<R>> query(void *parms) {
        return schedule_query(parms);
    }

    size_t get_record_count() {
        auto epoch = get_active_epoch_protected();
        auto t =  epoch->get_buffer().get_record_count() + epoch->get_structure()->get_record_count();
        epoch->end_job();

        return t;
    }

    size_t get_tombstone_count() {
        auto epoch = get_active_epoch_protected();
        auto t = epoch->get_buffer().get_tombstone_count() + epoch->get_structure()->get_tombstone_count();
        epoch->end_job();

        return t;
    }

    size_t get_height() {
        auto epoch = get_active_epoch_protected();
        auto t = epoch->get_structure()->get_height();
        epoch->end_job();

        return t;
    }

    size_t get_memory_usage() {
        auto epoch = get_active_epoch_protected();
        auto t= epoch->get_buffer().get_memory_usage() + epoch->get_structure()->get_memory_usage();
        epoch->end_job();

        return t;
    }

    size_t get_aux_memory_usage() {
        auto epoch = get_active_epoch_protected();
        auto t = epoch->get_buffer().get_aux_memory_usage() + epoch->get_structure()->get_aux_memory_usage();
        epoch->end_job();

        return t;
    }

    size_t get_buffer_capacity() {
        return m_buffer->get_capacity();
    }
    
    Shard *create_static_structure(bool await_reconstruction_completion=false) {
        if (await_reconstruction_completion) {
            await_next_epoch();
        }

        auto epoch = get_active_epoch_protected();
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

        epoch->end_job();
        return flattened;
    }

    /*
     * If the current epoch is *not* the newest one, then wait for
     * the newest one to become available. Otherwise, returns immediately.
     */
    void await_next_epoch() {
        while (m_current_epoch.load() != m_newest_epoch.load()) {
            std::unique_lock<std::mutex> lk(m_epoch_cv_lk);
            m_epoch_cv.wait(lk);
        }

        return;
    }

    /*
     * Mostly exposed for unit-testing purposes. Verifies that the current
     * active version of the ExtensionStructure doesn't violate the maximum
     * tombstone proportion invariant. 
     */
    bool validate_tombstone_proportion() {
        auto epoch = get_active_epoch_protected();
        auto t = epoch->get_structure()->validate_tombstone_proportion();
        epoch->end_job();
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

    std::atomic<size_t> m_current_epoch;
    std::atomic<size_t> m_newest_epoch;
    std::unordered_map<size_t, _Epoch *> m_epochs;

    std::condition_variable m_epoch_cv;
    std::mutex m_epoch_cv_lk;

    std::mutex m_epoch_transition_lk;
    std::shared_mutex m_epoch_retire_lk;

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

            /* 
             * the reconstruction process calls end_job(), 
             * so we must start one before calling it
             */
            epoch->start_job();

            m_sched.schedule_job(reconstruction, 0, args, RECONSTRUCTION);

            /* wait for compaction completion */
            wait.get();

            /* get a new batch of compactions to perform, if needed */
            compactions = structure->get_compaction_tasks();
        }
    }

    _Epoch *get_active_epoch() {
        return m_epochs[m_current_epoch.load()];
    }

    _Epoch *get_active_epoch_protected() {
        m_epoch_retire_lk.lock_shared();
        m_struct_lock.lock();
        auto cur_epoch = m_current_epoch.load();
        m_epochs[cur_epoch]->start_job();
        m_struct_lock.unlock();
        m_epoch_retire_lk.unlock_shared();

        return m_epochs[cur_epoch];
    }

    void advance_epoch(size_t buffer_head) {

        m_epoch_transition_lk.lock();

        size_t new_epoch_num = m_newest_epoch.load();
        size_t old_epoch_num = m_current_epoch.load();
        assert(new_epoch_num != old_epoch_num);

        _Epoch *new_epoch = m_epochs[new_epoch_num];
        _Epoch *old_epoch = m_epochs[old_epoch_num];

        /*
         * Verify the tombstone invariant within the epoch's structure, this
         * may require scheduling additional reconstructions.
         *
         * FIXME: having this inside the lock is going to TANK
         * insertion performance.
         */
        enforce_delete_invariant(new_epoch);

        // FIXME: this may currently block because there isn't any
        // query preemption yet. At this point, we'd need to either
        // 1) wait for all queries on the old_head to finish
        // 2) kill all queries on the old_head
        // 3) somehow migrate all queries on the old_head to the new
        //    version
        while (!new_epoch->advance_buffer_head(buffer_head)) {
            _mm_pause();
        }

        m_current_epoch.fetch_add(1);
        old_epoch->set_inactive();
        m_epoch_transition_lk.unlock();

        /* notify any blocking threads that the new epoch is available */
        m_epoch_cv_lk.lock();
        m_epoch_cv.notify_all();
        m_epoch_cv_lk.unlock();

        retire_epoch(old_epoch);
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
        m_newest_epoch.fetch_add(1);
        auto new_epoch = get_active_epoch()->clone(m_newest_epoch.load());
        std::unique_lock<std::mutex> m_struct_lock;
        m_versions.insert(new_epoch->get_structure());
        m_epochs.insert({m_newest_epoch.load(), new_epoch});
        m_struct_lock.release();

        return new_epoch;
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

        do {
            if (epoch->retirable()) {
                m_epoch_retire_lk.lock();
                if (!epoch->retirable()) {
                    m_epoch_retire_lk.unlock();
                    continue;
                }
                break;
            }
        } while (true);

        /* remove epoch from the framework's map */
        m_epochs.erase(epoch->get_epoch_number());

        /*
         * The epoch's destructor will handle releasing
         * all the references it holds
         */ 
        delete epoch;
        m_epoch_retire_lk.unlock();

        /* NOTE: the BufferView mechanism handles freeing unused buffer space */

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

        args->epoch->end_job();
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

        auto buffer = args->epoch->get_buffer();
        auto vers = args->epoch->get_structure();
        void *parms = args->query_parms;

        /* Get the buffer query states */
        void *buffer_state = Q::get_buffer_query_state(std::move(buffer), parms);

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

            query_results[i] = std::move(filter_deletes(local_results, shid, vers)); 

            if constexpr (Q::EARLY_ABORT) {
                if (query_results[i].size() > 0) break;
            }
        }

        auto result = Q::merge(query_results, parms);
        args->result_set.set_value(std::move(result));

        args->epoch->end_job();

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
        epoch->start_job();

        ReconstructionArgs<R, S, Q, L> *args = new ReconstructionArgs<R, S, Q, L>();
        args->epoch = epoch;
        args->merges = epoch->get_structure()->get_reconstruction_tasks(m_buffer->get_high_watermark());
        args->extension = this;
        args->compaction = false;
        /* NOTE: args is deleted by the reconstruction job, so shouldn't be freed here */

        m_sched.schedule_job(reconstruction, 0, args, RECONSTRUCTION);
    }

    std::future<std::vector<R>> schedule_query(void *query_parms) {
        auto epoch = get_active_epoch_protected();
        
        QueryArgs<R, S, Q, L> *args = new QueryArgs<R, S, Q, L>();
        args->epoch = epoch;
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

    static std::vector<Wrapped<R>> filter_deletes(std::vector<Wrapped<R>> &records, ShardID shid, Structure *vers) {
        if constexpr (!Q::SKIP_DELETE_FILTER) {
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

};
}

