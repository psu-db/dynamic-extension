/*
 * include/framework/DynamicExtension.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <atomic>
#include <numeric>
#include <cstdio>
#include <vector>
#include <set>

#include "framework/structure/MutableBuffer.h"
#include "framework/structure/InternalLevel.h"
#include "framework/interface/Shard.h"
#include "framework/interface/Query.h"
#include "framework/interface/Record.h"
#include "framework/interface/Query.h"
#include "framework/interface/Scheduler.h"
#include "framework/structure/ExtensionStructure.h"

#include "framework/util/Configuration.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "framework/scheduling/SerialScheduler.h"
#include "framework/scheduling/Epoch.h"

#include "psu-util/timer.h"
#include "psu-ds/Alias.h"

namespace de {

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L=LayoutPolicy::TEIRING, 
          DeletePolicy D=DeletePolicy::TAGGING, SchedulerInterface SCHED=FIFOScheduler>
class DynamicExtension {
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;
    typedef ExtensionStructure<R, S, Q, L> Structure;
    typedef Epoch<R, S, Q, L> _Epoch;
    typedef BufferView<R, Q> BufView;

public:
    DynamicExtension(size_t buffer_cap, size_t scale_factor, double max_delete_prop, size_t memory_budget=0, 
                     size_t thread_cnt=16)
        : m_scale_factor(scale_factor)
        , m_max_delete_prop(max_delete_prop)
        , m_sched(memory_budget, thread_cnt)
        , m_buffer_capacity(buffer_cap)
        , m_buffer_delete_capacity(max_delete_prop*buffer_cap)
    {
        auto buf = new Buffer(m_buffer_capacity, m_buffer_delete_capacity);
        auto vers = new Structure(m_buffer_capacity, m_scale_factor, m_max_delete_prop);
        auto epoch = new _Epoch(vers, buf);

        m_buffers.insert(buf);
        m_versions.insert(vers);
        m_epochs.insert({0, epoch});
    }

    ~DynamicExtension() {
        for (auto e : m_epochs) {
            delete e.second;
        }

        for (auto e : m_buffers) {
            delete e;
        }

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
        if constexpr (D == DeletePolicy::TAGGING) {
            static_assert(std::same_as<SCHED, SerialScheduler>, "Tagging is only supported in single-threaded operation");
            BufView buffers = get_active_epoch()->get_buffer_view();

            if (get_active_epoch()->get_structure()->tagged_delete(rec)) {
                return 1;
            }

            /*
             * the buffer will take the longest amount of time, and 
             * probably has the lowest probability of having the record,
             * so we'll check it last.
             */
            return buffers.delete_record(rec);
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
        auto t =  epoch->get_buffer_view().get_record_count() + epoch->get_structure()->get_record_count();
        epoch->end_job();

        return t;
    }

    size_t get_tombstone_count() {
        auto epoch = get_active_epoch_protected();
        auto t = epoch->get_buffer_view().get_tombstone_count() + epoch->get_structure()->get_tombstone_count();
        epoch->end_job();

        return t;
    }

    size_t get_height() {
        return get_active_epoch()->get_structure()->get_height();
    }

    size_t get_memory_usage() {
        auto epoch = get_active_epoch_protected();
        auto t= epoch->get_buffer_view().get_memory_usage() + epoch->get_structure()->get_memory_usage();
        epoch->end_job();

        return t;
    }

    size_t get_aux_memory_usage() {
        auto epoch = get_active_epoch_protected();
        auto t = epoch->get_buffer_view().get_aux_memory_usage() + epoch->get_structure()->get_aux_memory_usage();
        epoch->end_job();

        return t;
    }

    size_t get_buffer_capacity() {
        return m_buffer_capacity;
    }
    
    Shard *create_static_structure(bool await_merge_completion=false) {
        if (await_merge_completion) {
            await_next_epoch();
        }

        auto epoch = get_active_epoch_protected();
        auto bv = epoch->get_buffer_view();

        auto vers = epoch->get_structure();
        std::vector<Shard *> shards;

        if (vers->get_levels().size() > 0) {
            for (int i=vers->get_levels().size() - 1; i>= 0; i--) {
                if (vers->get_levels()[i]) {
                    shards.emplace_back(vers->get_levels()[i]->get_merged_shard());
                }
            }
        }

        // FIXME: With an interface adjustment, this could be done in
        //        one call, rather than a loop.
        for (size_t i=bv.size() - 1; i>=0; i--) {
            shards.emplace_back(new S(bv.get_buffers()[i]));
        }

        Shard *shards_array[shards.size()];

        size_t j = 0;
        for (size_t i=0; i<shards.size(); i++) {
            if (shards[i]) {
                shards_array[j++] = shards[i];
            }
        }

        Shard *flattened = new S(shards_array, j);

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
            std::unique_lock<std::mutex> m_epoch_cv_lk;
            m_epoch_cv.wait(m_epoch_cv_lk);
        }

        return;
    }

    /*
     * Mostly exposed for unit-testing purposes. Verifies that the current
     * active version of the ExtensionStructure doesn't violate the maximum
     * tombstone proportion invariant. 
     */
    bool validate_tombstone_proportion() {
        return get_active_epoch()->get_structure()->validate_tombstone_proportion();
    }

private:
    SCHED m_sched;

    std::mutex m_struct_lock;
    std::set<Buffer*> m_buffers;
    std::set<Structure *> m_versions;

    std::atomic<size_t> m_current_epoch;
    std::atomic<size_t> m_newest_epoch;
    std::unordered_map<size_t, _Epoch *> m_epochs;

    std::condition_variable m_epoch_cv;
    std::mutex m_epoch_cv_lk;

    size_t m_scale_factor;
    double m_max_delete_prop;
    size_t m_buffer_capacity;
    size_t m_buffer_delete_capacity;

    _Epoch *get_active_epoch() {
        return m_epochs[m_current_epoch.load()];
    }

    _Epoch *get_active_epoch_protected() {
        m_epochs[m_current_epoch.load()]->start_job();
        return m_epochs[m_current_epoch.load()];
    }

    void advance_epoch() {
        size_t new_epoch_num = m_newest_epoch.load();
        _Epoch *new_epoch = m_epochs[new_epoch_num];
        _Epoch *old_epoch = m_epochs[m_current_epoch.load()];

        /*
         * Update the new Epoch to contain the buffers from the old one 
         * that it doesn't currently have
         */
        size_t old_buffer_cnt = new_epoch->clear_buffers();
        for (size_t i=old_buffer_cnt; i<old_epoch->get_buffers().size(); i++) { 
            new_epoch->add_buffer(old_epoch->get_buffers()[i]);
        }
        m_current_epoch.fetch_add(1);

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
         * only one merge will be able to trigger at a time. If that condition
         * is violated, it is possible that this code will clone a retired
         * epoch.
         */
        auto new_epoch = get_active_epoch()->clone();
        std::unique_lock<std::mutex> m_struct_lock;
        m_versions.insert(new_epoch->get_structure());
        m_newest_epoch.fetch_add(1);
        m_epochs.insert({m_newest_epoch.load(), new_epoch});
        m_struct_lock.release();

        return new_epoch;
    }

    /*
     * Add a new empty buffer to the specified epoch. This is intended to be used
     * when a merge is triggered, to allow for inserts to be sustained in the new
     * buffer while a new epoch is being created in the background. Returns a
     * pointer to the newly created buffer.
     */
    Buffer *add_empty_buffer(_Epoch *epoch) {
        auto new_buffer = new Buffer(m_buffer_capacity, m_buffer_delete_capacity); 

        std::unique_lock<std::mutex> m_struct_lock;
        m_buffers.insert(new_buffer);
        m_struct_lock.release();

        epoch->add_buffer(new_buffer);
        return new_buffer;
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
        while (!epoch->retirable()) 
            ;

        /*
         * The epoch's destructor will handle releasing
         * all the references it holds
         */ 
        delete epoch;

        /*
         * Following the epoch's destruction, any buffers
         * or structures with no remaining references can
         * be safely freed.
         */
        std::unique_lock<std::mutex> lock(m_struct_lock);
        for (auto buf : m_buffers) {
            if (buf->get_reference_count() == 0) {
                m_buffers.erase(buf);
                delete buf;
            }
        }

        for (auto vers : m_versions) {
            if (vers->get_reference_count() == 0) {
                m_versions.erase(vers);
                delete vers;
            }
        }
    }

    static void merge(void *arguments) {
        MergeArgs<R, S, Q, L> *args = (MergeArgs<R, S, Q, L> *) arguments;

        Structure *vers = args->epoch->get_structure();
        // FIXME: with an improved shard interface, multiple full buffers
        //        could be merged at once here.
        Buffer *buff = (Buffer *) args->epoch->get_buffers()[0];

        for (ssize_t i=args->merges.size() - 1; i>=0; i--) {
            vers->merge_levels(args->merges[i].second, args->merges[i].first);
        }

        vers->merge_buffer(buff);

        args->epoch->end_job();
        args->result.set_value(true);

        ((DynamicExtension *) args->extension)->advance_epoch();
        
        delete args;
    }
    
    static void async_query(void *arguments) {
        QueryArgs<R, S, Q, L> *args = (QueryArgs<R, S, Q, L> *) arguments;

        auto buffers = args->epoch->get_buffer_view();
        auto vers = args->epoch->get_structure();
        void *parms = args->query_parms;

        /* Get the buffer query states */
        std::vector<void *> buffer_states = buffers.get_query_states(parms);

        /* Get the shard query states */
        std::vector<std::pair<ShardID, Shard*>> shards;
        std::vector<void *> states = vers->get_query_states(shards, parms);

        Q::process_query_states(parms, states, buffer_states);

        std::vector<std::vector<Wrapped<R>>> query_results(shards.size() + buffer_states.size());
        for (size_t i=0; i<query_results.size(); i++) {
            std::vector<Wrapped<R>> local_results = (i < buffer_states.size()) 
                                              ? Q::buffer_query(buffers.get_buffers()[i], buffer_states[i], parms)
                                              : Q::query(shards[i - buffer_states.size()].second, 
                                                         states[i - buffer_states.size()], parms);
            ShardID shid = (i < buffer_states.size()) ? INVALID_SHID : shards[i - buffer_states.size()].first;
            query_results[i] = std::move(filter_deletes(local_results, shid, buffers, vers)); 

            if constexpr (Q::EARLY_ABORT) {
                if (query_results[i].size() > 0) break;
            }
        }

        auto result = Q::merge(query_results, parms);
        args->result_set.set_value(std::move(result));

        args->epoch->end_job();

        for (size_t i=0; i<buffer_states.size(); i++) {
            Q::delete_buffer_query_state(buffer_states[i]);
        }

        for (size_t i=0; i<states.size(); i++) {
            Q::delete_query_state(states[i]);
        }

        delete args;
    }

    void schedule_merge() {
        auto epoch = create_new_epoch();
        epoch->start_job();

        MergeArgs<R, S, Q, L> *args = new MergeArgs<R, S, Q, L>();
        args->epoch = epoch;
        // FIXME: all full buffers can be merged at this point--but that requires
        //        retooling the shard interface a bit to do efficiently.
        args->merges = epoch->get_structure()->get_merge_tasks(epoch->get_buffers()[0]->get_record_count());
        args->extension = this;
        m_sched.schedule_job(merge, 0, args);
    }

    std::future<std::vector<R>> schedule_query(void *query_parms) {
        auto epoch = get_active_epoch_protected();
        
        QueryArgs<R, S, Q, L> *args = new QueryArgs<R, S, Q, L>();
        args->epoch = epoch;
        args->query_parms = query_parms;
        auto result = args->result_set.get_future();

        m_sched.schedule_job(async_query, 0, args);

        return result;
    }

    int internal_append(const R &rec, bool ts) {
        Buffer *buffer = nullptr;
        do {
            auto epoch = get_active_epoch_protected();
            buffer = epoch->get_active_buffer();
            
            /* if the buffer is full, schedule a merge and add a new empty buffer */
            if (buffer->is_full()) {
                // FIXME: possible race here--two identical merges could be scheduled
                auto vers = epoch->get_structure();
                schedule_merge();
                buffer = add_empty_buffer(epoch);
            }
            // FIXME: not exactly the best spot for this
            epoch->end_job();
        } while(!buffer->append(rec, ts));

        /* internal append should always succeed, eventually */
        return 1;
    }

    static std::vector<Wrapped<R>> filter_deletes(std::vector<Wrapped<R>> &records, ShardID shid, 
                                                  BufView &buffers, Structure *vers) {
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

            if (buffers.check_tombstone(rec.rec)) {
                continue;
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
};
}

