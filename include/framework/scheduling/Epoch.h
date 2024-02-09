/*
 * include/framework/scheduling/Epoch.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <condition_variable>
#include <mutex>

#include "framework/structure/MutableBuffer.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/structure/BufferView.h"

namespace de {


template <RecordInterface R, ShardInterface<R> S, QueryInterface<R, S> Q, LayoutPolicy L>
class Epoch {
private:
    typedef MutableBuffer<R> Buffer;
    typedef ExtensionStructure<R, S, Q, L> Structure;
    typedef BufferView<R> BufView;
public:
    Epoch(size_t number=0)
        : m_buffer(nullptr)
        , m_structure(nullptr)
        , m_active_merge(false)
        , m_epoch_number(number)
        , m_buffer_head(0)
    {}

    Epoch(size_t number, Structure *structure, Buffer *buff, size_t head)
        : m_buffer(buff)
        , m_structure(structure)
        , m_active_merge(false)
        , m_epoch_number(number)
        , m_buffer_head(head)
    {
        structure->take_reference();
    }

    ~Epoch() {
        if (m_structure) {
            m_structure->release_reference();
        }
    }

    /*
     * Epochs are *not* copyable or movable. Only one can exist, and all users
     * of it work with pointers
     */
    Epoch(const Epoch&) = delete;
    Epoch(Epoch&&) = delete;
    Epoch &operator=(const Epoch&) = delete;
    Epoch &operator=(Epoch&&) = delete;

    size_t get_epoch_number() {
        return m_epoch_number;
    }

    Structure *get_structure() {
        return m_structure;
    }

    BufView get_buffer() {
        return m_buffer->get_buffer_view(m_buffer_head);
    }

    /*
     * Returns a new Epoch object that is a copy of this one. The new object
     * will also contain a copy of the m_structure, rather than a reference to
     * the same one. The epoch number of the new epoch will be set to the
     * provided argument.
     */
    Epoch *clone(size_t number) {
        std::unique_lock<std::mutex> m_buffer_lock;
        auto epoch = new Epoch(number);
        epoch->m_buffer = m_buffer;
        epoch->m_buffer_head = m_buffer_head;

        if (m_structure) {
            epoch->m_structure = m_structure->copy();
            /* the copy routine returns a structure with 0 references */
            epoch->m_structure->take_reference();
        }

        return epoch;
    }

    /*
     * Check if a merge can be started from this Epoch. At present, without
     * concurrent merging, this simply checks if there is currently a scheduled
     * merge based on this Epoch. If there is, returns false. If there isn't,
     * return true and set a flag indicating that there is an active merge.
     */
    bool prepare_reconstruction() {
        auto old = m_active_merge.load();
        if (old) {
            return false;
        }

        // FIXME: this needs cleaned up
        while (!m_active_merge.compare_exchange_strong(old, true)) {
            old = m_active_merge.load();
            if (old) {
                return false;
            }
        }

        return true;
    }

    bool advance_buffer_head(size_t head) {
        m_buffer_head = head;
        return m_buffer->advance_head(m_buffer_head);
    }

private:
    Structure *m_structure;
    Buffer *m_buffer;

    std::mutex m_buffer_lock;
    std::atomic<bool> m_active_merge;

    /*
     * The number of currently active jobs
     * (queries/merges) operating on this
     * epoch. An epoch can only be retired
     * when this number is 0.
     */
    size_t m_epoch_number;
    size_t m_buffer_head;
};
}
