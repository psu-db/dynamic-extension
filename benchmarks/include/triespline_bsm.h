#pragma once

#include <cstdlib>
#include <vector>
#include "ts/builder.h"

template <typename K, typename V, size_t E=1024>
class BSMTrieSpline {
private:
    typedef std::pair<K, V> R;

public:
    struct RangeQueryParameters {
        K lower_bound;
        K upper_bound;
    };

public:
    static BSMTrieSpline *build(std::vector<R> &records) {
        std::sort(records.begin(), records.end());
        return new BSMTrieSpline(records);
    }

    static BSMTrieSpline *build_presorted(std::vector<R> &records) {
        return new BSMTrieSpline(records);
    }

    std::vector<R> unbuild() {
        return std::move(m_data);
    }

    std::vector<R> query(void *q) {
        std::vector<R> rs;

        /* return an empty result set if q is invalid */
        if (q == nullptr) {
            return rs;
        }

        auto parms = (BSMTrieSpline::RangeQueryParameters*) q;

        size_t idx = lower_bound(parms->lower_bound);

        while (idx < m_data.size() && m_data[idx].first < parms->upper_bound) {
            rs.emplace_back(m_data[idx++]);
        }

        return std::move(rs);
    }

    std::vector<R> query_merge(std::vector<R> &rsa, std::vector<R> &rsb) {
        rsa.insert(rsa.end(), rsb.begin(), rsb.end());
        return std::move(rsa);
    }

    size_t record_count() {
        return m_data.size();
    }


    ~BSMTrieSpline() = default;


private:
    std::vector<R> m_data;
    K m_max_key;
    K m_min_key;
    ts::TrieSpline<K> m_ts;

    BSMTrieSpline(std::vector<R> &records) {
        m_data = std::move(records);
        m_min_key = m_data[0].first;
        m_max_key = m_data[m_data.size() - 1].first;

        auto bldr = ts::Builder<K>(m_min_key, m_max_key, E);
        for (size_t i=0; i<m_data.size(); i++) {
            bldr.AddKey(m_data[i].first);
        }

        if (m_data.size() > 1) {
            m_ts = bldr.Finalize();
        }
    }

    size_t lower_bound(K key) {
        if (m_data.size() == 0) {
            return 1;
        } else if (m_data.size() == 1) {
            if (m_data[0].first < key) {
                return 1;
            } else {
                return 0;
            }
        }

        auto bound = m_ts.GetSearchBound(key);
        size_t idx = bound.begin;

        if (idx >= m_data.size()) {
            return m_data.size();
        }

        // If the region to search is less than some pre-specified
        // amount, perform a linear scan to locate the record.
        if (bound.end - bound.begin < 256) {
            while (idx < bound.end && m_data[idx].first < key) {
                idx++;
            }
        } else {
            // Otherwise, perform a binary search
            idx = bound.begin;
            size_t max = bound.end;

            while (idx < max) {
                size_t mid = (idx + max) / 2;
                if (key > m_data[mid].first) {
                    idx = mid + 1;
                } else {
                    max = mid;
                }
            }
        }

        if (idx == m_data.size()) {
            return m_data.size();
        }

        if (m_data[idx].first > key && idx > 0 && m_data[idx-1].first <= key) {
            return idx-1;
        }

        return idx;
    }
};
