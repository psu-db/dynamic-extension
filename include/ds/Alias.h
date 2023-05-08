/*
 * include/ds/Alias.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <gsl/gsl_rng.h>
#include <vector>

namespace de {

/*
 * An implementation of Walker's Alias Structure for Weighted Set Sampling. Requires
 * that the input weight vector is already normalized.
 */
class Alias {
public:
    Alias(const std::vector<double>& weights)
    : m_alias(weights.size()), m_cutoff(weights.size()) {
        size_t n = weights.size();
        auto overfull = std::vector<size_t>();
        auto underfull = std::vector<size_t>();
        overfull.reserve(n);
        underfull.reserve(n);

        // initialize the probability_table with n*p(i) as well as the overfull and
        // underfull lists.
        for (size_t i = 0; i < n; i++) {
            m_cutoff[i] = (double) n * weights[i];
            if (m_cutoff[i] > 1) {
                overfull.emplace_back(i);
            } else if (m_cutoff[i] < 1) {
                underfull.emplace_back(i);
            } else {
                m_alias[i] = i;
            }
        }

        while (overfull.size() > 0 && underfull.size() > 0) {
            auto i = overfull.back(); overfull.pop_back();
            auto j = underfull.back(); underfull.pop_back();

            m_alias[j] = i;
            m_cutoff[i] = m_cutoff[i] + m_cutoff[j] - 1.0;

            if (m_cutoff[i] > 1.0) {
                overfull.emplace_back(i);
            } else if (m_cutoff[i] < 1.0) {
                underfull.emplace_back(i);
            }
        }
    }

    size_t get(const gsl_rng* rng) {
        double coin1 = gsl_rng_uniform(rng);
        double coin2 = gsl_rng_uniform(rng);

        size_t k = ((double) m_alias.size()) * coin1;
        return coin2 < m_cutoff[k] ? k : m_alias[k];
    }

private:
    std::vector<size_t> m_alias;
    std::vector<double> m_cutoff;
};

}
