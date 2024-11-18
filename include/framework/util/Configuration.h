/*
 * include/framework/util/Configuration.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <cstdlib>
#include <utility>

namespace de {

enum class LayoutPolicy { LEVELING, TEIRING, BSM };

enum class DeletePolicy { TOMBSTONE, TAGGING };

} // namespace de
