/*
 * tests/de_tier_tag.cpp
 *
 * Unit tests for Dynamic Extension Framework
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#include <set>
#include <random>
#include <algorithm>

#include "testing.h"
#include "framework/DynamicExtension.h"
#include "framework/scheduling/SerialScheduler.h"
#include "shard/ISAMTree.h"
#include "query/rangequery.h"

#include <check.h>
using namespace de;

typedef DynamicExtension<Rec, ISAMTree<Rec>, rq::Query<ISAMTree<Rec>, Rec>, LayoutPolicy::TEIRING, DeletePolicy::TAGGING, SerialScheduler> DE;

#include "dynamic_extension_tests.inc"
