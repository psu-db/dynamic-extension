/*
 * tests/dynamic_extension_tests.cpp
 *
 * Unit tests for Dynamic Extension Framework
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#include <set>
#include <random>
#include <algorithm>

#include "testing.h"
#include "framework/DynamicExtension.h"
#include "shard/WIRS.h"

#include <check.h>
using namespace de;

typedef DynamicExtension<WRec, WIRS<WRec>, WIRSQuery<WRec>, LayoutPolicy::LEVELING, DeletePolicy::TAGGING> DE;

#include "dynamic_extension_tests.inc"
