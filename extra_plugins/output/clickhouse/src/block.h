/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Block representation
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#pragma once

#include <vector>
#include "clickhouse.h"

struct Block {
    std::vector<std::shared_ptr<clickhouse::Column>> columns;
    // One scratch column per column slot; non-null only for Array(T) columns.
    // Reused across rows to avoid per-row allocations in write_list_to_column.
    std::vector<std::shared_ptr<clickhouse::Column>> list_scratch;
    clickhouse::Block block;
    unsigned int rows = 0;
};
