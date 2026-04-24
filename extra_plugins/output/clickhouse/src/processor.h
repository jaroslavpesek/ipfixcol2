/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Processor worker — parses IPFIX records and builds ClickHouse blocks in parallel
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#pragma once

#include "block.h"
#include "bounded_queue.h"
#include "column.h"
#include "common.h"
#include "recparser.h"
#include "stats.h"
#include "syncqueue.h"
#include "worker.h"

#include <ipfixcol2.h>
#include <libfds.h>

#include <ctime>
#include <memory>
#include <variant>
#include <vector>

// ---------------------------------------------------------------------------
// Work-item types enqueued by the dispatcher to each Processor worker.
// ---------------------------------------------------------------------------

/** Per-data-record descriptor stored inside a WorkItem. */
struct DrecInfo {
    uint16_t tmpl_id;
    uint32_t offset; ///< Byte offset into WorkItem::buf where this drec's data starts.
    uint16_t size;
};

/**
 * One IPFIX message worth of data records, with their field bytes deep-copied into buf.
 * Ownership of buf is with this struct (unique_ptr).
 */
struct WorkItem {
    std::unique_ptr<uint8_t[]> buf;
    std::vector<DrecInfo>      drecs;
    const ipx_session         *session;
    uint32_t                   odid;
};

/**
 * Delivers a deep-copied template to the worker so it can create a RecParser.
 * Ownership of tmplt is transferred to the worker (it calls fds_template_destroy).
 */
struct TemplateRegister {
    const ipx_session *session;
    uint32_t           odid;
    uint16_t           tmpl_id; // informational; also in tmplt->id
    fds_template      *tmplt;   // owned; worker must call fds_template_destroy
};

/** Session closed: worker must purge all parsers for this session. */
struct SessionClose {
    const ipx_session *session;
};

/** Nudge worker to flush a partially-filled block even if below threshold. */
struct PeriodicFlush {};

/** Sentinel: no more work; worker must flush and exit. */
struct Stop {};

using ProcItem = std::variant<WorkItem, TemplateRegister, SessionClose, PeriodicFlush, Stop>;

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

/**
 * @class Processor
 * @brief Worker thread that consumes ProcItems from its BoundedQueue, parses IPFIX data
 *        records, and writes rows into ClickHouse Block objects.
 *
 * Each Processor owns:
 *  - a RecParserManager (per-session/odid/template parser cache)
 *  - a current Block being filled (pulled from shared m_avail_blocks)
 *  - its queue (BoundedQueue<ProcItem>)
 *
 * Filled blocks are pushed to the shared m_filled_blocks queue consumed by Inserter threads.
 */
class Processor : public Worker {
public:
    Processor(
        int                         id,
        Logger                      logger,
        const std::vector<Column>  &columns,
        const fds_iemgr_t          *iemgr,
        Stats                      &stats,
        SyncQueue<Block *>         &avail_blocks,
        SyncQueue<Block *>         &filled_blocks,
        bool                        biflow_autoignore,
        uint64_t                    block_insert_threshold,
        uint64_t                    block_insert_max_delay_secs,
        BoundedQueue<ProcItem>     &queue);

private:
    int                        m_id;
    Logger                     m_logger;
    const std::vector<Column> &m_columns;
    const fds_iemgr_t         *m_iemgr;
    Stats                     &m_stats;
    SyncQueue<Block *>        &m_avail_blocks;
    SyncQueue<Block *>        &m_filled_blocks;
    bool                       m_biflow_autoignore;
    uint64_t                   m_block_insert_threshold;
    uint64_t                   m_block_insert_max_delay_secs;
    BoundedQueue<ProcItem>    &m_queue;

    std::unique_ptr<RecParserManager> m_rec_parsers;
    Block                            *m_current_block = nullptr;
    std::time_t                       m_last_flush_time = 0;

    void run() override;

    void handle(WorkItem &wi);
    void handle(TemplateRegister &tr);
    void handle(SessionClose &sc);
    void handle(PeriodicFlush &pf);

    void extract_values(RecParser &parser, Block &block, bool rev, uint32_t odid);

    /** Flush current block to m_filled_blocks if rows >= threshold or force=true. */
    void maybe_flush(bool force);

    /** Acquire a block from the pool if we don't have one yet. */
    void ensure_block();
};
