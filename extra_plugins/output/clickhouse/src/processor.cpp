/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Processor worker implementation
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "processor.h"
#include "datatype.h"

#include <cassert>
#include <ctime>

Processor::Processor(
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
    BoundedQueue<ProcItem>     &queue)
    : m_id(id)
    , m_logger(logger)
    , m_columns(columns)
    , m_iemgr(iemgr)
    , m_stats(stats)
    , m_avail_blocks(avail_blocks)
    , m_filled_blocks(filled_blocks)
    , m_biflow_autoignore(biflow_autoignore)
    , m_block_insert_threshold(block_insert_threshold)
    , m_block_insert_max_delay_secs(block_insert_max_delay_secs)
    , m_queue(queue)
    , m_rec_parsers(std::make_unique<RecParserManager>(columns, biflow_autoignore))
{}

void Processor::run()
{
    auto timeout = std::chrono::seconds(m_block_insert_max_delay_secs);

    while (true) {
        auto item_opt = m_queue.get(timeout);

        if (!item_opt.has_value()) {
            // Timeout — flush partial block so records don't sit forever
            maybe_flush(true);
            continue;
        }

        bool done = false;
        std::visit([this, &done](auto &v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, WorkItem>) {
                handle(v);
            } else if constexpr (std::is_same_v<T, TemplateRegister>) {
                handle(v);
            } else if constexpr (std::is_same_v<T, SessionClose>) {
                handle(v);
            } else if constexpr (std::is_same_v<T, PeriodicFlush>) {
                handle(v);
            } else if constexpr (std::is_same_v<T, Stop>) {
                done = true;
            }
        }, *item_opt);

        if (done) {
            maybe_flush(true);
            return;
        }
    }
}

void Processor::handle(WorkItem &wi)
{
    m_rec_parsers->select_session(wi.session);
    m_rec_parsers->select_odid(wi.odid);

    for (const auto &drec_info : wi.drecs) {
        RecParser *parser = m_rec_parsers->get_parser_by_id(drec_info.tmpl_id);
        if (!parser) {
            // TemplateRegister should always arrive before the WorkItem referencing it.
            m_logger.warning("[Processor %d] parser not found for tmpl_id=%u, skipping record", m_id, drec_info.tmpl_id);
            continue;
        }

        fds_drec drec{};
        drec.data  = wi.buf.get() + drec_info.offset;
        drec.size  = drec_info.size;
        drec.tmplt = parser->tmplt();
        drec.snap  = nullptr; // not needed for plain data record iteration

        parser->parse_record(drec);

        ensure_block();

        if (!parser->skip_fwd()) {
            extract_values(*parser, *m_current_block, false, wi.odid);
            m_stats.add_rows(1);
        }
        if (!parser->skip_rev()) {
            extract_values(*parser, *m_current_block, true, wi.odid);
            m_stats.add_rows(1);
        }

        maybe_flush(false);
    }
}

void Processor::handle(TemplateRegister &tr)
{
    // register_parser deep-copies the template into RecParser and then destroys tr.tmplt.
    m_rec_parsers->register_parser(tr.session, tr.odid, tr.tmplt);
    tr.tmplt = nullptr;
}

void Processor::handle(SessionClose &sc)
{
    m_rec_parsers->delete_session(sc.session);
}

void Processor::handle(PeriodicFlush &)
{
    maybe_flush(true);
}

void Processor::extract_values(RecParser &parser, Block &block, bool rev, uint32_t odid)
{
    std::size_t n_columns = m_columns.size();
    bool has_value;
    ValueVariant value;

    for (std::size_t i = 0; i < n_columns; i++) {
        if (m_columns[i].is_list) {
            fds_drec_field &field = parser.get_column(i, rev);
            if (field.data != nullptr) {
                try {
                    write_list_to_column(m_columns[i].datatype, m_columns[i].nullable,
                                         field, *block.columns[i], m_iemgr,
                                         block.list_scratch[i]);
                } catch (const ConversionError &err) {
                    m_logger.error("[Processor %d] List field conversion failed (field #%zu, \"%s\"): %s",
                                   m_id, i, m_columns[i].name.c_str(), err.what());
                    write_empty_list_to_column(*block.columns[i], block.list_scratch[i]);
                }
            } else {
                write_empty_list_to_column(*block.columns[i], block.list_scratch[i]);
            }
            continue;
        }

        has_value = false;

        if (m_columns[i].special == SpecialField::ODID) {
            value = odid;
            has_value = true;
        } else {
            fds_drec_field &field = parser.get_column(i, rev);
            if (field.data != nullptr) {
                try {
                    value = get_value(m_columns[i].datatype, field);
                    has_value = true;
                } catch (const ConversionError &err) {
                    m_logger.error("[Processor %d] Field conversion failed (field #%zu, \"%s\"): %s",
                                   m_id, i, m_columns[i].name.c_str(), err.what());
                }
            }
        }

        write_to_column(m_columns[i].datatype, m_columns[i].nullable,
                        *block.columns[i], has_value ? &value : nullptr);
    }

    block.rows++;
}

void Processor::maybe_flush(bool force)
{
    if (!m_current_block) return;
    if (m_current_block->rows == 0) return;

    time_t now = std::time(nullptr);
    bool thresh = m_current_block->rows >= m_block_insert_threshold;
    bool timeout = (m_last_flush_time > 0) && (uint64_t(now - m_last_flush_time) >= m_block_insert_max_delay_secs);

    if (force || thresh || timeout) {
        m_filled_blocks.put(m_current_block);
        m_current_block = nullptr;
        m_last_flush_time = now;
    }
}

void Processor::ensure_block()
{
    if (m_current_block) return;
    m_current_block = m_avail_blocks.get();
    if (m_last_flush_time == 0) {
        m_last_flush_time = std::time(nullptr);
    }
}
