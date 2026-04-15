/**
 * \file FlowConverter.cpp
 * \brief To Protobuf converter
 * \author Jaroslav Pesek
 * \date 2026
 */

#include "FlowConverter.hpp"

#include <arpa/inet.h>
#include <cstring>

namespace protobuf_kafka {

namespace {

inline bool
isUtf8Cont(uint8_t byte)
{
    return (byte & 0xC0U) == 0x80U;
}

/**
 * \brief Convert arbitrary bytes to valid UTF-8 by replacing invalid sequences with '?'
 */
void
sanitizeUtf8(const uint8_t* data, size_t size, std::string& out)
{
    out.clear();
    out.reserve(size);

    size_t i = 0;
    while (i < size) {
        const uint8_t c0 = data[i];
        if (c0 <= 0x7FU) {
            out.push_back(static_cast<char>(c0));
            ++i;
            continue;
        }

        size_t need = 0;
        if (c0 >= 0xC2U && c0 <= 0xDFU) {
            need = 1;
        } else if (c0 >= 0xE0U && c0 <= 0xEFU) {
            need = 2;
        } else if (c0 >= 0xF0U && c0 <= 0xF4U) {
            need = 3;
        } else {
            out.push_back('?');
            ++i;
            continue;
        }

        if (i + need >= size) {
            out.push_back('?');
            break;
        }

        bool ok = true;
        for (size_t j = 1; j <= need; ++j) {
            if (!isUtf8Cont(data[i + j])) {
                ok = false;
                break;
            }
        }

        if (ok && need == 2) {
            const uint8_t c1 = data[i + 1];
            if ((c0 == 0xE0U && c1 < 0xA0U) || (c0 == 0xEDU && c1 >= 0xA0U)) {
                ok = false;
            }
        } else if (ok && need == 3) {
            const uint8_t c1 = data[i + 1];
            if ((c0 == 0xF0U && c1 < 0x90U) || (c0 == 0xF4U && c1 > 0x8FU)) {
                ok = false;
            }
        }

        if (!ok) {
            out.push_back('?');
            ++i;
            continue;
        }

        out.append(reinterpret_cast<const char*>(data + i), need + 1);
        i += need + 1;
    }
}

inline void
appendVarint(std::string& out, uint64_t value)
{
    while (value >= 0x80U) {
        out.push_back(static_cast<char>((value & 0x7FU) | 0x80U));
        value >>= 7U;
    }
    out.push_back(static_cast<char>(value));
}

inline void
appendFixed32(std::string& out, uint32_t value)
{
    out.push_back(static_cast<char>(value & 0xFFU));
    out.push_back(static_cast<char>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 24U) & 0xFFU));
}

inline void
appendFixed64(std::string& out, uint64_t value)
{
    out.push_back(static_cast<char>(value & 0xFFU));
    out.push_back(static_cast<char>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 24U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 32U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 40U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 48U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 56U) & 0xFFU));
}

inline uint32_t
zigzag32(int32_t value)
{
    return static_cast<uint32_t>((value << 1) ^ (value >> 31));
}

inline uint64_t
zigzag64(int64_t value)
{
    return static_cast<uint64_t>((value << 1) ^ (value >> 63));
}

} // namespace

FlowConverter::FlowConverter(ProtoSchema& schema,
                             const TranslationTable& table,
                             PartitionMode mode)
    : m_table(table)
    , m_partition_mode(mode)
{
    (void)schema;
    m_buffer.reserve(4096);
    m_tmp_utf8.reserve(256);
    m_tmp_packed.reserve(512);
}

FlowConverter::~FlowConverter() = default;

bool
FlowConverter::convert(const fds_drec* rec,
                       const char** out_data,
                       size_t* out_len,
                       PartitionKey* partition_key,
                       uint32_t odid)
{
    m_buffer.clear();

    PartitionKey pk_local{};
    const bool need_partition_key = (m_partition_mode == PartitionMode::RSS && partition_key);

    struct fds_drec_iter it;
    fds_drec_iter_init(&it, const_cast<struct fds_drec*>(rec), 0);

    int rc = FDS_OK;
    while ((rc = fds_drec_iter_next(&it)) != FDS_EOC) {
        if (rc < 0 || it.field.info == nullptr) {
            continue;
        }

        const struct fds_tfield* info = it.field.info;
        MappingKey key;
        key.root_pen = info->en;
        key.root_id = info->id;

        const bool is_basic_list = (info->def != nullptr && info->def->data_type == FDS_ET_BASIC_LIST);
        if (is_basic_list) {
            struct fds_blist_iter list_it;
            fds_blist_iter_init(&list_it, &it.field, nullptr);
            const int list_rc = fds_blist_iter_next(&list_it);
            if (list_rc == FDS_OK && list_it.field.info != nullptr) {
                key.has_list_elem = true;
                key.list_pen = list_it.field.info->en;
                key.list_id = list_it.field.info->id;
            } else if (list_rc == FDS_ERR_FORMAT) {
                continue;
            }
        }

        const FieldEntry* entry = m_table.lookup(key);
        if (!entry && key.has_list_elem) {
            key.has_list_elem = false;
            key.list_pen = 0;
            key.list_id = 0;
            entry = m_table.lookup(key);
        }

        if (entry != nullptr) {
            const bool converted = entry->is_list
                ? setBasicListField(*entry, it.field)
                : setFieldValue(*entry, it.field.data, it.field.size);
            if (!converted) {
                continue;
            }
        }

        if (need_partition_key && info->en == IANA_PEN && !is_basic_list) {
            extractPartitionField(info->id, it.field.data, it.field.size, &pk_local);
        }
    }

    if (need_partition_key) {
        pk_local.valid = pk_local.has_flow_id ||
            (pk_local.src_ip != nullptr || pk_local.dst_ip != nullptr);
        *partition_key = pk_local;
    }

    // Emit ODID fields (filled from message context, not IPFIX data)
    const uint32_t be_odid = htonl(odid);
    for (const auto& entry : m_table.odid_entries()) {
        appendValue(entry, reinterpret_cast<const uint8_t*>(&be_odid), sizeof(be_odid), m_buffer, true);
    }

    *out_data = m_buffer.data();
    *out_len = m_buffer.size();
    return true;
}

bool
FlowConverter::setFieldValue(const FieldEntry& entry, const uint8_t* data, size_t size)
{
    if (entry.proto_packed) {
        m_tmp_packed.clear();
        if (!appendValue(entry, data, size, m_tmp_packed, false)) {
            return false;
        }
        if (m_tmp_packed.empty()) {
            return true;
        }

        m_buffer.append(reinterpret_cast<const char*>(entry.tag_bytes.data()), entry.tag_len);
        appendVarint(m_buffer, m_tmp_packed.size());
        m_buffer.append(m_tmp_packed);
        return true;
    }

    return appendValue(entry, data, size, m_buffer, true);
}

bool
FlowConverter::setBasicListField(const FieldEntry& entry, const struct fds_drec_field& field)
{
    if (!entry.is_list || !entry.proto_repeated) {
        return false;
    }

    struct fds_blist_iter list_it;
    fds_blist_iter_init(&list_it, const_cast<struct fds_drec_field*>(&field), nullptr);

    int rc = FDS_OK;
    if (entry.proto_packed) {
        m_tmp_packed.clear();

        while ((rc = fds_blist_iter_next(&list_it)) == FDS_OK) {
            if (!appendValue(entry, list_it.field.data, list_it.field.size, m_tmp_packed, false)) {
                return false;
            }
        }

        if (rc != FDS_EOC) {
            return false;
        }

        if (!m_tmp_packed.empty()) {
            m_buffer.append(reinterpret_cast<const char*>(entry.tag_bytes.data()), entry.tag_len);
            appendVarint(m_buffer, m_tmp_packed.size());
            m_buffer.append(m_tmp_packed);
        }

        return true;
    }

    while ((rc = fds_blist_iter_next(&list_it)) == FDS_OK) {
        if (!appendValue(entry, list_it.field.data, list_it.field.size, m_buffer, true)) {
            return false;
        }
    }

    return rc == FDS_EOC;
}

bool
FlowConverter::appendValue(const FieldEntry& entry,
                           const uint8_t* data,
                           size_t size,
                           std::string& out,
                           bool with_tag)
{
    using ProtoType = google::protobuf::FieldDescriptor::Type;

    const bool is_datetime = (entry.ipfix_type == FDS_ET_DATE_TIME_SECONDS ||
                              entry.ipfix_type == FDS_ET_DATE_TIME_MILLISECONDS ||
                              entry.ipfix_type == FDS_ET_DATE_TIME_MICROSECONDS ||
                              entry.ipfix_type == FDS_ET_DATE_TIME_NANOSECONDS);

    auto appendTagIfNeeded = [&]() {
        if (with_tag) {
            out.append(reinterpret_cast<const char*>(entry.tag_bytes.data()), entry.tag_len);
        }
    };

    auto appendLengthDelimited = [&](const uint8_t* ptr, size_t len) {
        appendTagIfNeeded();
        appendVarint(out, len);
        if (len > 0) {
            out.append(reinterpret_cast<const char*>(ptr), len);
        }
    };

    switch (entry.proto_type) {
    case ProtoType::TYPE_DOUBLE:
        if (size != 8) {
            return true;
        }
        {
            double value = 0;
            uint64_t bits = 0;
            std::memcpy(&value, data, sizeof(value));
            std::memcpy(&bits, &value, sizeof(bits));
            appendTagIfNeeded();
            appendFixed64(out, bits);
            return true;
        }

    case ProtoType::TYPE_FLOAT:
        if (size != 4) {
            return true;
        }
        {
            float value = 0;
            uint32_t bits = 0;
            std::memcpy(&value, data, sizeof(value));
            std::memcpy(&bits, &value, sizeof(bits));
            appendTagIfNeeded();
            appendFixed32(out, bits);
            return true;
        }

    case ProtoType::TYPE_INT64:
    case ProtoType::TYPE_SINT64:
    case ProtoType::TYPE_SFIXED64: {
        int64_t value = 0;
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) != FDS_OK) {
                return false;
            }
            value = static_cast<int64_t>(ts_ms);
        } else {
            if (fds_get_int_be(data, size, &value) != FDS_OK) {
                return false;
            }
        }

        appendTagIfNeeded();
        if (entry.proto_type == ProtoType::TYPE_SFIXED64) {
            appendFixed64(out, static_cast<uint64_t>(value));
        } else if (entry.proto_type == ProtoType::TYPE_SINT64) {
            appendVarint(out, zigzag64(value));
        } else {
            appendVarint(out, static_cast<uint64_t>(value));
        }
        return true;
    }

    case ProtoType::TYPE_UINT64:
    case ProtoType::TYPE_FIXED64: {
        uint64_t value = 0;
        if (is_datetime) {
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &value) != FDS_OK) {
                return false;
            }
        } else {
            if (fds_get_uint_be(data, size, &value) != FDS_OK) {
                return false;
            }
        }

        appendTagIfNeeded();
        if (entry.proto_type == ProtoType::TYPE_FIXED64) {
            appendFixed64(out, value);
        } else {
            appendVarint(out, value);
        }
        return true;
    }

    case ProtoType::TYPE_INT32:
    case ProtoType::TYPE_SINT32:
    case ProtoType::TYPE_SFIXED32: {
        int32_t value = 0;
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) != FDS_OK) {
                return false;
            }
            value = static_cast<int32_t>(ts_ms / 1000U);
        } else {
            int64_t val = 0;
            if (fds_get_int_be(data, size, &val) != FDS_OK) {
                return false;
            }
            value = static_cast<int32_t>(val);
        }

        appendTagIfNeeded();
        if (entry.proto_type == ProtoType::TYPE_SFIXED32) {
            appendFixed32(out, static_cast<uint32_t>(value));
        } else if (entry.proto_type == ProtoType::TYPE_SINT32) {
            appendVarint(out, zigzag32(value));
        } else {
            appendVarint(out, static_cast<uint64_t>(static_cast<int64_t>(value)));
        }
        return true;
    }

    case ProtoType::TYPE_UINT32:
    case ProtoType::TYPE_FIXED32: {
        uint32_t value = 0;
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) != FDS_OK) {
                return false;
            }
            value = static_cast<uint32_t>(ts_ms / 1000U);
        } else {
            uint64_t val = 0;
            if (fds_get_uint_be(data, size, &val) != FDS_OK) {
                return false;
            }
            value = static_cast<uint32_t>(val);
        }

        appendTagIfNeeded();
        if (entry.proto_type == ProtoType::TYPE_FIXED32) {
            appendFixed32(out, value);
        } else {
            appendVarint(out, value);
        }
        return true;
    }

    case ProtoType::TYPE_BOOL: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) != FDS_OK) {
            return false;
        }
        appendTagIfNeeded();
        appendVarint(out, val != 0 ? 1U : 0U);
        return true;
    }

    case ProtoType::TYPE_STRING:
        sanitizeUtf8(data, size, m_tmp_utf8);
        appendLengthDelimited(reinterpret_cast<const uint8_t*>(m_tmp_utf8.data()), m_tmp_utf8.size());
        return true;

    case ProtoType::TYPE_BYTES:
        appendTagIfNeeded();
        appendVarint(out, size);
        if (entry.source_is_ip_address) {
            for (size_t i = size; i > 0; --i) {
                out.push_back(static_cast<char>(data[i - 1]));
            }
        } else if (size > 0) {
            out.append(reinterpret_cast<const char*>(data), size);
        }
        return true;

    case ProtoType::TYPE_ENUM: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) != FDS_OK) {
            return false;
        }
        appendTagIfNeeded();
        appendVarint(out, static_cast<uint64_t>(static_cast<int64_t>(static_cast<int>(val))));
        return true;
    }

    default:
        return false;
    }
}

void
FlowConverter::extractPartitionField(uint16_t id, const uint8_t* data, size_t size, PartitionKey* key)
{
    switch (id) {
    case ID_SRC_IPV4:
        if (!key->src_ip) {
            key->src_ip = data;
            key->src_ip_len = size;
        }
        break;
    case ID_DST_IPV4:
        if (!key->dst_ip) {
            key->dst_ip = data;
            key->dst_ip_len = size;
        }
        break;
    case ID_SRC_IPV6:
        if (!key->src_ip) {
            key->src_ip = data;
            key->src_ip_len = size;
        }
        break;
    case ID_DST_IPV6:
        if (!key->dst_ip) {
            key->dst_ip = data;
            key->dst_ip_len = size;
        }
        break;
    case ID_SRC_PORT:
        if (size == 2) {
            key->src_port = ntohs(*reinterpret_cast<const uint16_t*>(data));
        }
        break;
    case ID_DST_PORT:
        if (size == 2) {
            key->dst_port = ntohs(*reinterpret_cast<const uint16_t*>(data));
        }
        break;
    case ID_PROTOCOL:
        if (size >= 1) {
            key->protocol = data[0];
        }
        break;
    case ID_FLOW_ID: {
        uint64_t flow_id = 0;
        if (fds_get_uint_be(data, size, &flow_id) == FDS_OK) {
            key->flow_id = flow_id;
            key->has_flow_id = true;
        }
        break;
    }
    default:
        break;
    }
}

} // namespace protobuf_kafka
