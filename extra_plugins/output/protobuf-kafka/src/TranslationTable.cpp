/**
 * @file TranslationTable.cpp
 * @brief Pre-computed IPFIX to Protobuf field mapping table
 * @author Jaroslav Pesek
 * @date 2026
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "TranslationTable.hpp"

#include <ipfixcol2.h>
#include <algorithm>
#include <stdexcept>

namespace protobuf_kafka {

namespace {

FieldEntry::WireType
protoTypeToWireType(google::protobuf::FieldDescriptor::Type type)
{
    using Type = google::protobuf::FieldDescriptor::Type;
    switch (type) {
    case Type::TYPE_DOUBLE:
    case Type::TYPE_FIXED64:
    case Type::TYPE_SFIXED64:
        return FieldEntry::WireType::FIXED64;
    case Type::TYPE_FLOAT:
    case Type::TYPE_FIXED32:
    case Type::TYPE_SFIXED32:
        return FieldEntry::WireType::FIXED32;
    case Type::TYPE_INT64:
    case Type::TYPE_UINT64:
    case Type::TYPE_INT32:
    case Type::TYPE_UINT32:
    case Type::TYPE_BOOL:
    case Type::TYPE_ENUM:
    case Type::TYPE_SINT32:
    case Type::TYPE_SINT64:
        return FieldEntry::WireType::VARINT;
    case Type::TYPE_STRING:
    case Type::TYPE_BYTES:
        return FieldEntry::WireType::LENGTH_DELIMITED;
    default:
        return FieldEntry::WireType::INVALID;
    }
}

bool
isIpAddressType(fds_iemgr_element_type type)
{
    return type == FDS_ET_IPV4_ADDRESS || type == FDS_ET_IPV6_ADDRESS;
}

uint8_t
encodeVarint(uint64_t value, uint8_t* dst)
{
    uint8_t len = 0;
    while (value >= 0x80U) {
        dst[len++] = static_cast<uint8_t>((value & 0x7FU) | 0x80U);
        value >>= 7U;
    }
    dst[len++] = static_cast<uint8_t>(value);
    return len;
}

} // namespace

void
TranslationTable::build(const std::vector<FieldMapping>& mappings,
                        ProtoSchema& schema,
                        const fds_iemgr_t* iemgr,
                        ipx_ctx_t* ctx)
{
    m_entries.clear();
    m_odid_entries.clear();
    m_lookup_sorted.clear();
    m_iana_scalar_direct.assign(UINT16_MAX + 1U, -1);

    m_entries.reserve(mappings.size());
    std::unordered_map<MappingKey, uint32_t, MappingKeyHash> index_map;
    index_map.reserve(mappings.size());

    for (const auto& mapping : mappings) {
        // ODID is a special field filled from the message context, not from IPFIX data
        if (mapping.is_odid) {
            const google::protobuf::FieldDescriptor* fd = schema.findField(mapping.proto_name);
            if (!fd) {
                throw std::runtime_error(
                    "Protobuf field '" + mapping.proto_name + "' not found in message type");
            }
            FieldEntry entry;
            entry.fd = fd;
            entry.is_odid = true;
            entry.proto_name = mapping.proto_name;
            entry.ipfix_spec = "odid";
            entry.is_list = false;
            entry.ipfix_type = FDS_ET_UNSIGNED_32;
            entry.proto_number = static_cast<uint32_t>(fd->number());
            entry.proto_type = fd->type();
            entry.proto_repeated = fd->is_repeated();
            entry.proto_packed = fd->is_packable() && fd->is_packed();
            entry.oneof_index = fd->containing_oneof() ? fd->containing_oneof()->index() : -1;
            entry.source_is_ip_address = false;
            entry.element_wire_type = protoTypeToWireType(fd->type());
            entry.field_wire_type = entry.proto_packed
                ? FieldEntry::WireType::LENGTH_DELIMITED
                : entry.element_wire_type;
            if (entry.field_wire_type == FieldEntry::WireType::INVALID ||
                entry.element_wire_type == FieldEntry::WireType::INVALID) {
                throw std::runtime_error(
                    "Unsupported protobuf field type for ODID field '" + mapping.proto_name + "'");
            }
            const uint64_t tag_value = (static_cast<uint64_t>(entry.proto_number) << 3U) |
                static_cast<uint64_t>(entry.field_wire_type);
            entry.tag_len = encodeVarint(tag_value, entry.tag_bytes.data());
            m_odid_entries.push_back(entry);
            IPX_CTX_DEBUG(ctx, "ODID mapping: odid -> %s (proto type=%d)",
                          mapping.proto_name.c_str(), static_cast<int>(fd->type()));
            continue;
        }

        const google::protobuf::FieldDescriptor* fd =
            schema.findField(mapping.proto_name);

        if (!fd) {
            throw std::runtime_error(
                "Protobuf field '" + mapping.proto_name +
                "' not found in message type");
        }

        const fds_iemgr_elem* root_elem =
            fds_iemgr_elem_find_id(iemgr, mapping.root_pen, mapping.root_id);
        if (!root_elem) {
            throw std::runtime_error(
                "IPFIX element for mapping '" + mapping.ipfix_spec + "' not found in IE manager");
        }

        MappingKey key;
        key.root_pen = mapping.root_pen;
        key.root_id = mapping.root_id;

        fds_iemgr_element_type value_type = root_elem->data_type;
        if (mapping.is_list) {
            if (root_elem->data_type != FDS_ET_BASIC_LIST) {
                throw std::runtime_error(
                    "Mapping '" + mapping.ipfix_spec +
                    "' uses list selector but root element is not basicList");
            }

            const fds_iemgr_elem* list_elem =
                fds_iemgr_elem_find_id(iemgr, mapping.list_pen, mapping.list_id);
            if (!list_elem) {
                throw std::runtime_error(
                    "List element in mapping '" + mapping.ipfix_spec + "' not found in IE manager");
            }

            if (!fd->is_repeated()) {
                throw std::runtime_error(
                    "Mapping '" + mapping.ipfix_spec + "' targets non-repeated protobuf field '" +
                    mapping.proto_name + "'");
            }

            key.has_list_elem = true;
            key.list_pen = mapping.list_pen;
            key.list_id = mapping.list_id;
            value_type = list_elem->data_type;
        }

        FieldEntry entry;
        entry.fd = fd;
        entry.proto_name = mapping.proto_name;
        entry.ipfix_spec = mapping.ipfix_spec;
        entry.is_list = mapping.is_list;
        entry.ipfix_type = value_type;
        entry.proto_number = static_cast<uint32_t>(fd->number());
        entry.proto_type = fd->type();
        entry.proto_repeated = fd->is_repeated();
        entry.proto_packed = fd->is_packable() && fd->is_packed();
        entry.oneof_index = fd->containing_oneof() ? fd->containing_oneof()->index() : -1;
        entry.source_is_ip_address = isIpAddressType(value_type);

        entry.element_wire_type = protoTypeToWireType(fd->type());
        entry.field_wire_type = entry.proto_packed
            ? FieldEntry::WireType::LENGTH_DELIMITED
            : entry.element_wire_type;
        if (entry.field_wire_type == FieldEntry::WireType::INVALID ||
            entry.element_wire_type == FieldEntry::WireType::INVALID) {
            throw std::runtime_error(
                "Unsupported protobuf field type for mapping '" + mapping.ipfix_spec +
                "' (" + mapping.proto_name + ")");
        }

        const uint64_t tag_value = (static_cast<uint64_t>(entry.proto_number) << 3U) |
            static_cast<uint64_t>(entry.field_wire_type);
        entry.tag_len = encodeVarint(tag_value, entry.tag_bytes.data());

        if (!mapping.is_list && fd->is_repeated()) {
            IPX_CTX_WARNING(ctx, "Mapping '%s' maps a scalar IPFIX field to repeated proto field '%s'; "
                            "each record will emit one repeated element",
                            mapping.ipfix_spec.c_str(), mapping.proto_name.c_str());
        }

        const uint32_t index = static_cast<uint32_t>(m_entries.size());
        m_entries.push_back(entry);

        const bool inserted = index_map.emplace(key, index).second;
        if (!inserted) {
            throw std::runtime_error(
                "Duplicate mapping key for IPFIX specification '" + mapping.ipfix_spec + "'");
        }

        IPX_CTX_DEBUG(ctx, "Mapping: %s (root PEN=%u, root ID=%u, list PEN=%u, list ID=%u) -> %s (proto type=%d)",
                      mapping.ipfix_spec.c_str(),
                      mapping.root_pen, mapping.root_id,
                      mapping.is_list ? mapping.list_pen : 0U,
                      mapping.is_list ? mapping.list_id : 0U,
                      mapping.proto_name.c_str(),
                      static_cast<int>(fd->type()));
    }

    m_lookup_sorted.reserve(index_map.size());
    for (const auto& item : index_map) {
        LookupRecord rec;
        rec.key = item.first;
        rec.index = item.second;
        m_lookup_sorted.push_back(rec);
    }

    std::sort(m_lookup_sorted.begin(), m_lookup_sorted.end(),
              [](const LookupRecord& lhs, const LookupRecord& rhs) {
                  return lhs.key < rhs.key;
              });

    for (const auto& rec : m_lookup_sorted) {
        if (rec.key.root_pen != 0 || rec.key.has_list_elem) {
            continue;
        }
        m_iana_scalar_direct[rec.key.root_id] = static_cast<int32_t>(rec.index);
    }

    IPX_CTX_INFO(ctx, "Translation table built with %zu field mappings",
                 m_entries.size());
}

const FieldEntry*
TranslationTable::lookup(const MappingKey& key) const
{
    if (key.root_pen == 0 && !key.has_list_elem && key.root_id < m_iana_scalar_direct.size()) {
        const int32_t direct_idx = m_iana_scalar_direct[key.root_id];
        if (direct_idx >= 0) {
            return &m_entries[static_cast<size_t>(direct_idx)];
        }
    }

    const auto it = std::lower_bound(
        m_lookup_sorted.begin(), m_lookup_sorted.end(), key,
        [](const LookupRecord& lhs, const MappingKey& rhs) {
            return lhs.key < rhs;
        });

    if (it == m_lookup_sorted.end()) {
        return nullptr;
    }

    if ((key < it->key) || (it->key < key)) {
        return nullptr;
    }

    return &m_entries[it->index];
}

} // namespace protobuf_kafka
