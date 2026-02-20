#include "TranslationTable.hpp"

namespace protobuf_kafka_compiled {

void TranslationTable::build(const std::vector<FieldMapping> &mappings,
                             const fds_iemgr_t *iemgr, ipx_ctx_t *ctx) {
  // Deleting old data
  m_lookup.clear();
  m_entries.clear();
  m_ipfix_ids.clear();

  // Memory reservation
  m_entries.reserve(mappings.size());
  m_ipfix_ids.reserve(mappings.size());

  // Iteration over each mapping IPFIX->PROTOBUF defined in element <map> a
  // saving to lookup table
  for (const auto &mapping : mappings) {
    fds_iemgr_element_type ipfix_type = FDS_ET_OCTET_ARRAY;
    const fds_iemgr_elem *elem = fds_iemgr_elem_find_id(
        iemgr, mapping.ipfix_pen, mapping.ipfix_id); // Find IPFIX type
    if (elem) {
      ipfix_type = elem->data_type;
    }

    // Create intern record (PROTO name, IPFIX type)
    FieldEntry entry;
    entry.proto_name = mapping.proto_name;
    entry.ipfix_type = ipfix_type;

    // Save entry (PROTO name, IPFIX type) to vector of entries
    size_t index = m_entries.size();
    m_entries.push_back(entry);
    // Save IPFIX ID (PEN, ID) to vector of IPFIX IDs
    m_ipfix_ids.emplace_back(mapping.ipfix_pen, mapping.ipfix_id);

    uint64_t key = makeKey(mapping.ipfix_pen,
                           mapping.ipfix_id); // Create hash of IPFIX ID (PEN,
                                              // ID) -> key for lookup table
    m_lookup[key] =
        index; // Save index of entry to lookup table according to its key

    IPX_CTX_DEBUG(ctx, "Mapping: %s (PEN=%u, ID=%u) -> %s",
                  mapping.ipfix_spec.c_str(), mapping.ipfix_pen,
                  mapping.ipfix_id, mapping.proto_name.c_str());
  }

  IPX_CTX_INFO(ctx, "Translation table built with %zu field mappings",
               m_entries.size());
}

const FieldEntry *TranslationTable::lookup(uint32_t pen, uint16_t id) const {
  uint64_t key = makeKey(pen, id);
  auto it = m_lookup.find(key);
  if (it == m_lookup.end()) {
    return nullptr;
  }
  return &m_entries[it->second];
}

} // namespace protobuf_kafka_compiled
