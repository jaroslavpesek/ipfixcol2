#include "FlowConverter.hpp"

namespace protobuf_kafka_compiled {

bool FlowConverter::convert(const fds_drec *rec, ipfix::FlowRecord &msg,
                            PartitionKey *partition_key) {
  msg.Clear();
  PartitionKey pk_local{};

  const auto &ipfix_ids = m_table.ipfixIds();
  const auto &entries = m_table.entries();

  for (size_t i = 0; i < ipfix_ids.size(); ++i) {
    const auto &[pen, id] = ipfix_ids[i];
    const FieldEntry &entry = entries[i];

    struct fds_drec_field field;
    if (fds_drec_find(const_cast<fds_drec *>(rec), pen, id, &field) == FDS_EOC)
      continue;

    setField(msg, entry, field.data, field.size);

    if (m_partition_mode == PartitionMode::RSS && partition_key &&
        pen == IANA_PEN) {
      extractPartitionField(id, field.data, field.size, &pk_local);
    }
  }

  if (m_partition_mode == PartitionMode::PRECOMPUTED_RSS && partition_key) {
    pk_local.valid = true;
    partition_key->flowID = m_flowID;
  } else if (m_partition_mode == PartitionMode::RSS && partition_key) {
    pk_local.valid = (pk_local.src_ip != nullptr || pk_local.dst_ip != nullptr);
    *partition_key = pk_local;
  }

  return true;
}

void FlowConverter::setField(ipfix::FlowRecord &msg, const FieldEntry &entry,
                             const uint8_t *data, size_t size) {
  auto val_u16 = [&](const uint8_t *d) -> uint16_t {
    uint16_t v;
    std::memcpy(&v, d, sizeof(v));
    return ntohs(v);
  };

  auto val_u32 = [&](const uint8_t *d) -> uint32_t {
    uint32_t v;
    std::memcpy(&v, d, sizeof(v));
    return ntohl(v);
  };

  auto val_u64 = [&](const uint8_t *d) -> uint64_t {
    uint64_t v;
    std::memcpy(&v, d, sizeof(v));
    return be64toh(v);
  };

  const std::string &name = entry.proto_name;

  // Core fields
  if (name == "SRC_IP") {
    msg.set_src_ip(data, size);
  } else if (name == "DST_IP") {
    msg.set_dst_ip(data, size);
  } else if (name == "SRC_PORT" && size == 2) {
    msg.set_src_port(val_u16(data));
  } else if (name == "DST_PORT" && size == 2) {
    msg.set_dst_port(val_u16(data));
  } else if (name == "PROTOCOL" && size == 1) {
    msg.set_protocol(data[0]);
  } else if (name == "BYTES" && size == 8) {
    msg.set_bytes(val_u64(data));
  } else if (name == "BYTES_REV" && size == 8) {
    msg.set_bytes_rev(val_u64(data));
  } else if (name == "PACKETS" && size == 8) {
    msg.set_packets(val_u64(data));
  } else if (name == "PACKETS_REV" && size == 8) {
    msg.set_packets_rev(val_u64(data));
  } else if (name == "TIME_FIRST" && size == 8) {
    msg.set_time_first(val_u64(data));
  } else if (name == "TIME_LAST" && size == 8) {
    msg.set_time_last(val_u64(data));
  } else if (name == "TCP_FLAGS" && size <= 4) {
    msg.set_tcp_flags(val_u32(data));
  } else if (name == "TCP_FLAGS_REV" && size <= 4) {
    msg.set_tcp_flags_rev(val_u32(data));
  } else if (name == "TTL" && size <= 4) {
    msg.set_ttl(val_u32(data));
  } else if (name == "TTL_REV" && size <= 4) {
    msg.set_ttl_rev(val_u32(data));
  }

  // Flow metadata
  else if (name == "FLOW_END_REASON" && size <= 4) {
    msg.set_flow_end_reason(val_u32(data));
  }

  // DNS
  else if (name == "DNS_ANSWERS" && size <= 4) {
    msg.set_dns_answers(val_u32(data));
  } else if (name == "DNS_ID" && size <= 4) {
    msg.set_dns_id(val_u32(data));
  } else if (name == "DNS_Q_TYPE" && size <= 4) {
    msg.set_dns_q_type(val_u32(data));
  } else if (name == "DNS_RR_RLENGTH" && size <= 4) {
    msg.set_dns_rr_rlength(val_u32(data));
  } else if (name == "DNS_DO" && size <= 4) {
    msg.set_dns_do(val_u32(data));
  } else if (name == "DNS_RCODE" && size <= 4) {
    msg.set_dns_rcode(val_u32(data));
  } else if (name == "DNS_Q_NAME") {
    msg.set_dns_q_name(data, size);
  }

  // TLS
  else if (name == "FME_TLS_SERVER_VERSION" && size <= 4) {
    msg.set_fme_tls_server_version(val_u32(data));
  } else if (name == "TLS_SNI") {
    msg.set_tls_sni(reinterpret_cast<const char *>(data), size);
  }

  // HTTP
  else if (name == "HTTP_RESPONSE_STATUS_CODE" && size <= 4) {
    msg.set_http_response_status_code(val_u32(data));
  } else if (name == "HTTP_REQUEST_AGENT") {
    msg.set_http_request_agent(reinterpret_cast<const char *>(data), size);
  } else if (name == "HTTP_REQUEST_HOST") {
    msg.set_http_request_host(reinterpret_cast<const char *>(data), size);
  } else if (name == "HTTP_REQUEST_REFERER") {
    msg.set_http_request_referer(reinterpret_cast<const char *>(data), size);
  } else if (name == "HTTP_REQUEST_URL") {
    msg.set_http_request_url(reinterpret_cast<const char *>(data), size);
  } else if (name == "HTTP_RESPONSE_CONTENT_TYPE") {
    msg.set_http_response_content_type(reinterpret_cast<const char *>(data),
                                       size);
  }

  // Internal
  else if (name == "FLOW_ID" && size == 8) {
    m_flowID = val_u64(data);
  }

  // if the schema contains other types of elements, if-else statements similar
  // to those above must be added here...
}

void FlowConverter::extractPartitionField(uint16_t id, const uint8_t *data,
                                          size_t size, PartitionKey *key) {
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
      key->src_port = ntohs(*reinterpret_cast<const uint16_t *>(data));
    }
    break;
  case ID_DST_PORT:
    if (size == 2) {
      key->dst_port = ntohs(*reinterpret_cast<const uint16_t *>(data));
    }
    break;
  case ID_PROTOCOL:
    if (size >= 1) {
      key->protocol = data[0];
    }
    break;
  default:
    break;
  }
}

} // namespace protobuf_kafka_compiled
