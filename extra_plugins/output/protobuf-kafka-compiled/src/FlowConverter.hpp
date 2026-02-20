#ifndef PROTOBUF_KAFKA_COMPILED_FLOWCONVERTER_HPP
#define PROTOBUF_KAFKA_COMPILED_FLOWCONVERTER_HPP

#include "../generated_schema/schema.pb.h" // generated protobuf class (command protoc --cpp_out=. schema)
#include "Config.hpp"
#include "TranslationTable.hpp"

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <libfds.h>
#include <string>

namespace protobuf_kafka_compiled {

struct PartitionKey {
  const uint8_t *src_ip = nullptr;
  size_t src_ip_len = 0;
  const uint8_t *dst_ip = nullptr;
  size_t dst_ip_len = 0;
  uint16_t src_port = 0;
  uint16_t dst_port = 0;
  uint8_t protocol = 0;
  uint64_t flowID = 0;
  bool valid = false;
};

class FlowConverter {
public:
  FlowConverter(const TranslationTable &table, PartitionMode mode)
      : m_table(table), m_partition_mode(mode) {}

  bool convert(const fds_drec *rec, ipfix::FlowRecord &msg,
               PartitionKey *partition_key);

private:
  const TranslationTable &m_table;
  PartitionMode m_partition_mode;

  void setField(ipfix::FlowRecord &msg, const FieldEntry &entry,
                const uint8_t *data, size_t size);

  void extractPartitionField(uint16_t id, const uint8_t *data, size_t size,
                             PartitionKey *key);

  static constexpr uint32_t IANA_PEN = 0;
  static constexpr uint16_t ID_SRC_IPV4 = 8;
  static constexpr uint16_t ID_DST_IPV4 = 12;
  static constexpr uint16_t ID_SRC_IPV6 = 27;
  static constexpr uint16_t ID_DST_IPV6 = 28;
  static constexpr uint16_t ID_SRC_PORT = 7;
  static constexpr uint16_t ID_DST_PORT = 11;
  static constexpr uint16_t ID_PROTOCOL = 4;

  uint64_t m_flowID = 0;
};

} // namespace protobuf_kafka_compiled
#endif // PROTOBUF_KAFKA_COMPILED_FLOWCONVERTER_HPP
