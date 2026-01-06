/**
 * \file FlowConverter.hpp
 * \brief To Protobuf converter for hot path
 * \author Jaroslav Pesek
 * \date 2026
 */

#ifndef PROTOBUF_KAFKA_FLOWCONVERTER_HPP
#define PROTOBUF_KAFKA_FLOWCONVERTER_HPP

#include "ProtoSchema.hpp"
#include "TranslationTable.hpp"
#include "Config.hpp"

#include <string>
#include <cstdint>

#include <libfds.h>

namespace protobuf_kafka {

/**
 * \brief Inputs for RSS partition computation
 */
struct PartitionKey {
    uint64_t flow_id = 0;
    bool has_flow_id = false;

    const uint8_t* src_ip = nullptr;
    size_t src_ip_len = 0;
    const uint8_t* dst_ip = nullptr;
    size_t dst_ip_len = 0;
    uint16_t src_port = 0;
    uint16_t dst_port = 0;
    uint8_t protocol = 0;
    bool valid = false;
};

/**
 * \brief IPFIX to Protobuf flow converter
 */
class FlowConverter {
public:
    /**
     * \brief Create a converter
     *
     * \param schema  Protobuf schema (must outlive this object)
     * \param table   Translation table (must outlive this object)
     * \param mode    Partition mode
     */
    FlowConverter(ProtoSchema& schema,
                  const TranslationTable& table,
                  PartitionMode mode);

    ~FlowConverter();

    FlowConverter(const FlowConverter&) = delete;
    FlowConverter& operator=(const FlowConverter&) = delete;
    FlowConverter(FlowConverter&&) = delete;
    FlowConverter& operator=(FlowConverter&&) = delete;

    /**
     * \brief Convert an IPFIX record to serialized Protobuf
     *
     * \param[in]  rec            IPFIX data record
     * \param[out] out_data       Pointer to serialized data
     * \param[out] out_len        Length of serialized data
     * \param[out] partition_key  Partition key for RSS (if mode is RSS)
     * \return true on success, false if conversion failed
     */
    bool convert(const fds_drec* rec,
                 const char** out_data,
                 size_t* out_len,
                 PartitionKey* partition_key);

private:
    const TranslationTable& m_table;
    PartitionMode m_partition_mode;
    std::string m_buffer;
    std::string m_tmp_utf8;
    std::string m_tmp_packed;

    static constexpr uint32_t IANA_PEN = 0;
    static constexpr uint16_t ID_SRC_IPV4 = 8;
    static constexpr uint16_t ID_DST_IPV4 = 12;
    static constexpr uint16_t ID_SRC_IPV6 = 27;
    static constexpr uint16_t ID_DST_IPV6 = 28;
    static constexpr uint16_t ID_SRC_PORT = 7;
    static constexpr uint16_t ID_DST_PORT = 11;
    static constexpr uint16_t ID_PROTOCOL = 4;
    static constexpr uint16_t ID_FLOW_ID = 148;

    /// Convert one scalar value and append encoded protobuf field into output buffer.
    bool setFieldValue(const FieldEntry& entry, const uint8_t* data, size_t size);

    /// Convert whole basicList and append encoded protobuf field(s) into output buffer.
    bool setBasicListField(const FieldEntry& entry, const struct fds_drec_field& field);

    /// Convert one scalar value and append either encoded field or encoded packed element.
    bool appendValue(const FieldEntry& entry,
                     const uint8_t* data,
                     size_t size,
                     std::string& out,
                     bool with_tag);

    /**
     * \brief Extract partition field from current IPFIX field (single-pass)
     *
     * \param id    IPFIX Information Element ID
     * \param data  Raw field data
     * \param size  Size of data
     * \param key   Partition key to populate
     */
    void extractPartitionField(uint16_t id, const uint8_t* data, size_t size, PartitionKey* key);
};

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_FLOWCONVERTER_HPP
