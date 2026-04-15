/**
 * @file ProtoSchema.hpp
 * @brief Dynamic protobuf schema loading using reflection
 * @author Jaroslav Pesek
 * @date 2026
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef PROTOBUF_KAFKA_PROTOSCHEMA_HPP
#define PROTOBUF_KAFKA_PROTOSCHEMA_HPP

#include <string>
#include <memory>
#include <vector>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/compiler/importer.h>

#if GOOGLE_PROTOBUF_VERSION >= 5000000
#include <absl/strings/string_view.h>
#endif

namespace protobuf_kafka {

/**
 * \brief Error collector for protobuf parsing errors
 */
class ProtoErrorCollector : public google::protobuf::compiler::MultiFileErrorCollector {
public:
#if GOOGLE_PROTOBUF_VERSION >= 5000000
    void RecordError(absl::string_view filename, int line, int column,
                     absl::string_view message) override;
    void RecordWarning(absl::string_view filename, int line, int column,
                       absl::string_view message) override;
#else
    void AddError(const std::string& filename, int line, int column,
                  const std::string& message) override;
    void AddWarning(const std::string& filename, int line, int column,
                    const std::string& message) override;
#endif

    bool has_errors() const { return !m_errors.empty(); }
    const std::vector<std::string>& errors() const { return m_errors; }
    std::string error_string() const;

private:
    std::vector<std::string> m_errors;
};

/**
 * \brief Dynamic protobuf schema loader
 *
 * Loads .proto file at runtime and provides access to message descriptors
 * and a factory for creating dynamic messages.
 */
class ProtoSchema {
public:
    /**
     * \brief Load a .proto file and resolve a message type
     *
     * \param proto_file    Path to the .proto file
     * \param message_type  Fully qualified message type name (e.g., "Retina.FlowRecord")
     * \throws std::runtime_error on parse or resolution error
     */
    ProtoSchema(const std::string& proto_file, const std::string& message_type);

    ~ProtoSchema() = default;

    // Non-copyable, movable
    ProtoSchema(const ProtoSchema&) = delete;
    ProtoSchema& operator=(const ProtoSchema&) = delete;
    ProtoSchema(ProtoSchema&&) = default;
    ProtoSchema& operator=(ProtoSchema&&) = default;

    /**
     * \brief Get the message descriptor
     * \return Pointer to the descriptor (never null after successful construction)
     */
    const google::protobuf::Descriptor* descriptor() const { return m_descriptor; }

    /**
     * \brief Get the dynamic message factory
     * \return Pointer to the factory
     */
    google::protobuf::DynamicMessageFactory* factory() { return &m_factory; }

    /**
     * \brief Find a field descriptor by name
     *
     * This should only be called during initialization, not in the hot path.
     *
     * \param name  Field name
     * \return Field descriptor or nullptr if not found
     */
    const google::protobuf::FieldDescriptor* findField(const std::string& name) const;

    /**
     * \brief Create a new dynamic message instance
     *
     * The caller owns the returned message and must delete it.
     *
     * \return New message instance
     */
    google::protobuf::Message* createMessage();

private:
    google::protobuf::compiler::DiskSourceTree m_source_tree;
    ProtoErrorCollector m_error_collector;
    std::unique_ptr<google::protobuf::compiler::Importer> m_importer;
    google::protobuf::DynamicMessageFactory m_factory;
    const google::protobuf::Descriptor* m_descriptor;
};

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_PROTOSCHEMA_HPP
