/**
 * \file ProtoSchema.cpp
 * \brief Dynamic protobuf schema loading using reflection
 * \author Jaroslav Pesek
 * \date 2026
 */

#include "ProtoSchema.hpp"

#include <stdexcept>
#include <sstream>
#include <libgen.h>
#include <cstring>

namespace protobuf_kafka {

#if GOOGLE_PROTOBUF_VERSION >= 5000000
void
ProtoErrorCollector::RecordError(absl::string_view filename, int line, int column,
                                  absl::string_view message)
{
    std::ostringstream oss;
    oss << filename << ":" << line << ":" << column << ": error: " << message;
    m_errors.push_back(oss.str());
}

void
ProtoErrorCollector::RecordWarning(absl::string_view filename, int line, int column,
                                    absl::string_view message)
{
    (void)filename;
    (void)line;
    (void)column;
    (void)message;
}
#else
void
ProtoErrorCollector::AddError(const std::string& filename, int line, int column,
                               const std::string& message)
{
    std::ostringstream oss;
    oss << filename << ":" << line << ":" << column << ": error: " << message;
    m_errors.push_back(oss.str());
}

void
ProtoErrorCollector::AddWarning(const std::string& filename, int line, int column,
                                 const std::string& message)
{
    (void)filename;
    (void)line;
    (void)column;
    (void)message;
}
#endif

std::string
ProtoErrorCollector::error_string() const
{
    std::ostringstream oss;
    for (size_t i = 0; i < m_errors.size(); ++i) {
        if (i > 0) {
            oss << "; ";
        }
        oss << m_errors[i];
    }
    return oss.str();
}

ProtoSchema::ProtoSchema(const std::string& proto_file, const std::string& message_type)
    : m_descriptor(nullptr)
{
    std::vector<char> path_buf(proto_file.begin(), proto_file.end());
    path_buf.push_back('\0');

    char* dir = dirname(path_buf.data());
    std::string proto_dir = dir;

    path_buf.assign(proto_file.begin(), proto_file.end());
    path_buf.push_back('\0');
    char* base = basename(path_buf.data());
    std::string proto_filename = base;

    m_source_tree.MapPath("", proto_dir);
    m_source_tree.MapPath("", "/usr/include");
    m_source_tree.MapPath("", "/usr/local/include");

    m_importer = std::make_unique<google::protobuf::compiler::Importer>(
        &m_source_tree, &m_error_collector);

    const google::protobuf::FileDescriptor* file_desc =
        m_importer->Import(proto_filename);

    if (!file_desc) {
        throw std::runtime_error(
            "Failed to parse proto file '" + proto_file + "': " +
            m_error_collector.error_string());
    }

    if (m_error_collector.has_errors()) {
        throw std::runtime_error(
            "Errors parsing proto file '" + proto_file + "': " +
            m_error_collector.error_string());
    }

    m_descriptor = m_importer->pool()->FindMessageTypeByName(message_type);
    if (!m_descriptor) {
        throw std::runtime_error(
            "Message type '" + message_type + "' not found in proto file '" +
            proto_file + "'");
    }
}

const google::protobuf::FieldDescriptor*
ProtoSchema::findField(const std::string& name) const
{
    return m_descriptor->FindFieldByName(name);
}

google::protobuf::Message*
ProtoSchema::createMessage()
{
    const google::protobuf::Message* prototype = m_factory.GetPrototype(m_descriptor);
    return prototype->New();
}

} // namespace protobuf_kafka
