#include "iggy.hpp"

#include <limits>
#include <utility>

namespace iggy {

IggyError::IggyError(std::string operation, ErrorCategory category,
                     std::string message)
    : std::runtime_error(std::move(message)),
      operation_(std::move(operation)),
      category_(category) {}

const std::string &IggyError::operation() const noexcept { return operation_; }

ErrorCategory IggyError::category() const noexcept { return category_; }

Identifier::Identifier(Kind kind, std::vector<std::uint8_t> value)
    : kind_(kind), value_(std::move(value)) {}

Identifier Identifier::numeric(std::uint32_t value) {
  std::vector<std::uint8_t> bytes(4);
  bytes[0] = static_cast<std::uint8_t>(value & 0xffU);
  bytes[1] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
  bytes[2] = static_cast<std::uint8_t>((value >> 16U) & 0xffU);
  bytes[3] = static_cast<std::uint8_t>((value >> 24U) & 0xffU);
  return Identifier(Kind::Numeric, std::move(bytes));
}

Identifier Identifier::named(const std::string &value) {
  if (value.empty()) {
    throw IggyError("Identifier::named", ErrorCategory::Validation,
                    "identifier must not be empty");
  }
  if (value.size() > 255) {
    throw IggyError("Identifier::named", ErrorCategory::Validation,
                    "identifier length must be <= 255 bytes");
  }
  std::vector<std::uint8_t> bytes;
  bytes.reserve(value.size());
  for (unsigned char ch : value) {
    bytes.push_back(static_cast<std::uint8_t>(ch));
  }
  return Identifier(Kind::String, std::move(bytes));
}

Identifier::Kind Identifier::kind() const noexcept { return kind_; }

const std::vector<std::uint8_t> &Identifier::value() const noexcept {
  return value_;
}

std::uint8_t Identifier::length() const noexcept {
  return static_cast<std::uint8_t>(value_.size());
}

PollingStrategy PollingStrategy::offset(std::uint64_t offset_value) {
  return PollingStrategy{PollingKind::Offset, offset_value};
}

PollingStrategy PollingStrategy::timestamp(std::uint64_t timestamp_value) {
  return PollingStrategy{PollingKind::Timestamp, timestamp_value};
}

PollingStrategy PollingStrategy::first() {
  return PollingStrategy{PollingKind::First, 0};
}

PollingStrategy PollingStrategy::last() {
  return PollingStrategy{PollingKind::Last, 0};
}

PollingStrategy PollingStrategy::next() {
  return PollingStrategy{PollingKind::Next, 0};
}

Partitioning::Partitioning(std::uint32_t id) : partition_id(id) {}

std::string HeaderValue::text() const {
  if (value.empty()) {
    return std::string();
  }
  return std::string(reinterpret_cast<const char *>(value.data()), value.size());
}

MessageHeader &IggyMessage::header() & { return message_header; }

const MessageHeader &IggyMessage::header() const & { return message_header; }

IggyMessage &IggyMessage::payload(std::string_view text) & {
  payload_bytes.clear();
  payload_bytes.reserve(text.size());
  for (unsigned char ch : text) {
    payload_bytes.push_back(static_cast<std::uint8_t>(ch));
  }
  payload_text.assign(text.data(), text.size());
  message_header.payload_length =
      static_cast<std::uint32_t>(payload_bytes.size());
  return *this;
}

IggyMessage &&IggyMessage::payload(std::string_view text) && {
  payload(text);
  return std::move(*this);
}

IggyMessage &IggyMessage::payload(std::vector<std::uint8_t> data) & {
  payload_bytes = std::move(data);
  payload_text.assign(payload_bytes.begin(), payload_bytes.end());
  message_header.payload_length =
      static_cast<std::uint32_t>(payload_bytes.size());
  return *this;
}

IggyMessage &&IggyMessage::payload(std::vector<std::uint8_t> data) && {
  payload(std::move(data));
  return std::move(*this);
}

IggyMessage &IggyMessage::header(std::string_view key,
                                 std::string_view value) & {
  HeaderEntry entry;
  entry.key = std::string(key);
  entry.value.kind = HeaderKind::String;
  entry.value.value.reserve(value.size());
  for (unsigned char ch : value) {
    entry.value.value.push_back(static_cast<std::uint8_t>(ch));
  }
  headers.entries.push_back(std::move(entry));
  return *this;
}

IggyMessage &&IggyMessage::header(std::string_view key,
                                  std::string_view value) && {
  header(key, value);
  return std::move(*this);
}

IggyMessage &IggyMessage::user_headers(HeaderMap map) & {
  headers = std::move(map);
  return *this;
}

IggyMessage &&IggyMessage::user_headers(HeaderMap map) && {
  headers = std::move(map);
  return std::move(*this);
}

IggyMessageBuilder IggyMessage::Builder() { return IggyMessageBuilder(); }

IggyMessageBuilder &IggyMessageBuilder::payload(std::string_view text) & {
  message_.payload(text);
  return *this;
}

IggyMessageBuilder &&IggyMessageBuilder::payload(std::string_view text) && {
  message_.payload(text);
  return std::move(*this);
}

IggyMessageBuilder &IggyMessageBuilder::payload(std::vector<std::uint8_t> data) & {
  message_.payload(std::move(data));
  return *this;
}

IggyMessageBuilder &&IggyMessageBuilder::payload(std::vector<std::uint8_t> data) && {
  message_.payload(std::move(data));
  return std::move(*this);
}

IggyMessageBuilder &IggyMessageBuilder::header(std::string_view key,
                                               std::string_view value) & {
  message_.header(key, value);
  return *this;
}

IggyMessageBuilder &&IggyMessageBuilder::header(std::string_view key,
                                                std::string_view value) && {
  message_.header(key, value);
  return std::move(*this);
}

IggyMessageBuilder &IggyMessageBuilder::user_headers(HeaderMap map) & {
  message_.user_headers(std::move(map));
  return *this;
}

IggyMessageBuilder &&IggyMessageBuilder::user_headers(HeaderMap map) && {
  message_.user_headers(std::move(map));
  return std::move(*this);
}

IggyMessage IggyMessageBuilder::build() && { return std::move(message_); }

IggyMessageBuilder::operator IggyMessage() && { return std::move(message_); }

namespace detail {

void throw_validation(const char *operation, const std::string &message) {
  throw IggyError(operation, ErrorCategory::Validation, message);
}

std::string to_std_string(const ::rust::String &value) {
  return std::string(value);
}

::rust::Vec<std::uint8_t>
to_rust_bytes(const std::vector<std::uint8_t> &value) {
  ::rust::Vec<std::uint8_t> bytes;
  bytes.reserve(value.size());
  for (auto byte : value) {
    bytes.push_back(byte);
  }
  return bytes;
}

std::vector<std::uint8_t>
from_rust_bytes(const ::rust::Vec<std::uint8_t> &value) {
  std::vector<std::uint8_t> bytes;
  bytes.reserve(value.size());
  for (auto byte : value) {
    bytes.push_back(byte);
  }
  return bytes;
}

::iggy::ffi::FfiIdentifier to_ffi_identifier(const Identifier &identifier) {
  ::iggy::ffi::FfiIdentifier ffi{};
  ffi.kind = identifier.kind() == Identifier::Kind::Numeric
                 ? ::iggy::ffi::FfiIdentifierKind::Numeric
                 : ::iggy::ffi::FfiIdentifierKind::String;
  ffi.length = identifier.length();
  ffi.value = to_rust_bytes(identifier.value());
  return ffi;
}

::iggy::ffi::FfiHeaderKind to_ffi_header_kind(HeaderKind kind) {
  switch (kind) {
  case HeaderKind::Raw:
    return ::iggy::ffi::FfiHeaderKind::Raw;
  case HeaderKind::String:
    return ::iggy::ffi::FfiHeaderKind::String;
  case HeaderKind::Bool:
    return ::iggy::ffi::FfiHeaderKind::Bool;
  case HeaderKind::Int8:
    return ::iggy::ffi::FfiHeaderKind::Int8;
  case HeaderKind::Int16:
    return ::iggy::ffi::FfiHeaderKind::Int16;
  case HeaderKind::Int32:
    return ::iggy::ffi::FfiHeaderKind::Int32;
  case HeaderKind::Int64:
    return ::iggy::ffi::FfiHeaderKind::Int64;
  case HeaderKind::Int128:
    return ::iggy::ffi::FfiHeaderKind::Int128;
  case HeaderKind::Uint8:
    return ::iggy::ffi::FfiHeaderKind::Uint8;
  case HeaderKind::Uint16:
    return ::iggy::ffi::FfiHeaderKind::Uint16;
  case HeaderKind::Uint32:
    return ::iggy::ffi::FfiHeaderKind::Uint32;
  case HeaderKind::Uint64:
    return ::iggy::ffi::FfiHeaderKind::Uint64;
  case HeaderKind::Uint128:
    return ::iggy::ffi::FfiHeaderKind::Uint128;
  case HeaderKind::Float32:
    return ::iggy::ffi::FfiHeaderKind::Float32;
  case HeaderKind::Float64:
    return ::iggy::ffi::FfiHeaderKind::Float64;
  }
  return ::iggy::ffi::FfiHeaderKind::Raw;
}

HeaderKind from_ffi_header_kind(::iggy::ffi::FfiHeaderKind kind) {
  switch (kind) {
  case ::iggy::ffi::FfiHeaderKind::Raw:
    return HeaderKind::Raw;
  case ::iggy::ffi::FfiHeaderKind::String:
    return HeaderKind::String;
  case ::iggy::ffi::FfiHeaderKind::Bool:
    return HeaderKind::Bool;
  case ::iggy::ffi::FfiHeaderKind::Int8:
    return HeaderKind::Int8;
  case ::iggy::ffi::FfiHeaderKind::Int16:
    return HeaderKind::Int16;
  case ::iggy::ffi::FfiHeaderKind::Int32:
    return HeaderKind::Int32;
  case ::iggy::ffi::FfiHeaderKind::Int64:
    return HeaderKind::Int64;
  case ::iggy::ffi::FfiHeaderKind::Int128:
    return HeaderKind::Int128;
  case ::iggy::ffi::FfiHeaderKind::Uint8:
    return HeaderKind::Uint8;
  case ::iggy::ffi::FfiHeaderKind::Uint16:
    return HeaderKind::Uint16;
  case ::iggy::ffi::FfiHeaderKind::Uint32:
    return HeaderKind::Uint32;
  case ::iggy::ffi::FfiHeaderKind::Uint64:
    return HeaderKind::Uint64;
  case ::iggy::ffi::FfiHeaderKind::Uint128:
    return HeaderKind::Uint128;
  case ::iggy::ffi::FfiHeaderKind::Float32:
    return HeaderKind::Float32;
  case ::iggy::ffi::FfiHeaderKind::Float64:
    return HeaderKind::Float64;
  }
  return HeaderKind::Raw;
}

::iggy::ffi::FfiHeaderMap to_ffi_header_map(const HeaderMap &map,
                                            const char *operation) {
  ::iggy::ffi::FfiHeaderMap ffi{};
  ffi.entries.reserve(map.entries.size());
  for (const auto &entry : map.entries) {
    if (entry.key.empty()) {
      throw_validation(operation, "header key must not be empty");
    }
    if (entry.key.size() > 255) {
      throw_validation(operation, "header key must be 1..=255 bytes");
    }
    if (entry.value.value.empty() || entry.value.value.size() > 255) {
      throw_validation(operation, "header value must be 1..=255 bytes");
    }
    ::iggy::ffi::FfiHeaderEntry ffi_entry{};
    ffi_entry.key = ::rust::String(entry.key);
    ffi_entry.value.kind = to_ffi_header_kind(entry.value.kind);
    ffi_entry.value.value = to_rust_bytes(entry.value.value);
    ffi.entries.push_back(std::move(ffi_entry));
  }
  return ffi;
}

HeaderMap from_ffi_header_map(const ::iggy::ffi::FfiHeaderMap &map) {
  HeaderMap result;
  result.entries.reserve(map.entries.size());
  for (const auto &entry : map.entries) {
    HeaderEntry out;
    out.key = to_std_string(entry.key);
    out.value.kind = from_ffi_header_kind(entry.value.kind);
    out.value.value = from_rust_bytes(entry.value.value);
    result.entries.push_back(std::move(out));
  }
  return result;
}

std::uint32_t user_headers_length(const HeaderMap &map, const char *operation) {
  std::uint64_t size = 0;
  for (const auto &entry : map.entries) {
    size += 4 + entry.key.size() + 1 + 4 + entry.value.value.size();
  }
  if (size > static_cast<std::uint64_t>(
                 std::numeric_limits<std::uint32_t>::max())) {
    throw_validation(operation, "user headers length exceeds u32::MAX");
  }
  return static_cast<std::uint32_t>(size);
}

::iggy::ffi::FfiPollingStrategy
to_ffi_polling_strategy(const PollingStrategy &strategy) {
  ::iggy::ffi::FfiPollingStrategy ffi{};
  switch (strategy.kind) {
  case PollingKind::Offset:
    ffi.kind = ::iggy::ffi::FfiPollingKind::Offset;
    break;
  case PollingKind::Timestamp:
    ffi.kind = ::iggy::ffi::FfiPollingKind::Timestamp;
    break;
  case PollingKind::First:
    ffi.kind = ::iggy::ffi::FfiPollingKind::First;
    break;
  case PollingKind::Last:
    ffi.kind = ::iggy::ffi::FfiPollingKind::Last;
    break;
  case PollingKind::Next:
    ffi.kind = ::iggy::ffi::FfiPollingKind::Next;
    break;
  }
  ffi.value = strategy.value;
  return ffi;
}

::iggy::ffi::FfiIggyMessage to_ffi_message(const IggyMessage &message,
                                          const char *operation) {
  if (!message.message_header.id.empty() &&
      message.message_header.id.size() != 16) {
    throw_validation(operation, "message id must be 16 bytes or empty");
  }
  if (message.payload_bytes.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    throw_validation(operation, "payload length exceeds u32::MAX");
  }
  ::iggy::ffi::FfiIggyMessageHeader header{};
  header.checksum = message.message_header.checksum;
  header.id = to_rust_bytes(message.message_header.id);
  header.offset = message.message_header.offset;
  header.timestamp = message.message_header.timestamp;
  header.origin_timestamp = message.message_header.origin_timestamp;
  header.payload_length = static_cast<std::uint32_t>(message.payload_bytes.size());

  ::iggy::ffi::FfiHeaderMap headers = to_ffi_header_map(message.headers, operation);
  header.user_headers_length = user_headers_length(message.headers, operation);
  ::iggy::ffi::FfiIggyMessage ffi{header, to_rust_bytes(message.payload_bytes),
                                 headers};
  return ffi;
}

IggyMessage from_ffi_message(const ::iggy::ffi::FfiIggyMessage &message) {
  IggyMessage out;
  out.message_header.checksum = message.header.checksum;
  out.message_header.id = from_rust_bytes(message.header.id);
  out.message_header.offset = message.header.offset;
  out.message_header.timestamp = message.header.timestamp;
  out.message_header.origin_timestamp = message.header.origin_timestamp;
  out.message_header.user_headers_length = message.header.user_headers_length;
  out.message_header.payload_length = message.header.payload_length;
  out.payload_bytes = from_rust_bytes(message.payload);
  out.payload_text.assign(out.payload_bytes.begin(), out.payload_bytes.end());
  out.headers = from_ffi_header_map(message.headers);
  return out;
}

CompressionAlgorithm
from_ffi_compression(::iggy::ffi::FfiCompressionAlgorithm algorithm) {
  switch (algorithm) {
  case ::iggy::ffi::FfiCompressionAlgorithm::None:
    return CompressionAlgorithm::None;
  case ::iggy::ffi::FfiCompressionAlgorithm::Gzip:
    return CompressionAlgorithm::Gzip;
  }
  return CompressionAlgorithm::None;
}

std::string compression_to_string(CompressionAlgorithm algorithm) {
  switch (algorithm) {
  case CompressionAlgorithm::None:
    return "none";
  case CompressionAlgorithm::Gzip:
    return "gzip";
  }
  return "none";
}

Partition from_ffi_partition(const ::iggy::ffi::FfiPartition &partition) {
  Partition out;
  out.id = partition.id;
  out.created_at = partition.created_at;
  out.segments_count = partition.segments_count;
  out.current_offset = partition.current_offset;
  out.size_bytes = partition.size_bytes;
  out.messages_count = partition.messages_count;
  return out;
}

Topic from_ffi_topic(const ::iggy::ffi::FfiTopic &topic) {
  Topic out;
  out.id = topic.id;
  out.created_at = topic.created_at;
  out.name = to_std_string(topic.name);
  out.size_bytes = topic.size_bytes;
  out.message_expiry = topic.message_expiry;
  out.compression_algorithm = from_ffi_compression(topic.compression_algorithm);
  out.max_topic_size = topic.max_topic_size;
  out.replication_factor = topic.replication_factor;
  out.messages_count = topic.messages_count;
  out.partitions_count = topic.partitions_count;
  return out;
}

StreamDetails from_ffi_stream_details(const ::iggy::ffi::FfiStreamDetails &details) {
  StreamDetails out;
  out.id = details.id;
  out.created_at = details.created_at;
  out.name = to_std_string(details.name);
  out.size_bytes = details.size_bytes;
  out.messages_count = details.messages_count;
  out.topics_count = details.topics_count;
  out.topics.reserve(details.topics.size());
  for (const auto &topic : details.topics) {
    out.topics.push_back(from_ffi_topic(topic));
  }
  return out;
}

TopicDetails from_ffi_topic_details(const ::iggy::ffi::FfiTopicDetails &details) {
  TopicDetails out;
  out.id = details.id;
  out.created_at = details.created_at;
  out.name = to_std_string(details.name);
  out.size_bytes = details.size_bytes;
  out.message_expiry = details.message_expiry;
  out.compression_algorithm = from_ffi_compression(details.compression_algorithm);
  out.max_topic_size = details.max_topic_size;
  out.replication_factor = details.replication_factor;
  out.messages_count = details.messages_count;
  out.partitions_count = details.partitions_count;
  out.partitions.reserve(details.partitions.size());
  for (const auto &partition : details.partitions) {
    out.partitions.push_back(from_ffi_partition(partition));
  }
  return out;
}

} // namespace detail

} // namespace iggy
