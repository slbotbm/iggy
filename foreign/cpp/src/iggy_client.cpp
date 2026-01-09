#include "iggy.hpp"

#include <utility>

namespace {

template <typename F>
auto with_ffi(const char *operation, F &&func) -> decltype(func()) {
  try {
    return func();
  } catch (const ::rust::Error &err) {
    throw iggy::IggyError(operation, iggy::ErrorCategory::Ffi, err.what());
  }
}

} // namespace

namespace iggy {

IggyClient IggyClient::ClientBuilder::create_client(
    const std::string &conn) const {
  return IggyClient::create_client(conn);
}

IggyClient IggyClient::create_client(const std::string &conn) {
  return with_ffi("create_client", [&]() {
    auto client = ::iggy::ffi::create_client(::rust::Str(conn));
    return IggyClient(std::move(client));
  });
}

IggyClient::IggyClient(::rust::Box<::iggy::ffi::IggyClient> inner)
    : inner_(std::move(inner)) {}

IggyClient::IggyClient(IggyClient &&) noexcept = default;

IggyClient &IggyClient::operator=(IggyClient &&) noexcept = default;

IggyClient::~IggyClient() = default;

IggyClient &IggyClient::login_user(const std::string &username,
                                   const std::string &password) & {
  with_ffi("login_user", [&]() {
    inner_->login_user(::rust::Str(username), ::rust::Str(password));
  });
  return *this;
}

IggyClient &&IggyClient::login_user(const std::string &username,
                                    const std::string &password) && {
  with_ffi("login_user", [&]() {
    inner_->login_user(::rust::Str(username), ::rust::Str(password));
  });
  return std::move(*this);
}

IggyClient &IggyClient::connect() & {
  with_ffi("connect", [&]() { inner_->connect(); });
  return *this;
}

IggyClient &&IggyClient::connect() && {
  with_ffi("connect", [&]() { inner_->connect(); });
  return std::move(*this);
}

IggyClient &IggyClient::ping() & {
  with_ffi("ping", [&]() { inner_->ping(); });
  return *this;
}

IggyClient &&IggyClient::ping() && {
  with_ffi("ping", [&]() { inner_->ping(); });
  return std::move(*this);
}

IggyClient &IggyClient::create_stream(const std::string &name) & {
  with_ffi("create_stream", [&]() { inner_->create_stream(::rust::Str(name)); });
  return *this;
}

IggyClient &&IggyClient::create_stream(const std::string &name) && {
  with_ffi("create_stream", [&]() { inner_->create_stream(::rust::Str(name)); });
  return std::move(*this);
}

StreamDetails IggyClient::get_stream(const Identifier &stream_id) {
  auto ffi_id = detail::to_ffi_identifier(stream_id);
  return with_ffi("get_stream", [&]() {
    auto details = inner_->get_stream(ffi_id);
    return detail::from_ffi_stream_details(*details);
  });
}

IggyClient &IggyClient::create_topic(const Identifier &stream,
                                     const std::string &name,
                                     std::uint32_t partitions_count,
                                     const std::string &compression_algorithm,
                                     std::uint8_t replication_factor) & {
  auto ffi_stream = detail::to_ffi_identifier(stream);
  with_ffi("create_topic", [&]() {
    inner_->create_topic(ffi_stream, ::rust::Str(name), partitions_count,
                         ::rust::Str(compression_algorithm), replication_factor);
  });
  return *this;
}

IggyClient &&IggyClient::create_topic(const Identifier &stream,
                                      const std::string &name,
                                      std::uint32_t partitions_count,
                                      const std::string &compression_algorithm,
                                      std::uint8_t replication_factor) && {
  auto ffi_stream = detail::to_ffi_identifier(stream);
  with_ffi("create_topic", [&]() {
    inner_->create_topic(ffi_stream, ::rust::Str(name), partitions_count,
                         ::rust::Str(compression_algorithm), replication_factor);
  });
  return std::move(*this);
}

IggyClient &IggyClient::create_topic(const Identifier &stream,
                                     const std::string &name,
                                     std::uint32_t partitions_count,
                                     CompressionAlgorithm compression_algorithm,
                                     std::uint8_t replication_factor) & {
  return create_topic(stream, name, partitions_count,
                      detail::compression_to_string(compression_algorithm),
                      replication_factor);
}

IggyClient &&IggyClient::create_topic(const Identifier &stream,
                                      const std::string &name,
                                      std::uint32_t partitions_count,
                                      CompressionAlgorithm compression_algorithm,
                                      std::uint8_t replication_factor) && {
  auto compression = detail::compression_to_string(compression_algorithm);
  auto ffi_stream = detail::to_ffi_identifier(stream);
  with_ffi("create_topic", [&]() {
    inner_->create_topic(ffi_stream, ::rust::Str(name), partitions_count,
                         ::rust::Str(compression), replication_factor);
  });
  return std::move(*this);
}

TopicDetails IggyClient::get_topic(const Identifier &stream_id,
                                  const Identifier &topic_id) {
  auto ffi_stream = detail::to_ffi_identifier(stream_id);
  auto ffi_topic = detail::to_ffi_identifier(topic_id);
  return with_ffi("get_topic", [&]() {
    auto details = inner_->get_topic(ffi_stream, ffi_topic);
    return detail::from_ffi_topic_details(*details);
  });
}

IggyClient &IggyClient::send_messages(const Identifier &stream,
                                      const Identifier &topic,
                                      std::uint32_t partition_id,
                                      const std::vector<IggyMessage> &messages) & {
  return send_messages(stream, topic, Partitioning(partition_id), messages);
}

IggyClient &&IggyClient::send_messages(const Identifier &stream,
                                       const Identifier &topic,
                                       std::uint32_t partition_id,
                                       const std::vector<IggyMessage> &messages) && {
  return std::move(send_messages(stream, topic, Partitioning(partition_id), messages));
}

IggyClient &IggyClient::send_messages(const Identifier &stream,
                                      const Identifier &topic,
                                      const Partitioning &partitioning,
                                      const std::vector<IggyMessage> &messages) & {
  auto ffi_stream = detail::to_ffi_identifier(stream);
  auto ffi_topic = detail::to_ffi_identifier(topic);
  ::rust::Vec<::iggy::ffi::FfiIggyMessage> ffi_messages;
  ffi_messages.reserve(messages.size());
  for (const auto &message : messages) {
    ffi_messages.push_back(detail::to_ffi_message(message, "send_messages"));
  }
  with_ffi("send_messages", [&]() {
    inner_->send_messages(ffi_stream, ffi_topic, partitioning.partition_id,
                          ffi_messages);
  });
  return *this;
}

IggyClient &&IggyClient::send_messages(const Identifier &stream,
                                       const Identifier &topic,
                                       const Partitioning &partitioning,
                                       const std::vector<IggyMessage> &messages) && {
  auto ffi_stream = detail::to_ffi_identifier(stream);
  auto ffi_topic = detail::to_ffi_identifier(topic);
  ::rust::Vec<::iggy::ffi::FfiIggyMessage> ffi_messages;
  ffi_messages.reserve(messages.size());
  for (const auto &message : messages) {
    ffi_messages.push_back(detail::to_ffi_message(message, "send_messages"));
  }
  with_ffi("send_messages", [&]() {
    inner_->send_messages(ffi_stream, ffi_topic, partitioning.partition_id,
                          ffi_messages);
  });
  return std::move(*this);
}
std::vector<IggyMessage> IggyClient::poll_messages(
    const Identifier &stream, const Identifier &topic,
    std::uint32_t partition_id, const PollingStrategy &strategy,
    std::uint32_t count, bool auto_commit) {
  return poll_messages(stream, topic, Partitioning(partition_id), strategy, count,
                       auto_commit);
}

std::vector<IggyMessage> IggyClient::poll_messages(
    const Identifier &stream, const Identifier &topic,
    const Partitioning &partitioning, const PollingStrategy &strategy,
    std::uint32_t count, bool auto_commit) {
  auto ffi_stream = detail::to_ffi_identifier(stream);
  auto ffi_topic = detail::to_ffi_identifier(topic);
  auto ffi_strategy = detail::to_ffi_polling_strategy(strategy);
  return with_ffi("poll_messages", [&]() {
    auto messages =
        inner_->poll_messages(ffi_stream, ffi_topic, partitioning.partition_id,
                              ffi_strategy, count, auto_commit);
    std::vector<IggyMessage> out;
    out.reserve(messages.size());
    for (const auto &message : messages) {
      out.push_back(detail::from_ffi_message(message));
    }
    return out;
  });
}

} // namespace iggy
