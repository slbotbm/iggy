#include "rust/cxx.h"
#include "iggy/src/lib.rs.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {
std::string env_or_default(const char *key, const char *fallback) {
  const char *value = std::getenv(key);
  if (value == nullptr || *value == '\0') {
    return std::string(fallback);
  }
  return std::string(value);
}

std::uint32_t env_u32(const char *key, std::uint32_t fallback) {
  const char *value = std::getenv(key);
  if (value == nullptr || *value == '\0') {
    return fallback;
  }
  char *end = nullptr;
  unsigned long parsed = std::strtoul(value, &end, 10);
  if (end == value || *end != '\0') {
    return fallback;
  }
  return static_cast<std::uint32_t>(parsed);
}

rust::Vec<std::uint8_t> to_bytes(const std::string &text) {
  rust::Vec<std::uint8_t> bytes;
  bytes.reserve(text.size());
  for (unsigned char ch : text) {
    bytes.push_back(static_cast<std::uint8_t>(ch));
  }
  return bytes;
}

iggy::ffi::FfiIdentifier make_string_identifier(const std::string &value) {
  if (value.empty()) {
    throw std::runtime_error("identifier must not be empty");
  }
  if (value.size() > 255) {
    throw std::runtime_error("identifier length must be <= 255 bytes");
  }
  iggy::ffi::FfiIdentifier id{};
  id.kind = iggy::ffi::FfiIdentifierKind::String;
  id.length = static_cast<std::uint8_t>(value.size());
  id.value = to_bytes(value);
  return id;
}

std::string bytes_to_string(const rust::Vec<std::uint8_t> &bytes) {
  if (bytes.empty()) {
    return std::string();
  }
  return std::string(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}
} // namespace

int main() {
  try {
    std::string address = env_or_default("ADDR", "127.0.0.1:8090");
    std::string stream = env_or_default("STREAM", "example-stream");
    std::string topic = env_or_default("TOPIC", "example-topic");
    std::string username = env_or_default("USERNAME", "iggy");
    std::string password = env_or_default("PASSWORD", "iggy");
    std::uint32_t count = env_u32("COUNT", 100);
    std::uint32_t stream_count = 5;
    std::uint32_t partitions_per_stream = 2;

    auto client = iggy::ffi::create_client(rust::String(address));
    client->connect();
    client->login_user(rust::String(username), rust::String(password));

    iggy::ffi::FfiIdentifier topic_id = make_string_identifier(topic);

    iggy::ffi::FfiPollingStrategy strategy{};
    strategy.kind = iggy::ffi::FfiPollingKind::Next;
    strategy.value = 0;

    std::uint32_t total_partitions = stream_count * partitions_per_stream;
    std::uint32_t base_messages = count / total_partitions;
    std::uint32_t extra_messages = count % total_partitions;
    if (base_messages == 0) {
      std::cerr << "COUNT must be at least " << total_partitions << ".\n";
      return 1;
    }

    for (std::uint32_t stream_index = 0; stream_index < stream_count; ++stream_index) {
      std::string stream_name = stream + "-" + std::to_string(stream_index + 1);
      iggy::ffi::FfiIdentifier stream_id = make_string_identifier(stream_name);

      for (std::uint32_t partition_id = 0; partition_id < partitions_per_stream;
           ++partition_id) {
        std::uint32_t partition_index =
            stream_index * partitions_per_stream + partition_id;
        std::uint32_t messages_in_partition =
            base_messages + (partition_index < extra_messages ? 1U : 0U);

        auto messages = client->poll_messages(stream_id, topic_id, partition_id,
                                              strategy, messages_in_partition, true);

        std::cout << "Polled " << messages.size() << " messages from "
                  << stream_name << "/" << topic << " partition " << partition_id
                  << ".\n";
        for (std::size_t i = 0; i < messages.size(); ++i) {
          const auto &message = messages[i];
          std::cout << "message[" << i << "]: " << bytes_to_string(message.payload)
                    << '\n';
        }
      }
    }
    return 0;
  } catch (const rust::Error &err) {
    std::cerr << "Iggy error: " << err.what() << '\n';
  } catch (const std::exception &err) {
    std::cerr << "Error: " << err.what() << '\n';
  }
  return 1;
}
