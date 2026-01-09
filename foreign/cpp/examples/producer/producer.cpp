#include "iggy.hpp"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

int main() {
  try {
    std::string address = "127.0.0.1:8090";
    std::string stream = "example-stream";
    std::string topic = "example-topic";
    std::string username = "iggy";
    std::string password = "iggy";
    std::uint32_t count = 10;
    std::uint32_t stream_count = 5;
    std::uint32_t partitions_per_stream = 2;

    auto client = iggy::IggyClient::Builder.create_client(address).connect().login_user(username, password);

    iggy::Identifier topic_id = iggy::Identifier::named(topic);

    std::uint32_t total_partitions = stream_count * partitions_per_stream;
    std::uint32_t messages_per_partition = count / total_partitions;

    std::uint32_t message_number = 1;
    for (std::uint32_t stream_index = 0; stream_index < stream_count; ++stream_index) {
      std::string stream_name = stream + "-" + std::to_string(stream_index + 1);
      auto stream_id = iggy::Identifier::named(stream_name);

      try {
        client.create_stream(stream_name);
      } catch (const iggy::IggyError &err) {
        std::cerr << "create_stream: " << err.what() << '\n';
      }

      try {
        client.create_topic(stream_id, topic, partitions_per_stream, std::string(), 0);
      } catch (const iggy::IggyError &err) {
        std::cerr << "create_topic: " << err.what() << '\n';
      }

      for (std::uint32_t partition_id = 0; partition_id < partitions_per_stream; ++partition_id) {
        std::vector<iggy::IggyMessage> messages;
        messages.reserve(messages_per_partition);
        for (std::uint32_t i = 0; i < messages_per_partition; ++i) {
          std::string payload = "message-" + std::to_string(message_number++);
          messages.emplace_back(iggy::IggyMessage::Builder().payload(payload).header("source", "cpp-example").header("message-id", payload));
        }

        client.send_messages(stream_id, topic_id, partition_id, messages);
        std::cout << "Sent " << messages_per_partition << " messages to "
                  << stream_name
                  << "/" << topic << " partition " << partition_id << ".\n";
      }
    }
    return 0;
  } catch (const iggy::IggyError &err) {
    std::cerr << "Iggy error (" << err.operation() << "): " << err.what()
              << '\n';
  } catch (const std::exception &err) {
    std::cerr << "Error: " << err.what() << '\n';
  }
  return 1;
}
