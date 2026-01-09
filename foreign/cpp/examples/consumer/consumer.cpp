// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "iggy.hpp"

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

        auto topic_id = iggy::Identifier::named(topic);

        auto strategy = iggy::PollingStrategy::next();

        std::uint32_t total_partitions = stream_count * partitions_per_stream;
        std::uint32_t messages_per_partition = count / total_partitions;

        for (std::uint32_t stream_index = 0; stream_index < stream_count; ++stream_index) {
            std::string stream_name = stream + "-" + std::to_string(stream_index + 1);
            auto stream_id = iggy::Identifier::named(stream_name);

            for (std::uint32_t partition_id = 0; partition_id < partitions_per_stream; ++partition_id) {
                auto messages = client.poll_messages(stream_id, topic_id, iggy::Partitioning(partition_id), strategy,
                                                     messages_per_partition, true);

                std::cout << "Polled " << messages.size() << " messages from " << stream_name << "/" << topic
                          << " partition " << partition_id << ".\n";
                for (std::size_t i = 0; i < messages.size(); ++i) {
                    const auto& message = messages[i];
                    std::cout << "message[" << i << "]: " << message.payload_text << '\n';
                    for (const auto& header : message.headers.entries) {
                        std::cout << "  header[" << header.key << "]: " << header.value.text() << '\n';
                    }
                }
            }
        }
        return 0;
    } catch (const std::exception& err) {
        std::cerr << "Error: " << err.what() << '\n';
    }
    return 1;
}
