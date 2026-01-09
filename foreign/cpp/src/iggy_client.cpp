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

#include <utility>

#include "iggy.hpp"

namespace iggy {

    IggyClient IggyClient::ClientBuilder::create_client(const std::string& conn) const {
        return IggyClient::create_client(conn);
    }

    IggyClient IggyClient::create_client(const std::string& conn) {
        auto client = ::iggy::ffi::create_client(::rust::Str(conn));
        return IggyClient(std::move(client));
    }

    IggyClient::IggyClient(::rust::Box<::iggy::ffi::FfiIggyClient> inner) : inner_(std::move(inner)) {}

    IggyClient::IggyClient(IggyClient&&) noexcept = default;

    IggyClient& IggyClient::operator=(IggyClient&&) noexcept = default;

    IggyClient::~IggyClient() = default;

    IggyClient& IggyClient::login_user(const std::string& username, const std::string& password) & {
        inner_->login_user(::rust::Str(username), ::rust::Str(password));
        return *this;
    }

    IggyClient&& IggyClient::login_user(const std::string& username, const std::string& password) && {
        inner_->login_user(::rust::Str(username), ::rust::Str(password));
        return std::move(*this);
    }

    IggyClient& IggyClient::connect() & {
        inner_->connect();
        return *this;
    }

    IggyClient&& IggyClient::connect() && {
        inner_->connect();
        return std::move(*this);
    }

    IggyClient& IggyClient::ping() & {
        inner_->ping();
        return *this;
    }

    IggyClient&& IggyClient::ping() && {
        inner_->ping();
        return std::move(*this);
    }

    IggyClient& IggyClient::create_stream(const std::string& name) & {
        inner_->create_stream(::rust::Str(name));
        return *this;
    }

    IggyClient&& IggyClient::create_stream(const std::string& name) && {
        inner_->create_stream(::rust::Str(name));
        return std::move(*this);
    }

    StreamDetails IggyClient::get_stream(const Identifier& stream_id) {
        auto ffi_id = detail::cpp_to_ffi_identifier(stream_id);
        auto details = inner_->get_stream(ffi_id);
        return detail::ffi_stream_details_to_cpp(*details);
    }

    IggyClient& IggyClient::create_topic(const Identifier& stream, const std::string& name,
                                         std::uint32_t partitions_count, CompressionAlgorithm compression_algorithm,
                                         std::uint8_t replication_factor) & {
        auto compression = detail::compression_to_string(compression_algorithm);
        auto ffi_stream = detail::cpp_to_ffi_identifier(stream);
        inner_->create_topic(ffi_stream, ::rust::Str(name), partitions_count, ::rust::Str(compression),
                             replication_factor);
        return *this;
    }

    IggyClient&& IggyClient::create_topic(const Identifier& stream, const std::string& name,
                                          std::uint32_t partitions_count, CompressionAlgorithm compression_algorithm,
                                          std::uint8_t replication_factor) && {
        auto compression = detail::compression_to_string(compression_algorithm);
        auto ffi_stream = detail::cpp_to_ffi_identifier(stream);
        inner_->create_topic(ffi_stream, ::rust::Str(name), partitions_count, ::rust::Str(compression),
                             replication_factor);
        return std::move(*this);
    }

    TopicDetails IggyClient::get_topic(const Identifier& stream_id, const Identifier& topic_id) {
        auto ffi_stream = detail::cpp_to_ffi_identifier(stream_id);
        auto ffi_topic = detail::cpp_to_ffi_identifier(topic_id);
        auto details = inner_->get_topic(ffi_stream, ffi_topic);
        return detail::ffi_topic_details_to_cpp(*details);
    }

    IggyClient& IggyClient::send_messages(const Identifier& stream, const Identifier& topic,
                                          const Partitioning& partitioning,
                                          const std::vector<IggyMessage>& messages) & {
        auto ffi_stream = detail::cpp_to_ffi_identifier(stream);
        auto ffi_topic = detail::cpp_to_ffi_identifier(topic);
        ::rust::Vec<::iggy::ffi::FfiIggyMessage> ffi_messages;
        ffi_messages.reserve(messages.size());
        for (const auto& message : messages) {
            ffi_messages.push_back(detail::cpp_to_ffi_message(message));
        }
        inner_->send_messages(ffi_stream, ffi_topic, partitioning.partition_id, ffi_messages);
        return *this;
    }

    IggyClient&& IggyClient::send_messages(const Identifier& stream, const Identifier& topic,
                                           const Partitioning& partitioning,
                                           const std::vector<IggyMessage>& messages) && {
        auto ffi_stream = detail::cpp_to_ffi_identifier(stream);
        auto ffi_topic = detail::cpp_to_ffi_identifier(topic);
        ::rust::Vec<::iggy::ffi::FfiIggyMessage> ffi_messages;
        ffi_messages.reserve(messages.size());
        for (const auto& message : messages) {
            ffi_messages.push_back(detail::cpp_to_ffi_message(message));
        }
        inner_->send_messages(ffi_stream, ffi_topic, partitioning.partition_id, ffi_messages);
        return std::move(*this);
    }
    std::vector<IggyMessage> IggyClient::poll_messages(const Identifier& stream, const Identifier& topic,
                                                       const Partitioning& partitioning,
                                                       const PollingStrategy& strategy, std::uint32_t count,
                                                       bool auto_commit) & {
        auto ffi_stream = detail::cpp_to_ffi_identifier(stream);
        auto ffi_topic = detail::cpp_to_ffi_identifier(topic);
        auto ffi_strategy = detail::cpp_to_ffi_polling_strategy(strategy);
        auto messages = inner_->poll_messages(ffi_stream, ffi_topic, partitioning.partition_id, ffi_strategy, count,
                                              auto_commit);
        std::vector<IggyMessage> out;
        out.reserve(messages.size());
        for (const auto& message : messages) {
            out.push_back(detail::ffi_message_to_cpp(message));
        }
        return out;
    }

    std::vector<IggyMessage> IggyClient::poll_messages(const Identifier& stream, const Identifier& topic,
                                                       const Partitioning& partitioning,
                                                       const PollingStrategy& strategy, std::uint32_t count,
                                                       bool auto_commit) && {
        return static_cast<IggyClient&>(*this).poll_messages(stream, topic, partitioning, strategy, count, auto_commit);
    }

}  // namespace iggy
