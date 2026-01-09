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

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "iggy-cpp/src/lib.rs.h"
#include "rust/cxx.h"

namespace iggy {

    class Identifier {
       public:
        enum class Kind {
            Numeric,
            String,
        };

        static Identifier numeric(std::uint32_t value);
        static Identifier named(const std::string& value);

        Kind kind() const noexcept;
        const std::vector<std::uint8_t>& value() const noexcept;
        std::uint8_t length() const noexcept;

       private:
        Identifier(Kind kind, std::vector<std::uint8_t> value);

        Kind kind_;
        std::vector<std::uint8_t> value_;
    };  // class Identifier

    enum class HeaderKind {
        Raw = 1,
        String = 2,
        Bool = 3,
        Int8 = 4,
        Int16 = 5,
        Int32 = 6,
        Int64 = 7,
        Int128 = 8,
        Uint8 = 9,
        Uint16 = 10,
        Uint32 = 11,
        Uint64 = 12,
        Uint128 = 13,
        Float32 = 14,
        Float64 = 15,
    };

    struct HeaderValue {
        HeaderKind kind = HeaderKind::Raw;
        std::vector<std::uint8_t> value;

        std::string text() const;
    };

    struct HeaderEntry {
        std::string key;
        HeaderValue value;
    };

    struct HeaderMap {
        std::vector<HeaderEntry> entries;
    };

    struct IggyByteSize {
        std::uint64_t value = 0;
    };

    enum class MaxTopicSizeKind {
        ServerDefault = 1,
        Custom = 2,
        Unlimited = 3,
    };

    struct MaxTopicSize {
        MaxTopicSizeKind kind = MaxTopicSizeKind::ServerDefault;
        IggyByteSize value;
    };

    struct MessageHeader {
        std::uint64_t checksum = 0;
        std::vector<std::uint8_t> id;
        std::uint64_t offset = 0;
        std::uint64_t timestamp = 0;
        std::uint64_t origin_timestamp = 0;
        std::uint32_t user_headers_length = 0;
        std::uint32_t payload_length = 0;
    };

    class IggyMessageBuilder;

    struct IggyMessage {
        MessageHeader message_header;
        std::vector<std::uint8_t> payload_bytes;
        std::string payload_text;
        HeaderMap headers;

        static IggyMessageBuilder Builder();

        IggyMessage& payload(std::string_view text) &;
        IggyMessage&& payload(std::string_view text) &&;
        IggyMessage& payload(std::vector<std::uint8_t> data) &;
        IggyMessage&& payload(std::vector<std::uint8_t> data) &&;

        MessageHeader& header() &;
        const MessageHeader& header() const&;
        IggyMessage& header(std::string_view key, std::string_view value) &;
        IggyMessage&& header(std::string_view key, std::string_view value) &&;

        IggyMessage& user_headers(HeaderMap map) &;
        IggyMessage&& user_headers(HeaderMap map) &&;
    };  // struct IggyMessage

    class IggyMessageBuilder {
       public:
        IggyMessageBuilder& payload(std::string_view text) &;
        IggyMessageBuilder&& payload(std::string_view text) &&;
        IggyMessageBuilder& payload(std::vector<std::uint8_t> data) &;
        IggyMessageBuilder&& payload(std::vector<std::uint8_t> data) &&;

        IggyMessageBuilder& header(std::string_view key, std::string_view value) &;
        IggyMessageBuilder&& header(std::string_view key, std::string_view value) &&;
        IggyMessageBuilder& user_headers(HeaderMap map) &;
        IggyMessageBuilder&& user_headers(HeaderMap map) &&;

        IggyMessage build() &&;
        operator IggyMessage() &&;

       private:
        IggyMessage message_;
    };

    enum class CompressionAlgorithm {
        None = 1,
        Gzip = 2,
    };

    enum class PollingKind {
        Offset = 1,
        Timestamp = 2,
        First = 3,
        Last = 4,
        Next = 5,
    };

    struct PollingStrategy {
        PollingKind kind = PollingKind::Next;
        std::uint64_t value = 0;

        static PollingStrategy offset(std::uint64_t offset_value);
        static PollingStrategy timestamp(std::uint64_t timestamp_value);
        static PollingStrategy first();
        static PollingStrategy last();
        static PollingStrategy next();
    };

    struct Partitioning {
        explicit Partitioning(std::uint32_t id);
        std::uint32_t partition_id = 0;
    };

    struct Partition {
        std::uint32_t id = 0;
        std::uint64_t created_at = 0;
        std::uint32_t segments_count = 0;
        std::uint64_t current_offset = 0;
        IggyByteSize size;
        std::uint64_t messages_count = 0;
    };

    struct Topic {
        std::uint32_t id = 0;
        std::uint64_t created_at = 0;
        std::string name;
        IggyByteSize size;
        std::uint64_t message_expiry = 0;
        CompressionAlgorithm compression_algorithm = CompressionAlgorithm::None;
        MaxTopicSize max_topic_size;
        std::uint8_t replication_factor = 0;
        std::uint64_t messages_count = 0;
        std::uint32_t partitions_count = 0;
    };

    struct StreamDetails {
        std::uint32_t id = 0;
        std::uint64_t created_at = 0;
        std::string name;
        IggyByteSize size;
        std::uint64_t messages_count = 0;
        std::uint32_t topics_count = 0;
        std::vector<Topic> topics;
    };

    struct TopicDetails {
        std::uint32_t id = 0;
        std::uint64_t created_at = 0;
        std::string name;
        IggyByteSize size;
        std::uint64_t message_expiry = 0;
        CompressionAlgorithm compression_algorithm = CompressionAlgorithm::None;
        MaxTopicSize max_topic_size;
        std::uint8_t replication_factor = 0;
        std::uint64_t messages_count = 0;
        std::uint32_t partitions_count = 0;
        std::vector<Partition> partitions;
    };

    namespace detail {

        std::string rust_string_to_cpp(const ::rust::String& value);
        ::rust::Vec<std::uint8_t> cpp_to_rust_bytes(const std::vector<std::uint8_t>& value);
        std::vector<std::uint8_t> rust_bytes_to_cpp(const ::rust::Vec<std::uint8_t>& value);

        ::iggy::ffi::FfiIdentifier cpp_to_ffi_identifier(const Identifier& identifier);

        ::iggy::ffi::FfiHeaderKind cpp_to_ffi_header_kind(HeaderKind kind);
        HeaderKind ffi_header_kind_to_cpp(::iggy::ffi::FfiHeaderKind kind);

        ::iggy::ffi::FfiHeaderMap cpp_to_ffi_header_map(const HeaderMap& map);
        HeaderMap ffi_header_map_to_cpp(const ::iggy::ffi::FfiHeaderMap& map);

        ::iggy::ffi::FfiPollingStrategy cpp_to_ffi_polling_strategy(const PollingStrategy& strategy);

        ::iggy::ffi::FfiIggyMessage cpp_to_ffi_message(const IggyMessage& message);
        IggyMessage ffi_message_to_cpp(const ::iggy::ffi::FfiIggyMessage& message);

        CompressionAlgorithm ffi_compression_to_cpp(::iggy::ffi::FfiCompressionAlgorithm algorithm);
        std::string compression_to_string(CompressionAlgorithm algorithm);

        Partition ffi_partition_to_cpp(const ::iggy::ffi::FfiPartition& partition);

        Topic ffi_topic_to_cpp(const ::iggy::ffi::FfiTopic& topic);

        StreamDetails ffi_stream_details_to_cpp(const ::iggy::ffi::FfiStreamDetails& details);

        TopicDetails ffi_topic_details_to_cpp(const ::iggy::ffi::FfiTopicDetails& details);

    }  // namespace detail

    class IggyClient {
       public:
        struct ClientBuilder {
            IggyClient create_client(const std::string& conn = std::string()) const;
        };

        static inline const ClientBuilder Builder{};

        static IggyClient create_client(const std::string& conn = std::string());

        IggyClient(IggyClient&&) noexcept;
        IggyClient& operator=(IggyClient&&) noexcept;
        IggyClient(const IggyClient&) = delete;
        IggyClient& operator=(const IggyClient&) = delete;
        ~IggyClient();

        IggyClient& login_user(const std::string& username, const std::string& password) &;
        IggyClient&& login_user(const std::string& username, const std::string& password) &&;
        IggyClient& connect() &;
        IggyClient&& connect() &&;
        IggyClient& ping() &;
        IggyClient&& ping() &&;

        IggyClient& create_stream(const std::string& name) &;
        IggyClient&& create_stream(const std::string& name) &&;
        StreamDetails get_stream(const Identifier& stream_id);

        IggyClient& create_topic(const Identifier& stream, const std::string& name, std::uint32_t partitions_count,
                                 CompressionAlgorithm compression_algorithm, std::uint8_t replication_factor) &;
        IggyClient&& create_topic(const Identifier& stream, const std::string& name, std::uint32_t partitions_count,
                                  CompressionAlgorithm compression_algorithm, std::uint8_t replication_factor) &&;

        TopicDetails get_topic(const Identifier& stream_id, const Identifier& topic_id);

        IggyClient& send_messages(const Identifier& stream, const Identifier& topic, const Partitioning& partitioning,
                                  const std::vector<IggyMessage>& messages) &;
        IggyClient&& send_messages(const Identifier& stream, const Identifier& topic, const Partitioning& partitioning,
                                   const std::vector<IggyMessage>& messages) &&;

        std::vector<IggyMessage> poll_messages(const Identifier& stream, const Identifier& topic,
                                               const Partitioning& partitioning, const PollingStrategy& strategy,
                                               std::uint32_t count, bool auto_commit) &;
        std::vector<IggyMessage> poll_messages(const Identifier& stream, const Identifier& topic,
                                               const Partitioning& partitioning, const PollingStrategy& strategy,
                                               std::uint32_t count, bool auto_commit) &&;

       private:
        explicit IggyClient(::rust::Box<::iggy::ffi::FfiIggyClient> inner);

        ::rust::Box<::iggy::ffi::FfiIggyClient> inner_;
    };  // class IggyClient

}  // namespace iggy
