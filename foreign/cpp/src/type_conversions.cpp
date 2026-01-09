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

#include <limits>
#include <utility>

#include "iggy.hpp"

namespace iggy {

	Identifier::Identifier(Kind kind, std::vector<std::uint8_t> value) : kind_(kind), value_(std::move(value)) {}

	Identifier Identifier::numeric(std::uint32_t value) {
		std::vector<std::uint8_t> bytes(4);
		bytes[0] = static_cast<std::uint8_t>(value & 0xffU);
		bytes[1] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
		bytes[2] = static_cast<std::uint8_t>((value >> 16U) & 0xffU);
		bytes[3] = static_cast<std::uint8_t>((value >> 24U) & 0xffU);
		return Identifier(Kind::Numeric, std::move(bytes));
	}

	Identifier Identifier::named(const std::string& value) {
		std::vector<std::uint8_t> bytes;
		bytes.reserve(value.size());
		for (unsigned char ch : value) {
			bytes.push_back(static_cast<std::uint8_t>(ch));
		}
		return Identifier(Kind::String, std::move(bytes));
	}

	Identifier::Kind Identifier::kind() const noexcept {
		return kind_;
	}

	const std::vector<std::uint8_t>& Identifier::value() const noexcept {
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
		return std::string(reinterpret_cast<const char*>(value.data()), value.size());
	}

	MessageHeader& IggyMessage::header() & {
		return message_header;
	}

	const MessageHeader& IggyMessage::header() const& {
		return message_header;
	}

	IggyMessage& IggyMessage::payload(std::string_view text) & {
		payload_bytes.clear();
		payload_bytes.reserve(text.size());
		for (unsigned char ch : text) {
			payload_bytes.push_back(static_cast<std::uint8_t>(ch));
		}
		payload_text.assign(text.data(), text.size());
		message_header.payload_length = static_cast<std::uint32_t>(payload_bytes.size());
		return *this;
	}

	IggyMessage&& IggyMessage::payload(std::string_view text) && {
		payload(text);
		return std::move(*this);
	}

	IggyMessage& IggyMessage::payload(std::vector<std::uint8_t> data) & {
		payload_bytes = std::move(data);
		payload_text.assign(payload_bytes.begin(), payload_bytes.end());
		message_header.payload_length = static_cast<std::uint32_t>(payload_bytes.size());
		return *this;
	}

	IggyMessage&& IggyMessage::payload(std::vector<std::uint8_t> data) && {
		payload(std::move(data));
		return std::move(*this);
	}

	IggyMessage& IggyMessage::header(std::string_view key, std::string_view value) & {
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

	IggyMessage&& IggyMessage::header(std::string_view key, std::string_view value) && {
		header(key, value);
		return std::move(*this);
	}

	IggyMessage& IggyMessage::user_headers(HeaderMap map) & {
		headers = std::move(map);
		return *this;
	}

	IggyMessage&& IggyMessage::user_headers(HeaderMap map) && {
		headers = std::move(map);
		return std::move(*this);
	}

	IggyMessageBuilder IggyMessage::Builder() {
		return IggyMessageBuilder();
	}

	IggyMessageBuilder& IggyMessageBuilder::payload(std::string_view text) & {
		message_.payload(text);
		return *this;
	}

	IggyMessageBuilder&& IggyMessageBuilder::payload(std::string_view text) && {
		message_.payload(text);
		return std::move(*this);
	}

	IggyMessageBuilder& IggyMessageBuilder::payload(std::vector<std::uint8_t> data) & {
		message_.payload(std::move(data));
		return *this;
	}

	IggyMessageBuilder&& IggyMessageBuilder::payload(std::vector<std::uint8_t> data) && {
		message_.payload(std::move(data));
		return std::move(*this);
	}

	IggyMessageBuilder& IggyMessageBuilder::header(std::string_view key, std::string_view value) & {
		message_.header(key, value);
		return *this;
	}

	IggyMessageBuilder&& IggyMessageBuilder::header(std::string_view key, std::string_view value) && {
		message_.header(key, value);
		return std::move(*this);
	}

	IggyMessageBuilder& IggyMessageBuilder::user_headers(HeaderMap map) & {
		message_.user_headers(std::move(map));
		return *this;
	}

	IggyMessageBuilder&& IggyMessageBuilder::user_headers(HeaderMap map) && {
		message_.user_headers(std::move(map));
		return std::move(*this);
	}

	IggyMessage IggyMessageBuilder::build() && {
		return std::move(message_);
	}

	IggyMessageBuilder::operator IggyMessage() && {
		return std::move(message_);
	}

	namespace detail {

		std::string rust_string_to_cpp(const ::rust::String& value) {
			return std::string(value);
		}

		::rust::Vec<std::uint8_t> cpp_to_rust_bytes(const std::vector<std::uint8_t>& value) {
			::rust::Vec<std::uint8_t> bytes;
			bytes.reserve(value.size());
			for (auto byte : value) {
				bytes.push_back(byte);
			}
			return bytes;
		}

		std::vector<std::uint8_t> rust_bytes_to_cpp(const ::rust::Vec<std::uint8_t>& value) {
			std::vector<std::uint8_t> bytes;
			bytes.reserve(value.size());
			for (auto byte : value) {
				bytes.push_back(byte);
			}
			return bytes;
		}

		::iggy::ffi::FfiIdentifier cpp_to_ffi_identifier(const Identifier& identifier) {
			::iggy::ffi::FfiIdentifier ffi{};
			ffi.kind = identifier.kind() == Identifier::Kind::Numeric ? ::iggy::ffi::FfiIdKind::Numeric
																	  : ::iggy::ffi::FfiIdKind::String;
			ffi.length = identifier.length();
			ffi.value = cpp_to_rust_bytes(identifier.value());
			return ffi;
		}

		::iggy::ffi::FfiHeaderKind cpp_to_ffi_header_kind(HeaderKind kind) {
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

		HeaderKind ffi_header_kind_to_cpp(::iggy::ffi::FfiHeaderKind kind) {
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

		::iggy::ffi::FfiHeaderMap cpp_to_ffi_header_map(const HeaderMap& map) {
			::iggy::ffi::FfiHeaderMap ffi{};
			ffi.entries.reserve(map.entries.size());
			for (const auto& entry : map.entries) {
				::iggy::ffi::FfiHeaderEntry ffi_entry{};
				ffi_entry.key.value = ::rust::String(entry.key);
				ffi_entry.value.kind = cpp_to_ffi_header_kind(entry.value.kind);
				ffi_entry.value.value = cpp_to_rust_bytes(entry.value.value);
				ffi.entries.push_back(std::move(ffi_entry));
			}
			return ffi;
		}

		HeaderMap ffi_header_map_to_cpp(const ::iggy::ffi::FfiHeaderMap& map) {
			HeaderMap result;
			result.entries.reserve(map.entries.size());
			for (const auto& entry : map.entries) {
				HeaderEntry out;
				out.key = rust_string_to_cpp(entry.key.value);
				out.value.kind = ffi_header_kind_to_cpp(entry.value.kind);
				out.value.value = rust_bytes_to_cpp(entry.value.value);
				result.entries.push_back(std::move(out));
			}
			return result;
		}

		std::uint32_t user_headers_length(const HeaderMap& map) {
			std::uint64_t size = 0;
			for (const auto& entry : map.entries) {
				size += 4 + entry.key.size() + 1 + 4 + entry.value.value.size();
			}
			return static_cast<std::uint32_t>(size);
		}

		::iggy::ffi::FfiPollingStrategy cpp_to_ffi_polling_strategy(const PollingStrategy& strategy) {
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

		::iggy::ffi::FfiIggyExpiry cpp_to_ffi_expiry(std::uint64_t expiry) {
			::iggy::ffi::FfiIggyExpiry ffi{};
			if (expiry == 0) {
				ffi.kind = ::iggy::ffi::FfiIggyExpiryKind::ServerDefault;
				ffi.value = 0;
			} else if (expiry == std::numeric_limits<std::uint64_t>::max()) {
				ffi.kind = ::iggy::ffi::FfiIggyExpiryKind::NeverExpire;
				ffi.value = expiry;
			} else {
				ffi.kind = ::iggy::ffi::FfiIggyExpiryKind::ExpireDuration;
				ffi.value = expiry;
			}
			return ffi;
		}

		::iggy::ffi::FfiMaxTopicSize cpp_to_ffi_max_topic_size(const MaxTopicSize& size) {
			::iggy::ffi::FfiMaxTopicSize ffi{};
			switch (size.kind) {
				case MaxTopicSizeKind::ServerDefault:
					ffi.kind = ::iggy::ffi::FfiMaxTopicSizeKind::ServerDefault;
					ffi.value.value = 0;
					break;
				case MaxTopicSizeKind::Custom:
					ffi.kind = ::iggy::ffi::FfiMaxTopicSizeKind::Custom;
					ffi.value.value = size.value.value;
					break;
				case MaxTopicSizeKind::Unlimited:
					ffi.kind = ::iggy::ffi::FfiMaxTopicSizeKind::Unlimited;
					ffi.value.value = std::numeric_limits<std::uint64_t>::max();
					break;
			}
			return ffi;
		}

		::iggy::ffi::FfiIggyMessage cpp_to_ffi_message(const IggyMessage& message) {
			::iggy::ffi::FfiIggyMessageHeader header{};
			header.checksum = message.message_header.checksum;
			header.id = cpp_to_rust_bytes(message.message_header.id);
			header.offset = message.message_header.offset;
			header.timestamp = message.message_header.timestamp;
			header.origin_timestamp = message.message_header.origin_timestamp;
			header.payload_length = static_cast<std::uint32_t>(message.payload_bytes.size());

			::iggy::ffi::FfiHeaderMap headers = cpp_to_ffi_header_map(message.headers);
			header.user_headers_length = user_headers_length(message.headers);
			::iggy::ffi::FfiIggyMessage ffi{header, cpp_to_rust_bytes(message.payload_bytes), headers};
			return ffi;
		}

		IggyMessage ffi_message_to_cpp(const ::iggy::ffi::FfiIggyMessage& message) {
			IggyMessage out;
			out.message_header.checksum = message.header.checksum;
			out.message_header.id = rust_bytes_to_cpp(message.header.id);
			out.message_header.offset = message.header.offset;
			out.message_header.timestamp = message.header.timestamp;
			out.message_header.origin_timestamp = message.header.origin_timestamp;
			out.message_header.user_headers_length = message.header.user_headers_length;
			out.message_header.payload_length = message.header.payload_length;
			out.payload_bytes = rust_bytes_to_cpp(message.payload);
			out.payload_text.assign(out.payload_bytes.begin(), out.payload_bytes.end());
			out.headers = ffi_header_map_to_cpp(message.headers);
			return out;
		}

		CompressionAlgorithm ffi_compression_to_cpp(::iggy::ffi::FfiCompressionAlgorithm algorithm) {
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

		IggyByteSize byte_size_from_ffi(const ::iggy::ffi::FfiIggyByteSize& size) {
			return IggyByteSize{size.value};
		}

		MaxTopicSize max_topic_size_from_ffi(const ::iggy::ffi::FfiMaxTopicSize& size) {
			MaxTopicSize out;
			switch (size.kind) {
				case ::iggy::ffi::FfiMaxTopicSizeKind::ServerDefault:
					out.kind = MaxTopicSizeKind::ServerDefault;
					break;
				case ::iggy::ffi::FfiMaxTopicSizeKind::Custom:
					out.kind = MaxTopicSizeKind::Custom;
					break;
				case ::iggy::ffi::FfiMaxTopicSizeKind::Unlimited:
					out.kind = MaxTopicSizeKind::Unlimited;
					break;
			}
			out.value = byte_size_from_ffi(size.value);
			return out;
		}

		std::uint64_t timestamp_to_u64(const ::iggy::ffi::FfiIggyTimestamp& timestamp) {
			return timestamp.value;
		}

		std::uint64_t expiry_to_u64(const ::iggy::ffi::FfiIggyExpiry& expiry) {
			switch (expiry.kind) {
				case ::iggy::ffi::FfiIggyExpiryKind::ServerDefault:
					return 0;
				case ::iggy::ffi::FfiIggyExpiryKind::ExpireDuration:
					return expiry.value;
				case ::iggy::ffi::FfiIggyExpiryKind::NeverExpire:
					return std::numeric_limits<std::uint64_t>::max();
			}
			return expiry.value;
		}

		Partition ffi_partition_to_cpp(const ::iggy::ffi::FfiPartition& partition) {
			Partition out;
			out.id = partition.id;
			out.created_at = timestamp_to_u64(partition.created_at);
			out.segments_count = partition.segments_count;
			out.current_offset = partition.current_offset;
			out.size = byte_size_from_ffi(partition.size);
			out.messages_count = partition.messages_count;
			return out;
		}

		Topic ffi_topic_to_cpp(const ::iggy::ffi::FfiTopic& topic) {
			Topic out;
			out.id = topic.id;
			out.created_at = timestamp_to_u64(topic.created_at);
			out.name = rust_string_to_cpp(topic.name);
			out.size = byte_size_from_ffi(topic.size);
			out.message_expiry = expiry_to_u64(topic.message_expiry);
			out.compression_algorithm = ffi_compression_to_cpp(topic.compression_algorithm);
			out.max_topic_size = max_topic_size_from_ffi(topic.max_topic_size);
			out.replication_factor = topic.replication_factor;
			out.messages_count = topic.messages_count;
			out.partitions_count = topic.partitions_count;
			return out;
		}

		StreamDetails ffi_stream_details_to_cpp(const ::iggy::ffi::FfiStreamDetails& details) {
			StreamDetails out;
			out.id = details.id;
			out.created_at = timestamp_to_u64(details.created_at);
			out.name = rust_string_to_cpp(details.name);
			out.size = byte_size_from_ffi(details.size);
			out.messages_count = details.messages_count;
			out.topics_count = details.topics_count;
			out.topics.reserve(details.topics.size());
			for (const auto& topic : details.topics) {
				out.topics.push_back(ffi_topic_to_cpp(topic));
			}
			return out;
		}

		TopicDetails ffi_topic_details_to_cpp(const ::iggy::ffi::FfiTopicDetails& details) {
			TopicDetails out;
			out.id = details.id;
			out.created_at = timestamp_to_u64(details.created_at);
			out.name = rust_string_to_cpp(details.name);
			out.size = byte_size_from_ffi(details.size);
			out.message_expiry = expiry_to_u64(details.message_expiry);
			out.compression_algorithm = ffi_compression_to_cpp(details.compression_algorithm);
			out.max_topic_size = max_topic_size_from_ffi(details.max_topic_size);
			out.replication_factor = details.replication_factor;
			out.messages_count = details.messages_count;
			out.partitions_count = details.partitions_count;
			out.partitions.reserve(details.partitions.size());
			for (const auto& partition : details.partitions) {
				out.partitions.push_back(ffi_partition_to_cpp(partition));
			}
			return out;
		}

	}  // namespace detail

}  // namespace iggy
