#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "rust/cxx.h"
#include "iggy-cpp/src/lib.rs.h"

namespace iggy {
	using CompressionAlgorithm = iggy::ffi::FfiCompressionAlgorithm;
	using StreamDetails = iggy::ffi::FfiStreamDetails;
	using TopicDetails = iggy::ffi::FfiTopicDetails;
	using Message = iggy::ffi::FfiIggyMessage;
	using PollingStrategy = iggy::ffi::FfiPollingStrategy;

	struct HeaderValue {
		iggy::ffi::FfiHeaderKind kind;
		std::vector<uint8_t> bytes;

		static HeaderValue Raw(const std::vector<uint8_t>& bytes);
		static HeaderValue String(std::string_view value);
		static HeaderValue Bool(bool value);
		static HeaderValue Int8(int8_t value);
		static HeaderValue Int16(int16_t value);
		static HeaderValue Int32(int32_t value);
		static HeaderValue Int64(int64_t value);
		static HeaderValue Int128(const std::array<uint8_t, 16>& bytes);
		static HeaderValue Uint8(uint8_t value);
		static HeaderValue Uint16(uint16_t value);
		static HeaderValue Uint32(uint32_t value);
		static HeaderValue Uint64(uint64_t value);
		static HeaderValue Uint128(const std::array<uint8_t, 16>& bytes);
		static HeaderValue Float(float value);
		static HeaderValue Double(double value);
		static HeaderValue FromBytes(iggy::ffi::FfiHeaderKind kind, const std::vector<uint8_t>& bytes);
	};

	struct Result {
		bool success;
		std::string error_message;

		static Result Ok() { return {true, ""}; }
		static Result Error(const std::string& msg) { return {false, msg}; }
		bool ok() const { return success; }
	};

	class Identifier {
	   public:
		static iggy::ffi::FfiIdentifier from_id(uint32_t id);
		static iggy::ffi::FfiIdentifier from_name(const std::string& name);
	};

	class MessageBuilder {
		iggy::ffi::FfiIggyMessage msg_;

	   public:
		MessageBuilder();
		MessageBuilder& payload(const std::string& data);
		MessageBuilder& payload(const std::vector<uint8_t>& data);
		MessageBuilder& user_headers(const std::unordered_map<std::string, HeaderValue>& headers);
		iggy::ffi::FfiIggyMessage build();
	};

	class MessageBatchBuilder {
		rust::Vec<Message> messages_;

	   public:
		MessageBatchBuilder() = default;
		MessageBatchBuilder& reserve(size_t count);
		MessageBatchBuilder& add(const Message& message);
		MessageBatchBuilder& add(Message&& message);
		rust::Vec<Message> build();
	};

	class Client {
	   public:
		Client() = delete;
		~Client();

		static std::pair<std::unique_ptr<Client>, Result> create(const std::string& conn_str);

		Result connect();
		Result ping();
		Result login_user(const std::string& username, const std::string& password);

		Result create_stream(const std::string& name);
		std::pair<std::optional<StreamDetails>, Result> get_stream(const iggy::ffi::FfiIdentifier& stream_id);

		Result create_topic(const iggy::ffi::FfiIdentifier& stream_id, const std::string& name,
							uint32_t partitions_count, CompressionAlgorithm compression = CompressionAlgorithm::None,
							uint8_t replication_factor = 0,
							iggy::ffi::FfiIggyExpiry expiry = {iggy::ffi::FfiIggyExpiryKind::ServerDefault, 0},
							iggy::ffi::FfiMaxTopicSize max_size = {iggy::ffi::FfiMaxTopicSizeKind::ServerDefault, {0}});

		std::pair<std::optional<TopicDetails>, Result> get_topic(const iggy::ffi::FfiIdentifier& stream_id,
																 const iggy::ffi::FfiIdentifier& topic_id);

		Result send_messages(const iggy::ffi::FfiIdentifier& stream_id, const iggy::ffi::FfiIdentifier& topic_id,
							 uint32_t partitioning, const std::vector<Message>& messages);
		Result send_messages(const iggy::ffi::FfiIdentifier& stream_id, const iggy::ffi::FfiIdentifier& topic_id,
							 uint32_t partitioning, rust::Vec<Message> messages);

		std::pair<std::vector<Message>, Result> poll_messages(const iggy::ffi::FfiIdentifier& stream_id,
															  const iggy::ffi::FfiIdentifier& topic_id,
															  uint32_t partition_id, const PollingStrategy& strategy,
															  uint32_t count, bool auto_commit);

	   private:
		rust::Box<iggy::ffi::IggyClient> inner_;
		explicit Client(rust::Box<iggy::ffi::IggyClient> inner);
	};
}  // namespace iggy
