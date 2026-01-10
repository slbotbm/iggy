#include <cstring>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace {
template <typename T>
std::vector<uint8_t> to_le_bytes(T value) {
	std::vector<uint8_t> bytes(sizeof(T));
	for (size_t i = 0; i < bytes.size(); ++i) {
		bytes[i] = static_cast<uint8_t>((value >> (i * 8)) & 0xFF);
	}
	return bytes;
}
}  // namespace

#include "iggy.hpp"

namespace iggy {
	HeaderValue HeaderValue::Raw(const std::vector<uint8_t>& bytes) {
		return {iggy::ffi::FfiHeaderKind::Raw, bytes};
	}

	HeaderValue HeaderValue::String(std::string_view value) {
		return {iggy::ffi::FfiHeaderKind::String, std::vector<uint8_t>(value.begin(), value.end())};
	}

	HeaderValue HeaderValue::Bool(bool value) {
		return {iggy::ffi::FfiHeaderKind::Bool, std::vector<uint8_t>{static_cast<uint8_t>(value ? 1 : 0)}};
	}

	HeaderValue HeaderValue::Int8(int8_t value) {
		return {iggy::ffi::FfiHeaderKind::Int8, {static_cast<uint8_t>(value)}};
	}

	HeaderValue HeaderValue::Int16(int16_t value) {
		return {iggy::ffi::FfiHeaderKind::Int16, to_le_bytes(static_cast<uint16_t>(value))};
	}

	HeaderValue HeaderValue::Int32(int32_t value) {
		return {iggy::ffi::FfiHeaderKind::Int32, to_le_bytes(static_cast<uint32_t>(value))};
	}

	HeaderValue HeaderValue::Int64(int64_t value) {
		return {iggy::ffi::FfiHeaderKind::Int64, to_le_bytes(static_cast<uint64_t>(value))};
	}

	HeaderValue HeaderValue::Int128(const std::array<uint8_t, 16>& bytes) {
		return {iggy::ffi::FfiHeaderKind::Int128, std::vector<uint8_t>(bytes.begin(), bytes.end())};
	}

	HeaderValue HeaderValue::Uint8(uint8_t value) {
		return {iggy::ffi::FfiHeaderKind::Uint8, {value}};
	}

	HeaderValue HeaderValue::Uint16(uint16_t value) {
		return {iggy::ffi::FfiHeaderKind::Uint16, to_le_bytes(value)};
	}

	HeaderValue HeaderValue::Uint32(uint32_t value) {
		return {iggy::ffi::FfiHeaderKind::Uint32, to_le_bytes(value)};
	}

	HeaderValue HeaderValue::Uint64(uint64_t value) {
		return {iggy::ffi::FfiHeaderKind::Uint64, to_le_bytes(value)};
	}

	HeaderValue HeaderValue::Uint128(const std::array<uint8_t, 16>& bytes) {
		return {iggy::ffi::FfiHeaderKind::Uint128, std::vector<uint8_t>(bytes.begin(), bytes.end())};
	}

	HeaderValue HeaderValue::Float(float value) {
		static_assert(sizeof(float) == 4, "float must be 4 bytes");
		uint32_t bits = 0;
		std::memcpy(&bits, &value, sizeof(bits));
		return {iggy::ffi::FfiHeaderKind::Float32, to_le_bytes(bits)};
	}

	HeaderValue HeaderValue::Double(double value) {
		static_assert(sizeof(double) == 8, "double must be 8 bytes");
		uint64_t bits = 0;
		std::memcpy(&bits, &value, sizeof(bits));
		return {iggy::ffi::FfiHeaderKind::Float64, to_le_bytes(bits)};
	}

	HeaderValue HeaderValue::FromBytes(iggy::ffi::FfiHeaderKind kind, const std::vector<uint8_t>& bytes) {
		return {kind, bytes};
	}

	iggy::ffi::FfiIdentifier Identifier::from_id(uint32_t id) {
		iggy::ffi::FfiIdentifier ident;
		ident.kind = iggy::ffi::FfiIdKind::Numeric;
		ident.length = 4;

		ident.value.reserve(4);
		ident.value.push_back(static_cast<uint8_t>(id & 0xFF));
		ident.value.push_back(static_cast<uint8_t>((id >> 8) & 0xFF));
		ident.value.push_back(static_cast<uint8_t>((id >> 16) & 0xFF));
		ident.value.push_back(static_cast<uint8_t>((id >> 24) & 0xFF));

		return ident;
	}

	iggy::ffi::FfiIdentifier Identifier::from_name(const std::string& name) {
		if (name.empty() || name.size() > std::numeric_limits<uint8_t>::max()) {
			throw std::invalid_argument("Identifier name must be 1..255 bytes.");
		}

		iggy::ffi::FfiIdentifier ident;
		ident.kind = iggy::ffi::FfiIdKind::String;
		ident.length = static_cast<uint8_t>(name.size());

		ident.value.reserve(name.size());
		for (char c : name) {
			ident.value.push_back(static_cast<uint8_t>(c));
		}
		return ident;
	}

	MessageBuilder::MessageBuilder() {
		msg_.header.id.reserve(16);
		for (int i = 0; i < 16; ++i) {
			msg_.header.id.push_back(0);
		}
		msg_.header.payload_length = 0;
	}

	MessageBuilder& MessageBuilder::payload(const std::string& data) {
		std::vector<uint8_t> bytes(data.begin(), data.end());
		return payload(bytes);
	}

	MessageBuilder& MessageBuilder::payload(const std::vector<uint8_t>& data) {
		msg_.payload.clear();
		msg_.payload.reserve(data.size());
		for (auto b : data) msg_.payload.push_back(b);
		msg_.header.payload_length = static_cast<uint32_t>(data.size());
		return *this;
	}

	MessageBuilder& MessageBuilder::user_headers(const std::unordered_map<std::string, HeaderValue>& headers) {
		msg_.headers.entries.clear();
		msg_.headers.entries.reserve(headers.size());

		for (const auto& [key, value] : headers) {
			if (key.empty() || key.size() > std::numeric_limits<uint8_t>::max()) {
				throw std::invalid_argument("Header key must be 1..255 bytes.");
			}
			if (value.bytes.empty() || value.bytes.size() > std::numeric_limits<uint8_t>::max()) {
				throw std::invalid_argument("Header value must be 1..255 bytes.");
			}

			iggy::ffi::FfiHeaderEntry entry;
			entry.key.value = key;
			entry.value.kind = value.kind;
			entry.value.value.reserve(value.bytes.size());
			for (auto byte : value.bytes) {
				entry.value.value.push_back(byte);
			}
			msg_.headers.entries.push_back(entry);
		}

		return *this;
	}

	iggy::ffi::FfiIggyMessage MessageBuilder::build() {
		return msg_;
	}

	MessageBatchBuilder& MessageBatchBuilder::reserve(size_t count) {
		messages_.reserve(count);
		return *this;
	}

	MessageBatchBuilder& MessageBatchBuilder::add(const Message& message) {
		messages_.push_back(message);
		return *this;
	}

	MessageBatchBuilder& MessageBatchBuilder::add(Message&& message) {
		messages_.push_back(std::move(message));
		return *this;
	}

	rust::Vec<Message> MessageBatchBuilder::build() {
		return std::move(messages_);
	}

	Client::Client(rust::Box<iggy::ffi::IggyClient> inner) : inner_(std::move(inner)) {}

	Client::~Client() = default;

	std::pair<std::unique_ptr<Client>, Result> Client::create(const std::string& conn_str) {
		try {
			auto rust_box = iggy::ffi::create_client(conn_str);
			return {std::unique_ptr<Client>(new Client(std::move(rust_box))), Result::Ok()};
		} catch (const rust::Error& e) {
			return {nullptr, Result::Error(e.what())};
		}
	}

	Result Client::connect() {
		try {
			inner_->connect();
			return Result::Ok();
		} catch (const rust::Error& e) {
			return Result::Error(e.what());
		}
	}

	Result Client::login_user(const std::string& username, const std::string& password) {
		try {
			inner_->login_user(username, password);
			return Result::Ok();
		} catch (const rust::Error& e) {
			return Result::Error(e.what());
		}
	}

	Result Client::ping() {
		try {
			inner_->ping();
			return Result::Ok();
		} catch (const rust::Error& e) {
			return Result::Error(e.what());
		}
	}

	Result Client::create_stream(const std::string& name) {
		try {
			inner_->create_stream(name);
			return Result::Ok();
		} catch (const rust::Error& e) {
			return Result::Error(e.what());
		}
	}

	std::pair<std::optional<StreamDetails>, Result> Client::get_stream(const iggy::ffi::FfiIdentifier& stream_id) {
		try {
			auto details = inner_->get_stream(stream_id);
			return {details, Result::Ok()};
		} catch (const rust::Error& e) {
			return {std::nullopt, Result::Error(e.what())};
		}
	}

	Result Client::create_topic(const iggy::ffi::FfiIdentifier& stream_id, const std::string& name,
								uint32_t partitions_count, CompressionAlgorithm compression, uint8_t replication_factor,
								iggy::ffi::FfiIggyExpiry expiry, iggy::ffi::FfiMaxTopicSize max_size) {
		try {
			inner_->create_topic(stream_id, name, partitions_count, compression, replication_factor, expiry, max_size);
			return Result::Ok();
		} catch (const rust::Error& e) {
			return Result::Error(e.what());
		}
	}

	std::pair<std::optional<TopicDetails>, Result> Client::get_topic(const iggy::ffi::FfiIdentifier& stream_id,
																	 const iggy::ffi::FfiIdentifier& topic_id) {
		try {
			auto details = inner_->get_topic(stream_id, topic_id);
			return {details, Result::Ok()};
		} catch (const rust::Error& e) {
			return {std::nullopt, Result::Error(e.what())};
		}
	}

	Result Client::send_messages(const iggy::ffi::FfiIdentifier& stream_id, const iggy::ffi::FfiIdentifier& topic_id,
								 uint32_t partitioning, const std::vector<Message>& messages) {
		try {
			rust::Vec<Message> rust_messages;
			rust_messages.reserve(messages.size());
			for (const auto& message : messages) {
				rust_messages.push_back(message);
			}
			inner_->send_messages(stream_id, topic_id, partitioning, std::move(rust_messages));
			return Result::Ok();
		} catch (const rust::Error& e) {
			return Result::Error(e.what());
		}
	}

	Result Client::send_messages(const iggy::ffi::FfiIdentifier& stream_id, const iggy::ffi::FfiIdentifier& topic_id,
								 uint32_t partitioning, rust::Vec<Message> messages) {
		try {
			inner_->send_messages(stream_id, topic_id, partitioning, std::move(messages));
			return Result::Ok();
		} catch (const rust::Error& e) {
			return Result::Error(e.what());
		}
	}

	std::pair<std::vector<Message>, Result> Client::poll_messages(const iggy::ffi::FfiIdentifier& stream_id,
																  const iggy::ffi::FfiIdentifier& topic_id,
																  uint32_t partition_id,
																  const PollingStrategy& strategy, uint32_t count,
																  bool auto_commit) {
		try {
			auto msgs_rust = inner_->poll_messages(stream_id, topic_id, partition_id, strategy, count, auto_commit);
			std::vector<Message> msgs_cpp;
			msgs_cpp.reserve(msgs_rust.size());
			for (auto& m : msgs_rust) {
				msgs_cpp.push_back(m);
			}

			return {msgs_cpp, Result::Ok()};
		} catch (const rust::Error& e) {
			return {{}, Result::Error(e.what())};
		}
	}

}  // namespace iggy
