#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iggy-cpp/src/lib.rs.h"
#include "rust/cxx.h"

namespace iggy {
	using HeaderKind = iggy::ffi::HeaderKind;
	using IdKind = iggy::ffi::IdKind;
	using CompressionAlgorithm = iggy::ffi::CompressionAlgorithm;
	using IggyExpiryKind = iggy::ffi::IggyExpiryKind;
	using MaxTopicSizeKind = iggy::ffi::MaxTopicSizeKind;
	using PollingKind = iggy::ffi::PollingKind;

	using HeaderValue = iggy::ffi::HeaderValue;
	using HeaderKey = iggy::ffi::HeaderKey;
	using HeaderEntry = iggy::ffi::HeaderEntry;
	using HeaderMap = iggy::ffi::HeaderMap;
	using IggyMessageHeader = iggy::ffi::IggyMessageHeader;
	using Identifier = iggy::ffi::Identifier;
	using IggyExpiry = iggy::ffi::IggyExpiry;
	using IggyTimestamp = iggy::ffi::IggyTimestamp;
	using IggyByteSize = iggy::ffi::IggyByteSize;
	using MaxTopicSize = iggy::ffi::MaxTopicSize;
	using PollingStrategy = iggy::ffi::PollingStrategy;
	using IggyMessage = iggy::ffi::IggyMessage;
	using Partition = iggy::ffi::Partition;
	using Topic = iggy::ffi::Topic;
	using StreamDetails = iggy::ffi::StreamDetails;
	using TopicDetails = iggy::ffi::TopicDetails;
	using IggyError = iggy::ffi::IggyError;
	using FfiClient = iggy::ffi::IggyClient;

	class IggyException : public std::runtime_error {
	   public:
		explicit IggyException(IggyError error)
			: std::runtime_error(static_cast<std::string>(error.message)), error_(std::move(error)) {}

		const IggyError& error() const noexcept { return error_; }

	   private:
		IggyError error_;
	};

	namespace internal {
		inline std::string bytes_to_hex(const std::vector<uint8_t>& bytes) {
			std::ostringstream out;
			out << "0x";
			out << std::hex << std::setfill('0');
			for (uint8_t byte : bytes) {
				out << std::setw(2) << static_cast<int>(byte);
			}
			return out.str();
		}

		inline std::string bytes_to_hex(const rust::Vec<uint8_t>& bytes) {
			std::ostringstream out;
			out << "0x";
			out << std::hex << std::setfill('0');
			for (uint8_t byte : bytes) {
				out << std::setw(2) << static_cast<int>(byte);
			}
			return out.str();
		}

		template <typename T>
		inline bool read_le(const std::vector<uint8_t>& bytes, T& out_value) {
			if (bytes.size() != sizeof(T)) {
				return false;
			}
			std::make_unsigned_t<T> value = 0;
			for (size_t i = 0; i < sizeof(T); ++i) {
				std::make_unsigned_t<T> byte = static_cast<std::make_unsigned_t<T>>(bytes[i]);
				value |= (byte << (i * 8));
			}
			std::memcpy(&out_value, &value, sizeof(out_value));
			return true;
		}

		template <typename T>
		inline bool read_le(const rust::Vec<uint8_t>& bytes, T& out_value) {
			if (bytes.size() != sizeof(T)) {
				return false;
			}
			std::make_unsigned_t<T> value = 0;
			for (size_t i = 0; i < sizeof(T); ++i) {
				std::make_unsigned_t<T> byte = static_cast<std::make_unsigned_t<T>>(bytes[i]);
				value = value | (byte << (i * 8));
			}
			std::memcpy(&out_value, &value, sizeof(out_value));
			return true;
		}

		inline std::string format_header_value(const HeaderValue& value) {
			switch (value.kind) {
				case HeaderKind::Raw:
					return bytes_to_hex(value.value);
				case HeaderKind::String:
					return std::string(value.value.begin(), value.value.end());
				case HeaderKind::Bool:
					return (!value.value.empty() && value.value[0] != 0) ? "true" : "false";
				case HeaderKind::Int8:
					return value.value.empty() ? "" : std::to_string(static_cast<int8_t>(value.value[0]));
				case HeaderKind::Int16: {
					int16_t v = 0;
					return read_le<int16_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
				}
				case HeaderKind::Int32: {
					int32_t v = 0;
					return read_le<int32_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
				}
				case HeaderKind::Int64: {
					int64_t v = 0;
					return read_le<int64_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
				}
				case HeaderKind::Int128:
					return bytes_to_hex(value.value);
				case HeaderKind::Uint8:
					return value.value.empty() ? "" : std::to_string(value.value[0]);
				case HeaderKind::Uint16: {
					uint16_t v = 0;
					return read_le<uint16_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
				}
				case HeaderKind::Uint32: {
					uint32_t v = 0;
					return read_le<uint32_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
				}
				case HeaderKind::Uint64: {
					uint64_t v = 0;
					return read_le<uint64_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
				}
				case HeaderKind::Uint128:
					return bytes_to_hex(value.value);
				case HeaderKind::Float32: {
					uint32_t bits = 0;
					if (!read_le<uint32_t>(value.value, bits)) {
						return bytes_to_hex(value.value);
					}
					float f = 0.0f;
					std::memcpy(&f, &bits, sizeof(f));
					return std::to_string(f);
				}
				case HeaderKind::Float64: {
					uint64_t bits = 0;
					if (!read_le<uint64_t>(value.value, bits)) {
						return bytes_to_hex(value.value);
					}
					double d = 0.0;
					std::memcpy(&d, &bits, sizeof(d));
					return std::to_string(d);
				}
			}

			return bytes_to_hex(value.value);
		}

		inline bool parse_u32(std::string_view text, uint32_t& out_value) {
			if (text.empty()) {
				return false;
			}
			uint64_t value = 0;
			for (char c : text) {
				if (c < '0' || c > '9') {
					return false;
				}
				value = value * 10 + static_cast<uint64_t>(c - '0');
				if (value > std::numeric_limits<uint32_t>::max()) {
					return false;
				}
			}
			out_value = static_cast<uint32_t>(value);
			return true;
		}

		inline IggyError parse_iggy_error_message(std::string_view text) {
			IggyError error;
			const size_t separator = text.find(':');
			uint32_t code = 0;
			if (separator != std::string_view::npos) {
				std::string_view code_part = text.substr(0, separator);
				std::string_view message_part = text.substr(separator + 1);
				if (!message_part.empty() && message_part.front() == ' ') {
					message_part.remove_prefix(1);
				}
				if (parse_u32(code_part, code)) {
					error.code = code;
					error.message = rust::String(std::string(message_part));
					return error;
				}
			}

			error.code = std::numeric_limits<uint32_t>::max();
			error.message = rust::String(std::string(text));
			return error;
		}

		template <typename Func>
		inline auto call_or_throw(Func&& func) -> decltype(func()) {
			try {
				if constexpr (std::is_void_v<decltype(func())>) {
					std::forward<Func>(func)();
					return;
				} else {
					return std::forward<Func>(func)();
				}
			} catch (const rust::Error& error) {
				throw IggyException(parse_iggy_error_message(error.what()));
			}
		}
	}  // namespace internal

	namespace conversion {
		inline std::string format_header_value(const HeaderValue& value) {
			return internal::format_header_value(value);
		}
	}  // namespace conversion

	namespace identifier {
		inline Identifier from_name(std::string_view name) {
			if (name.empty() || name.size() > std::numeric_limits<uint8_t>::max()) {
				throw std::invalid_argument("Identifier name must be 1..255 bytes.");
			}
			Identifier identifier;
			identifier.kind = IdKind::String;
			identifier.length = static_cast<uint8_t>(name.size());
			identifier.value.reserve(name.size());
			for (char c : name) {
				identifier.value.push_back(static_cast<uint8_t>(c));
			}
			return identifier;
		}

		inline Identifier from_id(uint32_t value) {
			Identifier identifier;
			identifier.kind = IdKind::Numeric;
			identifier.length = 4;
			identifier.value.reserve(4);
			identifier.value.push_back(static_cast<uint8_t>(value & 0xFF));
			identifier.value.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
			identifier.value.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
			identifier.value.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
			return identifier;
		}
	}  // namespace identifier

	class IggyMessageBuilder {
		IggyMessage message_;

	   public:
		IggyMessageBuilder() {
			message_.header.id.reserve(16);
			for (int i = 0; i < 16; ++i) {
				message_.header.id.push_back(0);
			}
			message_.header.payload_length = 0;
		}

		IggyMessageBuilder& payload(std::string_view data) {
			std::vector<uint8_t> bytes(data.begin(), data.end());
			return payload(bytes);
		}

		IggyMessageBuilder& user_headers(const std::unordered_map<std::string, HeaderValue>& headers) {
			message_.headers.entries.clear();
			message_.headers.entries.reserve(headers.size());

			for (const auto& [key, value] : headers) {
				if (key.empty() || key.size() > std::numeric_limits<uint8_t>::max()) {
					throw std::invalid_argument("Header key must be 1..255 bytes.");
				}
				if (value.value.empty() || value.value.size() > std::numeric_limits<uint8_t>::max()) {
					throw std::invalid_argument("Header value must be 1..255 bytes.");
				}

				HeaderEntry entry;
				entry.key.value = key;
				entry.value.kind = value.kind;
				entry.value.value.reserve(value.value.size());
				for (auto byte : value.value) {
					entry.value.value.push_back(byte);
				}
				message_.headers.entries.push_back(entry);
			}

			return *this;
		}

		IggyMessage build() { return message_; }

	   private:
		IggyMessageBuilder& payload(const std::vector<uint8_t>& data) {
			message_.payload.clear();
			message_.payload.reserve(data.size());
			for (auto b : data) {
				message_.payload.push_back(b);
			}
			message_.header.payload_length = static_cast<uint32_t>(data.size());
			return *this;
		}
	};

	class IggyMessageBatchBuilder {
		rust::Vec<IggyMessage> messages_;

	   public:
		IggyMessageBatchBuilder() = default;
		IggyMessageBatchBuilder& reserve(size_t count) {
			messages_.reserve(count);
			return *this;
		}

		IggyMessageBatchBuilder& add_message(const IggyMessage& message) {
			messages_.push_back(message);
			return *this;
		}

		IggyMessageBatchBuilder& add_message(IggyMessage&& message) {
			messages_.push_back(std::move(message));
			return *this;
		}

		IggyMessageBatchBuilder& add_messages(rust::Vec<IggyMessage> messages) {
			for (auto& message : messages) {
				messages_.push_back(std::move(message));
			}
			return *this;
		}

		rust::Vec<IggyMessage> build() { return std::move(messages_); }
	};

	class IggyClient {
	   public:
		IggyClient() = delete;
		~IggyClient() = default;

		static std::unique_ptr<IggyClient> new_client(const std::string& conn_str) {
			return internal::call_or_throw([&]() {
				rust::Box<FfiClient> rust_box = iggy::ffi::create_client(conn_str);
				return std::unique_ptr<IggyClient>(new IggyClient(std::move(rust_box)));
			});
		}

		void connect() { internal::call_or_throw([this]() { inner_->connect(); }); }

		void ping() { internal::call_or_throw([this]() { inner_->ping(); }); }

		void login_user(const std::string& username, const std::string& password) {
			internal::call_or_throw([this, &username, &password]() { inner_->login_user(username, password); });
		}

		void create_stream(const std::string& name) {
			internal::call_or_throw([this, &name]() { inner_->create_stream(name); });
		}

		StreamDetails get_stream(const Identifier& stream_id) {
			return internal::call_or_throw([this, &stream_id]() { return inner_->get_stream(stream_id); });
		}

		void create_topic(const Identifier& stream_id, const std::string& name, uint32_t partitions_count,
						  CompressionAlgorithm compression = CompressionAlgorithm::None, uint8_t replication_factor = 0,
						  IggyExpiry expiry = {IggyExpiryKind::ServerDefault, 0},
						  MaxTopicSize max_size = {MaxTopicSizeKind::ServerDefault, {0}}) {
			internal::call_or_throw([this, &stream_id, &name, partitions_count, compression, replication_factor, expiry,
									 max_size]() {
				inner_->create_topic(stream_id, name, partitions_count, compression, replication_factor, expiry, max_size);
			});
		}

		TopicDetails get_topic(const Identifier& stream_id, const Identifier& topic_id) {
			return internal::call_or_throw([this, &stream_id, &topic_id]() { return inner_->get_topic(stream_id, topic_id); });
		}

		void send_messages(const Identifier& stream_id, const Identifier& topic_id, uint32_t partitioning,
						   const std::vector<IggyMessage>& messages) {
			internal::call_or_throw([this, &stream_id, &topic_id, partitioning, &messages]() {
				rust::Vec<IggyMessage> rust_messages;
				rust_messages.reserve(messages.size());
				for (const auto& message : messages) {
					rust_messages.push_back(message);
				}
				inner_->send_messages(stream_id, topic_id, partitioning, std::move(rust_messages));
			});
		}

		void send_messages(const Identifier& stream_id, const Identifier& topic_id, uint32_t partitioning,
						   rust::Vec<IggyMessage> messages) {
			internal::call_or_throw(
				[this, &stream_id, &topic_id, partitioning, messages = std::move(messages)]() mutable {
					inner_->send_messages(stream_id, topic_id, partitioning, std::move(messages));
				});
		}

		rust::Vec<IggyMessage> poll_messages(const Identifier& stream_id, const Identifier& topic_id,
											 uint32_t partition_id, const PollingStrategy& polling_strategy,
											 uint32_t count, bool auto_commit) {
			return internal::call_or_throw([this, &stream_id, &topic_id, partition_id, &polling_strategy, count,
											auto_commit]() {
				return inner_->poll_messages(stream_id, topic_id, partition_id, polling_strategy, count, auto_commit);
			});
		}

	   private:
		rust::Box<FfiClient> inner_;
		explicit IggyClient(rust::Box<FfiClient> inner) : inner_(std::move(inner)) {}
	};

}  // namespace iggy
