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
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lib.rs.h"
#include "rust/cxx.h"

namespace iggy {
	class IggyClient;
	class SentMessage;
	struct PollingStrategy;
	using ExpiryKind = ffi::ExpiryKind;
	using MaxTopicSizeKind = ffi::MaxTopicSizeKind;
	struct CompressionAlgorithm final
	{
			ffi::CompressionAlgorithm algorithm = ffi::CompressionAlgorithm::None;

			static CompressionAlgorithm none() { return CompressionAlgorithm{ ffi::CompressionAlgorithm::None }; }
			static CompressionAlgorithm gzip() { return CompressionAlgorithm{ ffi::CompressionAlgorithm::Gzip }; }

		private:
			friend class iggy::IggyClient;
			ffi::CompressionAlgorithm ffi() const { return algorithm; }
	};
	struct MaxTopicSize final
	{
			MaxTopicSizeKind kind = MaxTopicSizeKind::ServerDefault;
			std::uint64_t value = 0;

			static MaxTopicSize server_default() { return MaxTopicSize{ MaxTopicSizeKind::ServerDefault, 0 }; }
			static MaxTopicSize custom(std::uint64_t bytes) { return MaxTopicSize{ MaxTopicSizeKind::Custom, bytes }; }
			static MaxTopicSize unlimited() { return MaxTopicSize{ MaxTopicSizeKind::Unlimited, 0 }; }

		private:
			friend class iggy::IggyClient;
			ffi::MaxTopicSize ffi() const { return ffi::MaxTopicSize{ kind, value }; }
	};

	struct Expiry final
	{
			ExpiryKind kind = ExpiryKind::ServerDefault;
			std::uint64_t value = 0;

			static Expiry server_default() { return Expiry{ ExpiryKind::ServerDefault, 0 }; }
			static Expiry expire_duration(std::uint64_t milliseconds)
			{
				return Expiry{ ExpiryKind::ExpireDuration, milliseconds };
			}
			static Expiry never_expire() { return Expiry{ ExpiryKind::NeverExpire, 0 }; }

		private:
			friend class iggy::IggyClient;
			ffi::Expiry ffi() const { return ffi::Expiry{ kind, value }; }
	};

	struct PollingStrategyKind final
	{
			ffi::PollingStrategyKind value = ffi::PollingStrategyKind::Offset;

			static PollingStrategyKind offset() { return PollingStrategyKind{ ffi::PollingStrategyKind::Offset }; }
			static PollingStrategyKind timestamp() { return PollingStrategyKind{ ffi::PollingStrategyKind::Timestamp }; }
			static PollingStrategyKind first() { return PollingStrategyKind{ ffi::PollingStrategyKind::First }; }
			static PollingStrategyKind last() { return PollingStrategyKind{ ffi::PollingStrategyKind::Last }; }
			static PollingStrategyKind next() { return PollingStrategyKind{ ffi::PollingStrategyKind::Next }; }

		private:
			friend struct iggy::PollingStrategy;
			friend class iggy::IggyClient;
			ffi::PollingStrategyKind ffi() const { return value; }
	};

	struct PollingStrategy final
	{
			PollingStrategyKind kind = PollingStrategyKind::offset();
			std::uint64_t value = 0;

			static PollingStrategy offset(std::uint64_t offset_value)
			{
				return PollingStrategy{ PollingStrategyKind::offset(), offset_value };
			}
			static PollingStrategy timestamp(std::uint64_t timestamp_value)
			{
				return PollingStrategy{ PollingStrategyKind::timestamp(), timestamp_value };
			}
			static PollingStrategy first() { return PollingStrategy{ PollingStrategyKind::first(), 0 }; }
			static PollingStrategy last() { return PollingStrategy{ PollingStrategyKind::last(), 0 }; }
			static PollingStrategy next() { return PollingStrategy{ PollingStrategyKind::next(), 0 }; }

		private:
			friend class iggy::IggyClient;
			ffi::PollingStrategy ffi() const { return ffi::PollingStrategy{ kind.ffi(), value }; }
	};

	class Identifier final
	{
		public:
			// empty initialization not possible
			// something like `Identifier id;` not possible
			Identifier() = delete;
			// destructor
			~Identifier() noexcept { destroy(); }
			// copy constructor not possible
			// Identifier id1 = Identifier::Named("a");
			// Identifier id2 = id1; -> not possible
			Identifier(const Identifier&) = delete;
			// copy assignment constructor not possible
			// Identifier id1 = Identifier::Named("b");
			// Identifier id2 = Identifier::Named("c");
			// id2 = id1; -> not possible.
			Identifier& operator=(const Identifier&) = delete;

			// Exposes Identifier::Named("...")
			static Identifier from_string(std::string_view name)
			{
				rust::Str s{ name.data(), name.size() };
				iggy::ffi::Identifier* p = iggy::ffi::identifier_from_named(s);
				if (!p)
					throw std::runtime_error("iggy::ffi::identifier_from_named returned null");
				return Identifier(p);
			}

			// Exposes Identifier::Numeric("...")
			static Identifier from_uint32(std::uint32_t id)
			{
				iggy::ffi::Identifier* p = iggy::ffi::identifier_from_numeric(id);
				if (!p)
					throw std::runtime_error("iggy::ffi::identifier_from_numeric returned null");
				return Identifier(p);
			}

			// Move constructor: Allows initialization for Identifier id(Identifier::Named("..."));
			Identifier(Identifier&& other) noexcept
			  : ptr_(std::exchange(other.ptr_, nullptr))
			{
			}

			// Move Assignment: Used when replacing the value of an existing Identifier
			Identifier& operator=(Identifier&& other) noexcept
			{
				if (this != &other) {
					destroy();
					ptr_ = std::exchange(other.ptr_, nullptr);
				}
				return *this;
			}

			explicit operator bool() const noexcept { return ptr_ != nullptr; }

			std::string get_value() const { return std::string(ffi().get_value()); }

		private:
			friend class iggy::SentMessage;
			friend class iggy::IggyClient;

			explicit Identifier(iggy::ffi::Identifier* p)
			  : ptr_(p)
			{
			}
			iggy::ffi::Identifier* ptr_ = nullptr;

			void destroy() noexcept
			{
				if (!ptr_)
					return;
				iggy::ffi::delete_identifier(ptr_);
				ptr_ = nullptr;
			}

			// Allows read and write
			iggy::ffi::Identifier& ffi()
			{
				if (!ptr_)
					throw std::logic_error("Identifier is null (moved-from or default-constructed).");
				return *ptr_;
			}

			// Allows only reading
			const iggy::ffi::Identifier& ffi() const
			{
				if (!ptr_)
					throw std::logic_error("Identifier is null (moved-from or default-constructed).");
				return *ptr_;
			}
	};

	class SentMessage final
	{
		public:
			SentMessage() = default;

			void set_payload(std::string_view data) { payload_ = std::string(data); }

		private:
			friend class IggyClient;

			static rust::Vec<std::uint8_t> to_rust_vec(std::string_view data)
			{
				rust::Vec<std::uint8_t> out;
				out.reserve(data.size());
				for (unsigned char c : data)
					out.push_back(static_cast<std::uint8_t>(c));
				return out;
			}

			static rust::Vec<std::uint8_t> to_rust_vec(const std::vector<std::uint8_t>& data)
			{
				rust::Vec<std::uint8_t> out;
				out.reserve(data.size());
				for (auto b : data)
					out.push_back(b);
				return out;
			}

			ffi::SentMessage to_ffi_message() const
			{
				ffi::SentMessage message{};
				message.payload = to_rust_vec(payload_);
				return message;
			}

			std::string payload_{};
	};

	class IggyClient final
	{
		public:
			IggyClient() = delete;
			~IggyClient() noexcept { destroy(); }
			IggyClient(const IggyClient&) = delete;
			IggyClient& operator=(const IggyClient&) = delete;

			static IggyClient new_connection(std::string_view connection_string)
			{
				rust::Str s{ connection_string.data(), connection_string.size() };
				ffi::Client* p = iggy::ffi::new_connection(s);
				if (!p)
					throw std::runtime_error("iggy::new_connection returned null");
				return IggyClient(p);
			}

			IggyClient(IggyClient&& other) noexcept
			  : ptr_(std::exchange(other.ptr_, nullptr))
			{
			}
			IggyClient& operator=(IggyClient&& other) noexcept
			{
				if (this != &other) {
					destroy();
					ptr_ = std::exchange(other.ptr_, nullptr);
				}
				return *this;
			}

			explicit operator bool() const noexcept { return ptr_ != nullptr; }

			void connect() const { ffi().connect(); }

			void login_user(std::string_view username, std::string_view password) const
			{
				rust::Str u{ username.data(), username.size() };
				rust::Str p{ password.data(), password.size() };
				ffi().login_user(u, p);
			}

			void create_stream(std::string_view stream_name) const
			{
				rust::Str s{ stream_name.data(), stream_name.size() };
				ffi().create_stream(s);
			}

			void create_topic(const iggy::Identifier& stream_id,
							  std::string_view topic_name,
							  std::uint32_t partitions_count,
							  iggy::CompressionAlgorithm compression_algorithm,
							  std::uint8_t replication_factor,
							  iggy::Expiry expiry,
							  iggy::MaxTopicSize max_topic_size) const
			{
				rust::Str name{ topic_name.data(), topic_name.size() };
				ffi().create_topic(stream_id.ffi(),
								   name,
								   partitions_count,
								   compression_algorithm.ffi(),
								   replication_factor,
								   expiry.ffi(),
								   max_topic_size.ffi());
			}

			void send_messages(const std::vector<SentMessage>& messages,
							   const iggy::Identifier& stream_id,
							   const iggy::Identifier& topic,
							   std::uint32_t partitioning) const
			{
				rust::Vec<ffi::SentMessage> ffi_messages;
				ffi_messages.reserve(messages.size());
				for (const auto& message : messages)
					ffi_messages.push_back(message.to_ffi_message());
				ffi().send_messages(std::move(ffi_messages), stream_id.ffi(), topic.ffi(), partitioning);
			}

		private:
			explicit IggyClient(ffi::Client* p)
			  : ptr_(p)
			{
			}

			ffi::Client* get() const noexcept { return ptr_; }

			ffi::Client& ffi()
			{
				if (!ptr_)
					throw std::logic_error("IggyClient is null (moved-from or default-constructed).");
				return *ptr_;
			}
			const ffi::Client& ffi() const
			{
				if (!ptr_)
					throw std::logic_error("IggyClient is null (moved-from or default-constructed).");
				return *ptr_;
			}

			void destroy() noexcept
			{
				if (!ptr_)
					return;
				iggy::ffi::delete_connection(ptr_);
				ptr_ = nullptr;
			}

			ffi::Client* ptr_ = nullptr;
	};

} // namespace iggy
