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

#include <charconv>
#include <system_error>
#include <utility>

#include "iggy.hpp"

namespace iggy {

	namespace {

		IggyError make_iggy_error(const ::rust::Error& error) {
			std::string message = error.what();
			std::uint32_t code = 0;

			auto delimiter = message.find(':');
			if (delimiter != std::string::npos) {
				auto code_view = std::string_view(message.data(), delimiter);
				std::uint32_t parsed = 0;
				auto result = std::from_chars(code_view.data(), code_view.data() + code_view.size(), parsed);
				if (result.ec == std::errc() && result.ptr == code_view.data() + code_view.size()) {
					code = parsed;
					message.erase(0, delimiter + 1);
					if (!message.empty() && message.front() == ' ') {
						message.erase(0, 1);
					}
				}
			}

			return IggyError(code, std::move(message));
		}

	}  // namespace

	IggyClient IggyClient::ClientBuilder::create_client(const std::string& conn) const {
		return IggyClient::create_client(conn);
	}

	IggyClient IggyClient::create_client(const std::string& conn) {
		try {
			auto client = ::iggy::ffi::create_client(::rust::Str(conn));
			return IggyClient(std::move(client));
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
	}

	IggyClient::IggyClient(::rust::Box<::iggy::ffi::FfiIggyClient> inner) : inner_(std::move(inner)) {}

	IggyClient::IggyClient(IggyClient&&) noexcept = default;

	IggyClient& IggyClient::operator=(IggyClient&&) noexcept = default;

	IggyClient::~IggyClient() = default;

	IggyClient& IggyClient::login_user(const std::string& username, const std::string& password) & {
		try {
			inner_->login_user(::rust::Str(username), ::rust::Str(password));
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
		return *this;
	}

	IggyClient&& IggyClient::login_user(const std::string& username, const std::string& password) && {
		static_cast<IggyClient&>(*this).login_user(username, password);
		return std::move(*this);
	}

	IggyClient& IggyClient::connect() & {
		try {
			inner_->connect();
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
		return *this;
	}

	IggyClient&& IggyClient::connect() && {
		static_cast<IggyClient&>(*this).connect();
		return std::move(*this);
	}

	IggyClient& IggyClient::ping() & {
		try {
			inner_->ping();
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
		return *this;
	}

	IggyClient&& IggyClient::ping() && {
		static_cast<IggyClient&>(*this).ping();
		return std::move(*this);
	}

	IggyClient& IggyClient::create_stream(const std::string& name) & {
		try {
			inner_->create_stream(::rust::Str(name));
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
		return *this;
	}

	IggyClient&& IggyClient::create_stream(const std::string& name) && {
		static_cast<IggyClient&>(*this).create_stream(name);
		return std::move(*this);
	}

	StreamDetails IggyClient::get_stream(const Identifier& stream_id) {
		auto ffi_id = detail::cpp_to_ffi_identifier(stream_id);
		try {
			auto details = inner_->get_stream(ffi_id);
			return detail::ffi_stream_details_to_cpp(*details);
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
	}

	IggyClient& IggyClient::create_topic(const Identifier& stream, const std::string& name,
										 std::uint32_t partitions_count, CompressionAlgorithm compression_algorithm,
										 std::uint8_t replication_factor, std::uint64_t message_expiry,
										 MaxTopicSize max_topic_size) & {
		auto compression = detail::compression_to_string(compression_algorithm);
		auto ffi_stream = detail::cpp_to_ffi_identifier(stream);
		auto ffi_expiry = detail::cpp_to_ffi_expiry(message_expiry);
		auto ffi_max_topic_size = detail::cpp_to_ffi_max_topic_size(max_topic_size);
		try {
			inner_->create_topic(ffi_stream, ::rust::Str(name), partitions_count, ::rust::Str(compression),
								 replication_factor, ffi_expiry, ffi_max_topic_size);
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
		return *this;
	}

	IggyClient&& IggyClient::create_topic(const Identifier& stream, const std::string& name,
										  std::uint32_t partitions_count, CompressionAlgorithm compression_algorithm,
										  std::uint8_t replication_factor, std::uint64_t message_expiry,
										  MaxTopicSize max_topic_size) && {
		static_cast<IggyClient&>(*this).create_topic(stream, name, partitions_count, compression_algorithm,
													 replication_factor, message_expiry, max_topic_size);
		return std::move(*this);
	}

	TopicDetails IggyClient::get_topic(const Identifier& stream_id, const Identifier& topic_id) {
		auto ffi_stream = detail::cpp_to_ffi_identifier(stream_id);
		auto ffi_topic = detail::cpp_to_ffi_identifier(topic_id);
		try {
			auto details = inner_->get_topic(ffi_stream, ffi_topic);
			return detail::ffi_topic_details_to_cpp(*details);
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
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
		try {
			inner_->send_messages(ffi_stream, ffi_topic, partitioning.partition_id, ffi_messages);
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
		return *this;
	}

	IggyClient&& IggyClient::send_messages(const Identifier& stream, const Identifier& topic,
										   const Partitioning& partitioning,
										   const std::vector<IggyMessage>& messages) && {
		static_cast<IggyClient&>(*this).send_messages(stream, topic, partitioning, messages);
		return std::move(*this);
	}
	std::vector<IggyMessage> IggyClient::poll_messages(const Identifier& stream, const Identifier& topic,
													   const Partitioning& partitioning,
													   const PollingStrategy& strategy, std::uint32_t count,
													   bool auto_commit) & {
		auto ffi_stream = detail::cpp_to_ffi_identifier(stream);
		auto ffi_topic = detail::cpp_to_ffi_identifier(topic);
		auto ffi_strategy = detail::cpp_to_ffi_polling_strategy(strategy);
		try {
			auto messages = inner_->poll_messages(ffi_stream, ffi_topic, partitioning.partition_id, ffi_strategy, count,
												  auto_commit);
			std::vector<IggyMessage> out;
			out.reserve(messages.size());
			for (const auto& message : messages) {
				out.push_back(detail::ffi_message_to_cpp(message));
			}
			return out;
		} catch (const ::rust::Error& error) {
			throw make_iggy_error(error);
		}
	}

	std::vector<IggyMessage> IggyClient::poll_messages(const Identifier& stream, const Identifier& topic,
													   const Partitioning& partitioning,
													   const PollingStrategy& strategy, std::uint32_t count,
													   bool auto_commit) && {
		return static_cast<IggyClient&>(*this).poll_messages(stream, topic, partitioning, strategy, count, auto_commit);
	}

}  // namespace iggy
