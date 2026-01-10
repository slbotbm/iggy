#include <cstdint>
#include <cstring>
#include <exception>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "iggy.hpp"

namespace {
std::string bytes_to_hex(const rust::Vec<uint8_t>& bytes) {
	std::ostringstream out;
	out << "0x";
	out << std::hex << std::setfill('0');
	for (uint8_t byte : bytes) {
		out << std::setw(2) << static_cast<int>(byte);
	}
	return out.str();
}

template <typename T>
bool read_le(const rust::Vec<uint8_t>& bytes, T& out_value) {
	if (bytes.size() != sizeof(T)) {
		return false;
	}
	using Unsigned = typename std::make_unsigned<T>::type;
	Unsigned value = 0;
	for (size_t i = 0; i < sizeof(T); ++i) {
		value |= static_cast<Unsigned>(static_cast<Unsigned>(bytes[i]) << (i * 8));
	}
	std::memcpy(&out_value, &value, sizeof(out_value));
	return true;
}

std::string format_header_value(const iggy::ffi::FfiHeaderValue& value) {
	switch (value.kind) {
		case iggy::ffi::FfiHeaderKind::Raw:
			return bytes_to_hex(value.value);
		case iggy::ffi::FfiHeaderKind::String:
			return std::string(value.value.begin(), value.value.end());
		case iggy::ffi::FfiHeaderKind::Bool:
			return (!value.value.empty() && value.value[0] != 0) ? "true" : "false";
		case iggy::ffi::FfiHeaderKind::Int8:
			return value.value.empty() ? "" : std::to_string(static_cast<int8_t>(value.value[0]));
		case iggy::ffi::FfiHeaderKind::Int16: {
			int16_t v = 0;
			return read_le<int16_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
		}
		case iggy::ffi::FfiHeaderKind::Int32: {
			int32_t v = 0;
			return read_le<int32_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
		}
		case iggy::ffi::FfiHeaderKind::Int64: {
			int64_t v = 0;
			return read_le<int64_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
		}
		case iggy::ffi::FfiHeaderKind::Int128:
			return bytes_to_hex(value.value);
		case iggy::ffi::FfiHeaderKind::Uint8:
			return value.value.empty() ? "" : std::to_string(value.value[0]);
		case iggy::ffi::FfiHeaderKind::Uint16: {
			uint16_t v = 0;
			return read_le<uint16_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
		}
		case iggy::ffi::FfiHeaderKind::Uint32: {
			uint32_t v = 0;
			return read_le<uint32_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
		}
		case iggy::ffi::FfiHeaderKind::Uint64: {
			uint64_t v = 0;
			return read_le<uint64_t>(value.value, v) ? std::to_string(v) : bytes_to_hex(value.value);
		}
		case iggy::ffi::FfiHeaderKind::Uint128:
			return bytes_to_hex(value.value);
		case iggy::ffi::FfiHeaderKind::Float32: {
			uint32_t bits = 0;
			if (!read_le<uint32_t>(value.value, bits)) {
				return bytes_to_hex(value.value);
			}
			float f = 0.0f;
			std::memcpy(&f, &bits, sizeof(f));
			return std::to_string(f);
		}
		case iggy::ffi::FfiHeaderKind::Float64: {
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
}  // namespace

int main() {
	const std::string address = "127.0.0.1:8090";
	const std::string stream = "example-stream";
	const std::string topic = "example-topic";
	const int count = 5;

	auto [client, result] = iggy::Client::create(address);
	if (!result.ok() || !client) {
		std::cerr << "Failed to create client: " << result.error_message << "\n";
		return 1;
	}

	result = client->connect();
	if (!result.ok()) {
		std::cerr << "Failed to connect: " << result.error_message << "\n";
		return 1;
	}

	result = client->login_user("iggy", "iggy");
	if (!result.ok()) {
		std::cerr << "Login failed: " << result.error_message << "\n";
		return 1;
	}

	iggy::ffi::FfiIdentifier stream_id;
	iggy::ffi::FfiIdentifier topic_id;
	try {
		stream_id = iggy::Identifier::from_name(stream);
		topic_id = iggy::Identifier::from_name(topic);
	} catch (const std::exception& e) {
		std::cerr << "Invalid stream/topic identifier: " << e.what() << "\n";
		return 1;
	}

	iggy::PollingStrategy strategy{iggy::ffi::FfiPollingKind::Next, 0};
	auto [messages, poll_result] =
		client->poll_messages(stream_id, topic_id, 0, strategy, static_cast<uint32_t>(count), true);
	if (!poll_result.ok()) {
		std::cerr << "Poll messages failed: " << poll_result.error_message << "\n";
		return 1;
	}

	if (messages.empty()) {
		std::cout << "No messages available.\n";
		return 0;
	}

	for (const auto& message : messages) {
		std::string payload(message.payload.begin(), message.payload.end());
		std::cout << payload << "\n";
		if (!message.headers.entries.empty()) {
			std::cout << "headers:\n";
			for (const auto& entry : message.headers.entries) {
				std::cout << "  " << entry.key.value << ": " << format_header_value(entry.value) << "\n";
			}
		}
	}

	return 0;
}
