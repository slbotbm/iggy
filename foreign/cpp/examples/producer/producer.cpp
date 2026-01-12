#include <exception>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iggy.hpp"

int main() {
	const std::string address = "127.0.0.1:8090";
	const std::string stream = "example-stream";
	const std::string topic = "example-topic";
	const int count = 5;

	std::unique_ptr<iggy::IggyClient> client;
	try {
		client = iggy::IggyClient::new_client(address);
	} catch (const iggy::IggyException& e) {
		std::cerr << "Failed to create client: [" << e.error().code << "] " << e.what() << "\n";
		return 1;
	} catch (const std::exception& e) {
		std::cerr << "Failed to create client: " << e.what() << "\n";
		return 1;
	}

	try {
		client->connect();
	} catch (const iggy::IggyException& e) {
		std::cerr << "Failed to connect: [" << e.error().code << "] " << e.what() << "\n";
		return 1;
	} catch (const std::exception& e) {
		std::cerr << "Failed to connect: " << e.what() << "\n";
		return 1;
	}

	try {
		client->login_user("iggy", "iggy");
	} catch (const iggy::IggyException& e) {
		std::cerr << "Login failed: [" << e.error().code << "] " << e.what() << "\n";
		return 1;
	} catch (const std::exception& e) {
		std::cerr << "Login failed: " << e.what() << "\n";
		return 1;
	}

	iggy::Identifier stream_id;
	iggy::Identifier topic_id;
	try {
		stream_id = iggy::identifier::from_name(stream);
		topic_id = iggy::identifier::from_name(topic);
	} catch (const std::exception& e) {
		std::cerr << "Invalid stream/topic identifier: " << e.what() << "\n";
		return 1;
	}

	try {
		client->create_stream(stream);
	} catch (const iggy::IggyException& e) {
		std::cerr << "Create stream: [" << e.error().code << "] " << e.what() << "\n";
	} catch (const std::exception& e) {
		std::cerr << "Create stream: " << e.what() << "\n";
	}

	try {
		client->create_topic(stream_id, topic, 1);
	} catch (const iggy::IggyException& e) {
		std::cerr << "Create topic: [" << e.error().code << "] " << e.what() << "\n";
	} catch (const std::exception& e) {
		std::cerr << "Create topic: " << e.what() << "\n";
	}

	auto header_string = [](std::string_view value) {
		iggy::HeaderValue header;
		header.kind = iggy::HeaderKind::String;
		header.value.reserve(value.size());
		for (char c : value) {
			header.value.push_back(static_cast<uint8_t>(c));
		}
		return header;
	};
	auto header_bool = [](bool value) {
		iggy::HeaderValue header;
		header.kind = iggy::HeaderKind::Bool;
		header.value.push_back(static_cast<uint8_t>(value ? 1 : 0));
		return header;
	};
	auto header_uint32 = [](uint32_t value) {
		iggy::HeaderValue header;
		header.kind = iggy::HeaderKind::Uint32;
		header.value.reserve(sizeof(uint32_t));
		for (size_t i = 0; i < sizeof(uint32_t); ++i) {
			header.value.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xFF));
		}
		return header;
	};

	std::unordered_map<std::string, iggy::HeaderValue> headers;
	headers.emplace("trace_id", header_string("req-123"));
	headers.emplace("count", header_uint32(42));
	headers.emplace("is_urgent", header_bool(true));

	iggy::IggyMessageBatchBuilder batch;
	batch.reserve(static_cast<size_t>(count));
	for (int i = 0; i < count; ++i) {
		iggy::IggyMessageBuilder builder;
		std::string payload = "message-" + std::to_string(i);
		batch.add_message(builder.payload(payload).user_headers(headers).build());
	}
	auto messages = batch.build();
	auto message_count = messages.size();

	try {
		client->send_messages(stream_id, topic_id, 0, std::move(messages));
	} catch (const iggy::IggyException& e) {
		std::cerr << "Send messages failed: [" << e.error().code << "] " << e.what() << "\n";
		return 1;
	} catch (const std::exception& e) {
		std::cerr << "Send messages failed: " << e.what() << "\n";
		return 1;
	}

	std::cout << "Sent " << message_count << " messages to " << stream << "/" << topic << ".\n";
	return 0;
}
