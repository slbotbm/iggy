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

	result = client->create_stream(stream);
	if (!result.ok()) {
		std::cerr << "Create stream: " << result.error_message << "\n";
	}

	result = client->create_topic(stream_id, topic, 1);
	if (!result.ok()) {
		std::cerr << "Create topic: " << result.error_message << "\n";
	}

	std::unordered_map<std::string, iggy::HeaderValue> headers;
	headers.emplace("trace_id", iggy::HeaderValue::String("req-123"));
	headers.emplace("count", iggy::HeaderValue::Uint32(42));
	headers.emplace("is_urgent", iggy::HeaderValue::Bool(true));

	iggy::MessageBatchBuilder batch;
	batch.reserve(static_cast<size_t>(count));
	for (int i = 0; i < count; ++i) {
		iggy::MessageBuilder builder;
		std::string payload = "message-" + std::to_string(i);
		batch.add(builder.payload(payload).user_headers(headers).build());
	}
	auto messages = batch.build();
	auto message_count = messages.size();

	result = client->send_messages(stream_id, topic_id, 0, std::move(messages));
	if (!result.ok()) {
		std::cerr << "Send messages failed: " << result.error_message << "\n";
		return 1;
	}

	std::cout << "Sent " << message_count << " messages to " << stream << "/" << topic << ".\n";
	return 0;
}
