#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>
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

	iggy::PollingStrategy strategy{iggy::PollingKind::Next, 0};
	rust::Vec<iggy::IggyMessage> messages;
	try {
		messages = client->poll_messages(stream_id, topic_id, 0, strategy, static_cast<uint32_t>(count), true);
	} catch (const iggy::IggyException& e) {
		std::cerr << "Poll messages failed: [" << e.error().code << "] " << e.what() << "\n";
		return 1;
	} catch (const std::exception& e) {
		std::cerr << "Poll messages failed: " << e.what() << "\n";
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
				std::cout << "  " << entry.key.value << ": " << iggy::conversion::format_header_value(entry.value)
						  << "\n";
			}
		}
	}

	return 0;
}
