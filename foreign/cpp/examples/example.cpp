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

#include "iggy.hpp"
#include <iostream>
#include <vector>

int
main()
{
	try {
		auto client = iggy::IggyClient::new_connection("127.0.0.1:8090");
		client.connect();
		client.login_user("iggy", "iggy");

		std::string_view stream_name = "sample-stream";
		std::string_view topic_name = "sample-topic";
		const auto stream_id = iggy::Identifier::from_string(stream_name);
		const auto topic_id = iggy::Identifier::from_string(topic_name);

		client.create_stream(stream_name);
		client.create_topic(stream_id,
							topic_name,
							1,
							iggy::CompressionAlgorithm::gzip(),
							1,
							iggy::Expiry::never_expire(),
							iggy::MaxTopicSize::server_default());

		std::cout << "stream and topic created" << std::endl;
		std::vector<iggy::SentMessage> messages;
		messages.reserve(1000000);
		for (int i = 0; i < 1000000; ++i) {
			const std::string payload = "message-" + std::to_string(i);
			iggy::SentMessage message;
			message.set_payload(payload);
			messages.push_back(std::move(message));
		}
		client.send_messages(messages, stream_id, topic_id, 0);
		std::cout << "sent messages" << std::endl;
		return 0;
	} catch (const rust::Error& err) {
		std::cerr << "iggy error: " << err.what() << std::endl;
		return 1;
	} catch (const std::exception& err) {
		std::cerr << "error: " << err.what() << std::endl;
		return 1;
	}
}
