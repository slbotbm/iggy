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

#include <iostream>
#include <memory>

#include "lib.rs.h"
#include "rust/cxx.h"

struct IggyClientDeleter {
	void operator()(iggy::IggyClient* client) const noexcept { iggy::delete_connection(client); }
};

int main() {
	try {
		std::unique_ptr<iggy::IggyClient, IggyClientDeleter> client(iggy::new_connection("127.0.0.1:8090"));

		client->connect();
		client->login_user("iggy", "iggy");
		client->create_stream("sample-stream");
		const iggy::Expiry expiry{iggy::ExpiryKind::NeverExpire, 0};
		const iggy::MaxTopicSize max_topic_size{iggy::MaxTopicSizeKind::ServerDefault, 0};
		client->create_topic("sample-stream", "sample-topic", 1, iggy::CompressionAlgorithm::Gzip, 1, expiry,
							 max_topic_size);

		std::cout << "stream and topic created" << std::endl;
		return 0;
	} catch (const rust::Error& err) {
		std::cerr << "iggy error: " << err.what() << std::endl;
		return 1;
	} catch (const std::exception& err) {
		std::cerr << "error: " << err.what() << std::endl;
		return 1;
	}
}
