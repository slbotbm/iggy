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

mod type_conversions;

use iggy::prelude::{
    Consumer as RustConsumer, IggyClient as RustIggyClient,
    IggyClientBuilder as RustIggyClientBuilder, IggyMessage as RustIggyMessage, *,
};
use std::result::Result;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime.")
});

#[cxx::bridge(namespace = "iggy::ffi")]
mod ffi {

    enum FfiHeaderKind {
        Raw = 1,
        String = 2,
        Bool = 3,
        Int8 = 4,
        Int16 = 5,
        Int32 = 6,
        Int64 = 7,
        Int128 = 8,
        Uint8 = 9,
        Uint16 = 10,
        Uint32 = 11,
        Uint64 = 12,
        Uint128 = 13,
        Float32 = 14,
        Float64 = 15,
    }

    struct FfiHeaderValue {
        kind: FfiHeaderKind,
        value: Vec<u8>,
    }

    struct FfiHeaderEntry {
        key: String,
        value: FfiHeaderValue,
    }

    struct FfiHeaderMap {
        entries: Vec<FfiHeaderEntry>,
    }

    struct FfiIggyMessageHeader {
        checksum: u64,
        id: Vec<u8>,
        offset: u64,
        timestamp: u64,
        origin_timestamp: u64,
        user_headers_length: u32,
        payload_length: u32,
    }

    enum FfiIdentifierKind {
        Numeric = 1,
        String = 2,
    }

    struct FfiIdentifier {
        kind: FfiIdentifierKind,
        length: u8,
        value: Vec<u8>,
    }

    enum FfiCompressionAlgorithm {
        None = 1,
        Gzip = 2,
    }

    enum FfiPollingKind {
        Offset = 1,
        Timestamp = 2,
        First = 3,
        Last = 4,
        Next = 5,
    }

    struct FfiPollingStrategy {
        kind: FfiPollingKind,
        value: u64,
    }

    struct FfiIggyMessage {
        header: FfiIggyMessageHeader,
        payload: Vec<u8>,
        headers: FfiHeaderMap,
    }

    struct FfiPartition {
        id: u32,
        created_at: u64,
        segments_count: u32,
        current_offset: u64,
        size_bytes: u64,
        messages_count: u64,
    }

    struct FfiTopic {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        message_expiry: u64,
        compression_algorithm: FfiCompressionAlgorithm,
        max_topic_size: u64,
        replication_factor: u8,
        messages_count: u64,
        partitions_count: u32,
    }

    struct FfiStreamDetails {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        messages_count: u64,
        topics_count: u32,
        topics: Vec<FfiTopic>,
    }

    struct FfiTopicDetails {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        message_expiry: u64,
        compression_algorithm: FfiCompressionAlgorithm,
        max_topic_size: u64,
        replication_factor: u8,
        messages_count: u64,
        partitions_count: u32,
        partitions: Vec<FfiPartition>,
    }

    extern "Rust" {
        type IggyClient;

        fn create_client(conn: &str) -> Result<Box<IggyClient>>;

        fn login_user(self: &IggyClient, username: &str, password: &str) -> Result<()>;
        fn connect(self: &IggyClient) -> Result<()>;
        fn ping(self: &IggyClient) -> Result<()>;

        fn create_stream(self: &IggyClient, name: &str) -> Result<()>;
        fn get_stream(
            self: &IggyClient,
            stream_id: &FfiIdentifier,
        ) -> Result<Box<FfiStreamDetails>>;
        fn create_topic(
            self: &IggyClient,
            stream: &FfiIdentifier,
            name: &str,
            partitions_count: u32,
            compression_algorithm: &str,
            replication_factor: u8,
        ) -> Result<()>;
        fn get_topic(
            self: &IggyClient,
            stream_id: &FfiIdentifier,
            topic_id: &FfiIdentifier,
        ) -> Result<Box<FfiTopicDetails>>;
        fn send_messages(
            self: &IggyClient,
            stream: &FfiIdentifier,
            topic: &FfiIdentifier,
            partitioning: u32,
            messages: &Vec<FfiIggyMessage>,
        ) -> Result<()>;
        fn poll_messages(
            self: &IggyClient,
            stream: &FfiIdentifier,
            topic: &FfiIdentifier,
            partition_id: u32,
            polling_strategy: &FfiPollingStrategy,
            count: u32,
            auto_commit: bool,
        ) -> Result<Vec<FfiIggyMessage>>;
    }
}

pub struct IggyClient {
    inner: Arc<RustIggyClient>,
}

fn create_client(conn: &str) -> Result<Box<IggyClient>, Box<dyn std::error::Error>> {
    let conn = if conn.is_empty() {
        "127.0.0.1:8090".to_string()
    } else {
        conn.to_string()
    };

    let client = RustIggyClientBuilder::new()
        .with_tcp()
        .with_server_address(conn)
        .build()
        .unwrap();
    Ok(Box::new(IggyClient {
        inner: Arc::new(client),
    }))
}

impl IggyClient {
    fn login_user(&self, username: &str, password: &str) -> Result<(), Box<dyn std::error::Error>> {
        RUNTIME
            .block_on(async { self.inner.login_user(username, password).await })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    fn connect(&self) -> Result<(), Box<dyn std::error::Error>> {
        RUNTIME
            .block_on(async { self.inner.connect().await })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    fn ping(&self) -> Result<(), Box<dyn std::error::Error>> {
        RUNTIME
            .block_on(async { self.inner.ping().await })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    fn create_stream(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        RUNTIME
            .block_on(async { self.inner.create_stream(name).await })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    fn get_stream(
        &self,
        stream_id: &ffi::FfiIdentifier,
    ) -> Result<Box<ffi::FfiStreamDetails>, Box<dyn std::error::Error>> {
        let stream_id = type_conversions::ffi_identifier_to_rust(stream_id)?;
        let details = RUNTIME
            .block_on(async { self.inner.get_stream(&stream_id).await })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        let details = details.ok_or("Stream not found")?;
        Ok(Box::new(type_conversions::rust_stream_details_to_ffi(&details)))
    }

    fn create_topic(
        &self,
        stream: &ffi::FfiIdentifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: &str,
        replication_factor: u8,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stream = type_conversions::ffi_identifier_to_rust(stream)?;
        let compression = if compression_algorithm.is_empty() {
            CompressionAlgorithm::default()
        } else {
            CompressionAlgorithm::from_str(compression_algorithm)
                .map_err(|_| Box::<dyn std::error::Error>::from("Invalid compression algorithm"))?
        };

        let replication = if replication_factor == 0 {
            None
        } else {
            Some(replication_factor)
        };

        RUNTIME
            .block_on(async {
                self.inner
                    .create_topic(
                        &stream,
                        name,
                        partitions_count,
                        compression,
                        replication,
                        IggyExpiry::NeverExpire,
                        MaxTopicSize::ServerDefault,
                    )
                    .await
            })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    fn get_topic(
        &self,
        stream_id: &ffi::FfiIdentifier,
        topic_id: &ffi::FfiIdentifier,
    ) -> Result<Box<ffi::FfiTopicDetails>, Box<dyn std::error::Error>> {
        let stream_id = type_conversions::ffi_identifier_to_rust(stream_id)?;
        let topic_id = type_conversions::ffi_identifier_to_rust(topic_id)?;
        let details = RUNTIME
            .block_on(async { self.inner.get_topic(&stream_id, &topic_id).await })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        let details = details.ok_or("Topic not found")?;
        Ok(Box::new(type_conversions::rust_topic_details_to_ffi(&details)))
    }

    fn send_messages(
        self: &IggyClient,
        stream: &ffi::FfiIdentifier,
        topic: &ffi::FfiIdentifier,
        partitioning: u32,
        messages: &Vec<ffi::FfiIggyMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stream = type_conversions::ffi_identifier_to_rust(stream)?;
        let topic = type_conversions::ffi_identifier_to_rust(topic)?;
        let partitioning = Partitioning::partition_id(partitioning);
        let mut rust_messages: Vec<RustIggyMessage> = Vec::with_capacity(messages.len());
        for message in messages {
            rust_messages.push(type_conversions::ffi_message_to_rust(message)?);
        }

        RUNTIME
            .block_on(async {
                self.inner
                    .send_messages(&stream, &topic, &partitioning, &mut rust_messages)
                    .await
            })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    fn poll_messages(
        &self,
        stream: &ffi::FfiIdentifier,
        topic: &ffi::FfiIdentifier,
        partition_id: u32,
        polling_strategy: &ffi::FfiPollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<Vec<ffi::FfiIggyMessage>, Box<dyn std::error::Error>> {
        let stream = type_conversions::ffi_identifier_to_rust(stream)?;
        let topic = type_conversions::ffi_identifier_to_rust(topic)?;
        let consumer = RustConsumer::default();
        let strategy = type_conversions::ffi_polling_strategy_to_rust(polling_strategy)?;
        let polled_messages = RUNTIME
            .block_on(async {
                self.inner
                    .poll_messages(
                        &stream,
                        &topic,
                        Some(partition_id),
                        &consumer,
                        &strategy,
                        count,
                        auto_commit,
                    )
                    .await
            })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        let mut messages = Vec::with_capacity(polled_messages.messages.len());
        for message in &polled_messages.messages {
            messages.push(type_conversions::rust_message_to_ffi(message)?);
        }

        Ok(messages)
    }
}
