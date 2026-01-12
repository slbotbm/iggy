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
    IggyClientBuilder as RustIggyClientBuilder, IggyError as RustIggyError,
    IggyMessage as RustIggyMessage, *,
};
use std::fmt;
use std::result::Result;
use std::sync::{Arc, LazyLock};

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime.")
});

#[cxx::bridge(namespace = "iggy::ffi")]
mod ffi {

    enum HeaderKind {
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

    enum IdKind {
        Numeric = 1,
        String = 2,
    }

    enum CompressionAlgorithm {
        None = 1,
        Gzip = 2,
    }

    enum IggyExpiryKind {
        ServerDefault = 1,
        ExpireDuration = 2,
        NeverExpire = 3,
    }

    enum MaxTopicSizeKind {
        ServerDefault = 1,
        Custom = 2,
        Unlimited = 3,
    }

    enum PollingKind {
        Offset = 1,
        Timestamp = 2,
        First = 3,
        Last = 4,
        Next = 5,
    }

    struct HeaderValue {
        kind: HeaderKind,
        value: Vec<u8>,
    }

    struct HeaderKey {
        value: String,
    }

    struct HeaderEntry {
        key: HeaderKey,
        value: HeaderValue,
    }

    struct HeaderMap {
        entries: Vec<HeaderEntry>,
    }

    struct IggyMessageHeader {
        checksum: u64,
        id: Vec<u8>,
        offset: u64,
        timestamp: u64,
        origin_timestamp: u64,
        user_headers_length: u32,
        payload_length: u32,
    }

    struct Identifier {
        kind: IdKind,
        length: u8,
        value: Vec<u8>,
    }

    struct IggyExpiry {
        kind: IggyExpiryKind,
        value: u64,
    }

    struct IggyTimestamp {
        value: u64,
    }

    struct IggyByteSize {
        value: u64,
    }

    struct MaxTopicSize {
        kind: MaxTopicSizeKind,
        value: IggyByteSize,
    }

    struct PollingStrategy {
        kind: PollingKind,
        value: u64,
    }

    struct IggyMessage {
        header: IggyMessageHeader,
        payload: Vec<u8>,
        headers: HeaderMap,
    }

    struct Partition {
        id: u32,
        created_at: IggyTimestamp,
        segments_count: u32,
        current_offset: u64,
        size: IggyByteSize,
        messages_count: u64,
    }

    struct Topic {
        id: u32,
        created_at: IggyTimestamp,
        name: String,
        size: IggyByteSize,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
        messages_count: u64,
        partitions_count: u32,
    }

    struct StreamDetails {
        id: u32,
        created_at: IggyTimestamp,
        name: String,
        size: IggyByteSize,
        messages_count: u64,
        topics_count: u32,
        topics: Vec<Topic>,
    }

    struct TopicDetails {
        id: u32,
        created_at: IggyTimestamp,
        name: String,
        size: IggyByteSize,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
        messages_count: u64,
        partitions_count: u32,
        partitions: Vec<Partition>,
    }

    struct IggyError {
        code: u32,
        message: String,
    }

    extern "Rust" {
        type IggyClient;

        // analogous to new()
        fn create_client(conn: &str) -> Result<Box<IggyClient>>;

        fn login_user(self: &IggyClient, username: &str, password: &str) -> Result<()>;
        fn connect(self: &IggyClient) -> Result<()>;
        fn ping(self: &IggyClient) -> Result<()>;

        fn create_stream(self: &IggyClient, name: &str) -> Result<()>;
        fn get_stream(self: &IggyClient, stream_id: &Identifier) -> Result<StreamDetails>;
        fn create_topic(
            self: &IggyClient,
            stream: &Identifier,
            name: &str,
            partitions_count: u32,
            compression_algorithm: CompressionAlgorithm,
            replication_factor: u8,
            message_expiry: &IggyExpiry,
            max_topic_size: &MaxTopicSize,
        ) -> Result<()>;
        fn get_topic(
            self: &IggyClient,
            stream_id: &Identifier,
            topic_id: &Identifier,
        ) -> Result<TopicDetails>;
        fn send_messages(
            self: &IggyClient,
            stream_id: &Identifier,
            topic_id: &Identifier,
            partitioning: u32,
            messages: Vec<IggyMessage>,
        ) -> Result<()>;
        fn poll_messages(
            self: &IggyClient,
            stream_id: &Identifier,
            topic_id: &Identifier,
            partition_id: u32,
            polling_strategy: &PollingStrategy,
            count: u32,
            auto_commit: bool,
        ) -> Result<Vec<IggyMessage>>;
    }
}

impl fmt::Display for ffi::IggyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl From<RustIggyError> for ffi::IggyError {
    fn from(error: RustIggyError) -> Self {
        Self {
            code: error.as_code(),
            message: error.to_string(),
        }
    }
}

pub struct IggyClient {
    inner: Arc<RustIggyClient>,
}

fn create_client(conn: &str) -> Result<Box<IggyClient>, ffi::IggyError> {
    let conn = if conn.is_empty() {
        "127.0.0.1:8090".to_string()
    } else {
        conn.to_string()
    };

    let client = RustIggyClientBuilder::new()
        .with_tcp()
        .with_server_address(conn)
        .build()?;

    Ok(Box::new(IggyClient {
        inner: Arc::new(client),
    }))
}

impl IggyClient {
    fn login_user(&self, username: &str, password: &str) -> Result<(), ffi::IggyError> {
        RUNTIME.block_on(async { self.inner.login_user(username, password).await })?;

        Ok(())
    }

    fn connect(&self) -> Result<(), ffi::IggyError> {
        RUNTIME.block_on(async { self.inner.connect().await })?;

        Ok(())
    }

    fn ping(&self) -> Result<(), ffi::IggyError> {
        RUNTIME.block_on(async { self.inner.ping().await })?;

        Ok(())
    }

    fn create_stream(&self, name: &str) -> Result<(), ffi::IggyError> {
        RUNTIME.block_on(async { self.inner.create_stream(name).await })?;

        Ok(())
    }

    fn get_stream(
        &self,
        stream_id: &ffi::Identifier,
    ) -> Result<ffi::StreamDetails, ffi::IggyError> {
        let stream_id = type_conversions::ffi_identifier_to_rust(stream_id)?;
        let details = RUNTIME.block_on(async { self.inner.get_stream(&stream_id).await })?;

        let details =
            details.ok_or_else(|| RustIggyError::ResourceNotFound("stream".to_string()))?;

        Ok(type_conversions::rust_stream_details_to_ffi(&details)?)
    }

    fn create_topic(
        &self,
        stream: &ffi::Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: ffi::CompressionAlgorithm,
        replication_factor: u8,
        message_expiry: &ffi::IggyExpiry,
        max_topic_size: &ffi::MaxTopicSize,
    ) -> Result<(), ffi::IggyError> {
        let stream = type_conversions::ffi_identifier_to_rust(stream)?;
        let compression = type_conversions::ffi_compression_to_rust(compression_algorithm)?;

        let message_expiry = type_conversions::ffi_expiry_to_rust(message_expiry)?;
        let max_topic_size = type_conversions::ffi_max_topic_size_to_rust(max_topic_size)?;

        let replication = if replication_factor == 0 {
            None
        } else {
            Some(replication_factor)
        };

        RUNTIME.block_on(async {
            self.inner
                .create_topic(
                    &stream,
                    name,
                    partitions_count,
                    compression,
                    replication,
                    message_expiry,
                    max_topic_size,
                )
                .await
        })?;

        Ok(())
    }

    fn get_topic(
        &self,
        stream_id: &ffi::Identifier,
        topic_id: &ffi::Identifier,
    ) -> Result<ffi::TopicDetails, ffi::IggyError> {
        let stream_id = type_conversions::ffi_identifier_to_rust(stream_id)?;
        let topic_id = type_conversions::ffi_identifier_to_rust(topic_id)?;
        let details =
            RUNTIME.block_on(async { self.inner.get_topic(&stream_id, &topic_id).await })?;

        let details =
            details.ok_or_else(|| RustIggyError::ResourceNotFound("topic".to_string()))?;
        Ok(type_conversions::rust_topic_details_to_ffi(&details)?)
    }

    fn send_messages(
        &self,
        stream_id: &ffi::Identifier,
        topic_id: &ffi::Identifier,
        partitioning: u32,
        messages: Vec<ffi::IggyMessage>,
    ) -> Result<(), ffi::IggyError> {
        let stream = type_conversions::ffi_identifier_to_rust(stream_id)?;
        let topic = type_conversions::ffi_identifier_to_rust(topic_id)?;
        let partitioning = Partitioning::partition_id(partitioning);
        let mut rust_messages: Vec<RustIggyMessage> = Vec::with_capacity(messages.len());
        for message in messages {
            rust_messages.push(type_conversions::ffi_message_to_rust(message)?);
        }

        RUNTIME.block_on(async {
            self.inner
                .send_messages(&stream, &topic, &partitioning, &mut rust_messages)
                .await
        })?;

        Ok(())
    }

    fn poll_messages(
        &self,
        stream_id: &ffi::Identifier,
        topic_id: &ffi::Identifier,
        partition_id: u32,
        polling_strategy: &ffi::PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<Vec<ffi::IggyMessage>, ffi::IggyError> {
        let stream = type_conversions::ffi_identifier_to_rust(stream_id)?;
        let topic = type_conversions::ffi_identifier_to_rust(topic_id)?;
        let consumer = RustConsumer::default();
        let strategy = type_conversions::ffi_polling_strategy_to_rust(polling_strategy)?;
        let polled_messages = RUNTIME.block_on(async {
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
        })?;

        let mut messages = Vec::with_capacity(polled_messages.messages.len());
        for message in &polled_messages.messages {
            messages.push(type_conversions::rust_message_to_ffi(message)?);
        }

        Ok(messages)
    }
}
