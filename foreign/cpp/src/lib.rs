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
mod client;
mod identifier;
mod stream_details;
mod topic_details;

use client::{Client, delete_connection, new_connection};
use identifier::{Identifier, delete_identifier, identifier_from_named, identifier_from_numeric};
use stream_details::{StreamDetails, delete_stream_details};
use topic_details::{TopicDetails, delete_topic_details};

use anyhow::anyhow;
use bytes::Bytes;
use iggy::prelude::IggyError as RustIggyError;
use iggy::prelude::{
    IggyMessage as RustMessage, IggyTimestamp, PolledMessages as RustPolledMessages, PollingKind,
    PollingStrategy as RustPollingStrategy,
};
use std::str::FromStr;
use std::sync::LazyLock;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "iggy::ffi")]
mod ffi {
    struct PollingStrategy {
        kind: String,
        value: u64,
    }

    struct PolledMessage {
        id_high: u64,
        id_low: u64,
        offset: u64,
        timestamp: u64,
        checksum: u64,
        payload_length: u32,
        payload: Vec<u8>,
    }

    struct PolledMessages {
        partition_id: u32,
        current_offset: u64,
        count: u32,
        messages: Vec<PolledMessage>,
    }

    struct SentMessage {
        payload: Vec<u8>,
    }

    extern "Rust" {
        type Client;
        type Identifier;
        type StreamDetails;
        type TopicDetails;

        // Client functions
        fn new_connection(connection_string: &str) -> Result<*mut Client>;
        fn login_user(self: &Client, username: &str, password: &str) -> Result<()>;
        fn connect(self: &Client) -> Result<()>;
        fn create_stream(self: &Client, stream_name: &str) -> Result<()>;
        #[allow(clippy::too_many_arguments)]
        fn create_topic(
            self: &Client,
            stream_id: &Identifier,
            name: &str,
            partitions_count: u32,
            compression_algorithm: &str,
            replication_factor: u8,
            expiry: &str,
            max_topic_size: &str,
        ) -> Result<()>;
        fn send_messages(
            self: &Client,
            messages: Vec<SentMessage>,
            stream_id: &Identifier,
            topic: &Identifier,
            partitioning: u32,
        ) -> Result<()>;
        fn poll_messages(
            self: &Client,
            stream_id: &Identifier,
            topic: &Identifier,
            partition_id: u32,
            polling_strategy: PollingStrategy,
            count: u32,
            auto_commit: bool,
        ) -> Result<PolledMessages>;
        fn get_stream(self: &Client, stream_id: &Identifier) -> Result<*mut StreamDetails>;
        fn get_topic(
            self: &Client,
            stream_id: &Identifier,
            topic_id: &Identifier,
        ) -> Result<*mut TopicDetails>;

        // Identifier functions
        fn identifier_from_named(identifier_name: &str) -> Result<*mut Identifier>;
        fn identifier_from_numeric(identifier_id: u32) -> Result<*mut Identifier>;
        fn get_value(self: &Identifier) -> String;

        // StreamDetails functions
        fn id(self: &StreamDetails) -> u32;
        fn name(self: &StreamDetails) -> String;
        fn messages_count(self: &StreamDetails) -> u64;
        fn topics_count(self: &StreamDetails) -> u32;

        // TopicDetails functions
        fn id(self: &TopicDetails) -> u32;
        fn name(self: &TopicDetails) -> String;
        fn messages_count(self: &TopicDetails) -> u64;
        fn partitions_count(self: &TopicDetails) -> u32;

        // Destroyers
        unsafe fn delete_connection(client: *mut Client);
        unsafe fn delete_identifier(identifier: *mut Identifier);
        unsafe fn delete_stream_details(details: *mut StreamDetails);
        unsafe fn delete_topic_details(details: *mut TopicDetails);

    }
}

fn iggy_error_to_anyhow(error: RustIggyError) -> anyhow::Error {
    anyhow!("{}:{}", error.as_code(), error.as_string())
}

impl From<&RustMessage> for ffi::PolledMessage {
    fn from(message: &RustMessage) -> Self {
        let id_high = (message.header.id >> 64) as u64;
        let id_low = message.header.id as u64;

        ffi::PolledMessage {
            id_high,
            id_low,
            offset: message.header.offset,
            timestamp: message.header.timestamp,
            checksum: message.header.checksum,
            payload_length: message.header.payload_length,
            payload: message.payload.to_vec(),
        }
    }
}

impl From<&RustPolledMessages> for ffi::PolledMessages {
    fn from(polled_messages: &RustPolledMessages) -> Self {
        let messages = polled_messages
            .messages
            .iter()
            .map(ffi::PolledMessage::from)
            .collect();

        ffi::PolledMessages {
            partition_id: polled_messages.partition_id,
            current_offset: polled_messages.current_offset,
            count: polled_messages.count,
            messages,
        }
    }
}

impl TryFrom<ffi::SentMessage> for RustMessage {
    fn try_from(message: ffi::SentMessage) -> Result<Self, RustIggyError> {
        let payload = Bytes::from(message.payload);
        let message = RustMessage::builder().payload(payload).build()?;

        Ok(message)
    }
}

impl TryFrom<ffi::PollingStrategy> for RustPollingStrategy {
    fn try_from(strategy: ffi::PollingStrategy) -> Result<Self, RustIggyError> {
        match PollingKind::from_str(strategy.kind.as_str())? {
            PollingKind::Offset => Ok(RustPollingStrategy::offset(strategy.value)),
            PollingKind::Timestamp => Ok(RustPollingStrategy::timestamp(IggyTimestamp::from(
                strategy.value,
            ))),
            PollingKind::First => Ok(RustPollingStrategy::first()),
            PollingKind::Last => Ok(RustPollingStrategy::last()),
            PollingKind::Next => Ok(RustPollingStrategy::next()),
        }
    }
}
