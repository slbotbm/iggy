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

use crate::{Identifier, RUNTIME, StreamDetails, TopicDetails, ffi, iggy_error_to_anyhow};
use anyhow::{Result, anyhow};
use iggy::prelude::{
    Client as RustClient, CompressionAlgorithm as RustCompressionAlgorithm, Consumer,
    IggyClient as RustIggyClient, IggyClientBuilder as RustIggyClientBuilder,
    IggyError as RustIggyError, IggyExpiry as RustIggyExpiry, IggyMessage as RustMessage,
    MaxTopicSize as RustMaxTopicSize, MessageClient, Partitioning,
    PollingStrategy as RustPollingStrategy, StreamClient, TopicClient, UserClient,
};
use std::str::FromStr;
use std::sync::Arc;

pub(crate) struct Client {
    pub(crate) inner: Arc<RustIggyClient>,
}

pub(crate) fn new_connection(connection_string: &str) -> Result<*mut Client> {
    let client = if connection_string.is_empty() {
        RustIggyClientBuilder::new()
            .with_tcp()
            .build()
            .map_err(iggy_error_to_anyhow)?
    } else if connection_string.starts_with("iggy://") || connection_string.starts_with("iggy+") {
        RustIggyClient::from_connection_string(connection_string).map_err(iggy_error_to_anyhow)?
    } else {
        RustIggyClientBuilder::new()
            .with_tcp()
            .with_server_address(connection_string.to_string())
            .build()
            .map_err(iggy_error_to_anyhow)?
    };

    Ok(Box::into_raw(Box::new(Client {
        inner: Arc::new(client),
    })))
}

impl Client {
    pub(crate) fn connect(&self) -> Result<()> {
        RUNTIME
            .block_on(self.inner.connect())
            .map_err(iggy_error_to_anyhow)?;
        Ok(())
    }

    pub(crate) fn login_user(&self, username: &str, password: &str) -> Result<()> {
        RUNTIME
            .block_on(self.inner.login_user(username, password))
            .map_err(iggy_error_to_anyhow)?;
        Ok(())
    }

    pub(crate) fn create_stream(&self, stream_name: &str) -> Result<()> {
        RUNTIME
            .block_on(self.inner.create_stream(stream_name))
            .map_err(iggy_error_to_anyhow)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: &str,
        replication_factor: u8,
        expiry: &str,
        max_topic_size: &str,
    ) -> Result<()> {
        let compression_algorithm = RustCompressionAlgorithm::from_str(compression_algorithm)
            .map_err(|error| anyhow!(error))?;
        let replication_factor = if replication_factor == 0 {
            None
        } else {
            Some(replication_factor)
        };
        let expiry = RustIggyExpiry::from_str(expiry).map_err(|error| anyhow!(error))?;
        let max_topic_size =
            RustMaxTopicSize::from_str(max_topic_size).map_err(|error| anyhow!(error))?;

        RUNTIME
            .block_on(self.inner.create_topic(
                stream_id.as_rust(),
                name,
                partitions_count,
                compression_algorithm,
                replication_factor,
                expiry,
                max_topic_size,
            ))
            .map_err(iggy_error_to_anyhow)?;
        Ok(())
    }

    pub(crate) fn send_messages(
        &self,
        messages: Vec<ffi::SentMessage>,
        stream_id: &Identifier,
        topic: &Identifier,
        partitioning: u32,
    ) -> Result<()> {
        let mut rust_messages: Vec<RustMessage> = messages
            .into_iter()
            .map(RustMessage::try_from)
            .collect::<std::result::Result<_, _>>()
            .map_err(iggy_error_to_anyhow)?;

        let partitioning = Partitioning::partition_id(partitioning);
        RUNTIME
            .block_on(self.inner.send_messages(
                stream_id.as_rust(),
                topic.as_rust(),
                &partitioning,
                rust_messages.as_mut(),
            ))
            .map_err(iggy_error_to_anyhow)?;
        Ok(())
    }

    pub(crate) fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic: &Identifier,
        partition_id: u32,
        polling_strategy: ffi::PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<ffi::PolledMessages> {
        let consumer = Consumer::default();
        let polling_strategy =
            RustPollingStrategy::try_from(polling_strategy).map_err(iggy_error_to_anyhow)?;

        let polled_messages = RUNTIME
            .block_on(self.inner.poll_messages(
                stream_id.as_rust(),
                topic.as_rust(),
                Some(partition_id),
                &consumer,
                &polling_strategy,
                count,
                auto_commit,
            ))
            .map_err(iggy_error_to_anyhow)?;

        Ok(ffi::PolledMessages::from(&polled_messages))
    }

    pub(crate) fn get_stream(&self, stream_id: &Identifier) -> Result<*mut StreamDetails> {
        let stream = RUNTIME
            .block_on(self.inner.get_stream(stream_id.as_rust()))
            .map_err(iggy_error_to_anyhow)?;
        match stream {
            Some(details) => Ok(Box::into_raw(Box::new(StreamDetails::new(details)))),
            None => Err(iggy_error_to_anyhow(RustIggyError::ResourceNotFound(
                "stream not found".to_string(),
            ))),
        }
    }

    pub(crate) fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<*mut TopicDetails> {
        let topic = RUNTIME
            .block_on(
                self.inner
                    .get_topic(stream_id.as_rust(), topic_id.as_rust()),
            )
            .map_err(iggy_error_to_anyhow)?;
        match topic {
            Some(details) => Ok(Box::into_raw(Box::new(TopicDetails::new(details)))),
            None => Err(iggy_error_to_anyhow(RustIggyError::ResourceNotFound(
                "topic not found".to_string(),
            ))),
        }
    }
}

pub(crate) unsafe fn delete_connection(client: *mut Client) {
    if client.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(client));
    }
}
