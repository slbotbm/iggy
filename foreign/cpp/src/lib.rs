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

use anyhow::Result;
use iggy::prelude::{
    Client, Identifier, IggyClient as RustIggyClient, IggyClientBuilder as RustIggyClientBuilder,
    StreamClient, TopicClient, UserClient,
};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "iggy")]
mod ffi {
    enum CompressionAlgorithm {
        None = 1,
        Gzip = 2,
    }

    enum ExpiryKind {
        ServerDefault = 0,
        ExpireDuration = 1,
        NeverExpire = 2,
    }

    struct Expiry {
        kind: ExpiryKind,
        value: u64,
    }

    enum MaxTopicSizeKind {
        ServerDefault = 0,
        Custom = 1,
        Unlimited = 2,
    }

    struct MaxTopicSize {
        kind: MaxTopicSizeKind,
        value: u64,
    }

    extern "Rust" {
        type IggyClient;

        fn new_connection(connection_string: &str) -> Result<*mut IggyClient>;
        unsafe fn delete_connection(client: *mut IggyClient);
        fn login_user(self: &IggyClient, username: &str, password: &str) -> Result<()>;
        fn connect(self: &IggyClient) -> Result<()>;
        fn create_stream(self: &IggyClient, stream_name: &str) -> Result<()>;
        fn create_topic(
            self: &IggyClient,
            stream_id: &str,
            name: &str,
            partitions_count: u32,
            compression_algorithm: CompressionAlgorithm,
            replication_factor: u8,
            expiry: Expiry,
            max_topic_size: MaxTopicSize,
        ) -> Result<()>;
    }
}

pub struct IggyClient {
    inner: Arc<RustIggyClient>,
}

pub fn new_connection(connection_string: &str) -> Result<*mut IggyClient> {
    let client = if connection_string.is_empty() {
        RustIggyClientBuilder::new().with_tcp().build()?
    } else if connection_string.starts_with("iggy://") || connection_string.starts_with("iggy+") {
        RustIggyClient::from_connection_string(connection_string)?
    } else {
        RustIggyClientBuilder::new()
            .with_tcp()
            .with_server_address(connection_string.to_string())
            .build()?
    };

    Ok(Box::into_raw(Box::new(IggyClient {
        inner: Arc::new(client),
    })))
}

impl IggyClient {
    fn connect(&self) -> Result<()> {
        RUNTIME.block_on(self.inner.connect())?;
        Ok(())
    }

    fn login_user(&self, username: &str, password: &str) -> Result<()> {
        RUNTIME.block_on(self.inner.login_user(username, password))?;
        Ok(())
    }

    fn create_stream(&self, stream_name: &str) -> Result<()> {
        RUNTIME.block_on(self.inner.create_stream(stream_name))?;
        Ok(())
    }

    fn create_topic(
        &self,
        stream_id: &str,
        name: &str,
        partitions_count: u32,
        compression_algorithm: ffi::CompressionAlgorithm,
        replication_factor: u8,
        expiry: ffi::Expiry,
        max_topic_size: ffi::MaxTopicSize,
    ) -> Result<()> {
        let stream_id = Identifier::from_str(stream_id)?;
        let compression_algorithm =
            type_conversions::compression_algorithm_from_ffi(compression_algorithm)?;
        let replication_factor = if replication_factor == 0 {
            None
        } else {
            Some(replication_factor)
        };
        let expiry = type_conversions::expiry_from_ffi(expiry)?;
        let max_topic_size = type_conversions::max_topic_size_from_ffi(max_topic_size)?;

        RUNTIME.block_on(self.inner.create_topic(
            &stream_id,
            name,
            partitions_count,
            compression_algorithm,
            replication_factor,
            expiry,
            max_topic_size,
        ))?;
        Ok(())
    }
}

pub unsafe fn delete_connection(client: *mut IggyClient) {
    if client.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(client));
    }
}
