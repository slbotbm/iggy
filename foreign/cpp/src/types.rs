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
use crate::ffi;
use bytes::Bytes;
use iggy::prelude::{
    BytesSerializable as RustBytesSerializable, CompressionAlgorithm as RustCompressionAlgorithm,
    HeaderKey as RustHeaderKey, HeaderValue as RustHeaderValue, Identifier as RustIdentifier,
    IggyByteSize as RustIggyByteSize, IggyExpiry as RustIggyExpiry, IggyMessage as RustIggyMessage,
    IggyMessageHeader as RustIggyMessageHeader, IggyTimestamp as RustIggyTimestamp,
    MaxTopicSize as RustMaxTopicSize, Partition as RustPartition,
    PollingStrategy as RustPollingStrategy, StreamDetails as RustStreamDetails, Topic as RustTopic,
    TopicDetails as RustTopicDetails, MAX_USER_HEADERS_SIZE,
};
use iggy_common::{HeaderKind as RustHeaderKind, PollingKind as RustPollingKind};
use std::collections::HashMap;

fn ffi_header_kind_to_rust(
    kind: ffi::FfiHeaderKind,
) -> Result<RustHeaderKind, Box<dyn std::error::Error>> {
    match kind {
        ffi::FfiHeaderKind::Raw => Ok(RustHeaderKind::Raw),
        ffi::FfiHeaderKind::String => Ok(RustHeaderKind::String),
        ffi::FfiHeaderKind::Bool => Ok(RustHeaderKind::Bool),
        ffi::FfiHeaderKind::Int8 => Ok(RustHeaderKind::Int8),
        ffi::FfiHeaderKind::Int16 => Ok(RustHeaderKind::Int16),
        ffi::FfiHeaderKind::Int32 => Ok(RustHeaderKind::Int32),
        ffi::FfiHeaderKind::Int64 => Ok(RustHeaderKind::Int64),
        ffi::FfiHeaderKind::Int128 => Ok(RustHeaderKind::Int128),
        ffi::FfiHeaderKind::Uint8 => Ok(RustHeaderKind::Uint8),
        ffi::FfiHeaderKind::Uint16 => Ok(RustHeaderKind::Uint16),
        ffi::FfiHeaderKind::Uint32 => Ok(RustHeaderKind::Uint32),
        ffi::FfiHeaderKind::Uint64 => Ok(RustHeaderKind::Uint64),
        ffi::FfiHeaderKind::Uint128 => Ok(RustHeaderKind::Uint128),
        ffi::FfiHeaderKind::Float32 => Ok(RustHeaderKind::Float32),
        ffi::FfiHeaderKind::Float64 => Ok(RustHeaderKind::Float64),
        _ => Err(Box::<dyn std::error::Error>::from(
            "Invalid FfiHeaderKind value",
        )),
    }
}

fn ffi_polling_kind_to_rust(
    kind: ffi::FfiPollingKind,
) -> Result<RustPollingKind, Box<dyn std::error::Error>> {
    match kind {
        ffi::FfiPollingKind::Offset => Ok(RustPollingKind::Offset),
        ffi::FfiPollingKind::Timestamp => Ok(RustPollingKind::Timestamp),
        ffi::FfiPollingKind::First => Ok(RustPollingKind::First),
        ffi::FfiPollingKind::Last => Ok(RustPollingKind::Last),
        ffi::FfiPollingKind::Next => Ok(RustPollingKind::Next),
        _ => Err(Box::<dyn std::error::Error>::from(
            "Invalid FfiPollingKind value",
        )),
    }
}

fn rust_header_kind_to_ffi(kind: RustHeaderKind) -> ffi::FfiHeaderKind {
    match kind {
        RustHeaderKind::Raw => ffi::FfiHeaderKind::Raw,
        RustHeaderKind::String => ffi::FfiHeaderKind::String,
        RustHeaderKind::Bool => ffi::FfiHeaderKind::Bool,
        RustHeaderKind::Int8 => ffi::FfiHeaderKind::Int8,
        RustHeaderKind::Int16 => ffi::FfiHeaderKind::Int16,
        RustHeaderKind::Int32 => ffi::FfiHeaderKind::Int32,
        RustHeaderKind::Int64 => ffi::FfiHeaderKind::Int64,
        RustHeaderKind::Int128 => ffi::FfiHeaderKind::Int128,
        RustHeaderKind::Uint8 => ffi::FfiHeaderKind::Uint8,
        RustHeaderKind::Uint16 => ffi::FfiHeaderKind::Uint16,
        RustHeaderKind::Uint32 => ffi::FfiHeaderKind::Uint32,
        RustHeaderKind::Uint64 => ffi::FfiHeaderKind::Uint64,
        RustHeaderKind::Uint128 => ffi::FfiHeaderKind::Uint128,
        RustHeaderKind::Float32 => ffi::FfiHeaderKind::Float32,
        RustHeaderKind::Float64 => ffi::FfiHeaderKind::Float64,
    }
}

fn ffi_identifier_kind_to_code(
    kind: ffi::FfiIdentifierKind,
) -> Result<u8, Box<dyn std::error::Error>> {
    match kind {
        ffi::FfiIdentifierKind::Numeric => Ok(1),
        ffi::FfiIdentifierKind::String => Ok(2),
        _ => Err(Box::<dyn std::error::Error>::from(
            "Invalid FfiIdentifierKind value",
        )),
    }
}

fn ffi_id_to_u128(id: &[u8]) -> Result<u128, Box<dyn std::error::Error>> {
    if id.is_empty() {
        return Ok(0);
    }
    if id.len() != 16 {
        return Err(format!("IggyMessageHeader.id must be 16 bytes, got {}", id.len()).into());
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(id);
    Ok(u128::from_le_bytes(buf))
}

fn u128_to_ffi_id(id: u128) -> Vec<u8> {
    id.to_le_bytes().to_vec()
}

pub(crate) fn ffi_identifier_to_rust(
    identifier: &ffi::FfiIdentifier,
) -> Result<RustIdentifier, Box<dyn std::error::Error>> {
    let length = identifier.length as usize;
    if length == 0 {
        return Err(Box::<dyn std::error::Error>::from(
            "Identifier length must be greater than 0",
        ));
    }
    if identifier.value.len() != length {
        return Err(Box::<dyn std::error::Error>::from(
            "Identifier length does not match value length",
        ));
    }
    if identifier.value.len() > u8::MAX as usize {
        return Err(Box::<dyn std::error::Error>::from(
            "Identifier value exceeds 255 bytes",
        ));
    }

    let kind_code = ffi_identifier_kind_to_code(identifier.kind)?;
    let mut raw = Vec::with_capacity(2 + identifier.value.len());
    raw.push(kind_code);
    raw.push(identifier.length);
    raw.extend_from_slice(&identifier.value);

    RustIdentifier::from_raw_bytes(&raw).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

pub(crate) fn ffi_polling_strategy_to_rust(
    strategy: &ffi::FfiPollingStrategy,
) -> Result<RustPollingStrategy, Box<dyn std::error::Error>> {
    match ffi_polling_kind_to_rust(strategy.kind)? {
        RustPollingKind::Offset => Ok(RustPollingStrategy::offset(strategy.value)),
        RustPollingKind::Timestamp => Ok(RustPollingStrategy::timestamp(strategy.value.into())),
        RustPollingKind::First => Ok(RustPollingStrategy::first()),
        RustPollingKind::Last => Ok(RustPollingStrategy::last()),
        RustPollingKind::Next => Ok(RustPollingStrategy::next()),
    }
}

fn timestamp_to_u64(timestamp: RustIggyTimestamp) -> u64 {
    u64::from(timestamp)
}

fn byte_size_to_u64(size: RustIggyByteSize) -> u64 {
    size.as_bytes_u64()
}

fn expiry_to_u64(expiry: RustIggyExpiry) -> u64 {
    u64::from(expiry)
}

fn max_topic_size_to_u64(size: RustMaxTopicSize) -> u64 {
    u64::from(size)
}

fn rust_compression_to_ffi(compression: RustCompressionAlgorithm) -> ffi::FfiCompressionAlgorithm {
    match compression {
        RustCompressionAlgorithm::None => ffi::FfiCompressionAlgorithm::None,
        RustCompressionAlgorithm::Gzip => ffi::FfiCompressionAlgorithm::Gzip,
    }
}

fn rust_partition_to_ffi(partition: &RustPartition) -> ffi::FfiPartition {
    ffi::FfiPartition {
        id: partition.id,
        created_at: timestamp_to_u64(partition.created_at),
        segments_count: partition.segments_count,
        current_offset: partition.current_offset,
        size_bytes: byte_size_to_u64(partition.size),
        messages_count: partition.messages_count,
    }
}

fn rust_topic_to_ffi(topic: &RustTopic) -> ffi::FfiTopic {
    ffi::FfiTopic {
        id: topic.id,
        created_at: timestamp_to_u64(topic.created_at),
        name: topic.name.clone(),
        size_bytes: byte_size_to_u64(topic.size),
        message_expiry: expiry_to_u64(topic.message_expiry),
        compression_algorithm: rust_compression_to_ffi(topic.compression_algorithm),
        max_topic_size: max_topic_size_to_u64(topic.max_topic_size),
        replication_factor: topic.replication_factor,
        messages_count: topic.messages_count,
        partitions_count: topic.partitions_count,
    }
}

pub(crate) fn rust_stream_details_to_ffi(details: &RustStreamDetails) -> ffi::FfiStreamDetails {
    let topics = details.topics.iter().map(rust_topic_to_ffi).collect();
    ffi::FfiStreamDetails {
        id: details.id,
        created_at: timestamp_to_u64(details.created_at),
        name: details.name.clone(),
        size_bytes: byte_size_to_u64(details.size),
        messages_count: details.messages_count,
        topics_count: details.topics_count,
        topics,
    }
}

pub(crate) fn rust_topic_details_to_ffi(details: &RustTopicDetails) -> ffi::FfiTopicDetails {
    let partitions = details
        .partitions
        .iter()
        .map(rust_partition_to_ffi)
        .collect();
    ffi::FfiTopicDetails {
        id: details.id,
        created_at: timestamp_to_u64(details.created_at),
        name: details.name.clone(),
        size_bytes: byte_size_to_u64(details.size),
        message_expiry: expiry_to_u64(details.message_expiry),
        compression_algorithm: rust_compression_to_ffi(details.compression_algorithm),
        max_topic_size: max_topic_size_to_u64(details.max_topic_size),
        replication_factor: details.replication_factor,
        messages_count: details.messages_count,
        partitions_count: details.partitions_count,
        partitions,
    }
}

pub(crate) fn ffi_message_to_rust(
    message: &ffi::FfiIggyMessage,
) -> Result<RustIggyMessage, Box<dyn std::error::Error>> {
    let id = ffi_id_to_u128(&message.header.id)?;
    let payload_length =
        u32::try_from(message.payload.len()).map_err(|_| "Payload length exceeds u32::MAX")?;

    let headers = ffi_header_map_to_headers(&message.headers)?;
    let user_headers = if headers.is_empty() {
        None
    } else {
        Some(headers.to_bytes())
    };
    let user_headers_len = user_headers.as_ref().map(|h| h.len()).unwrap_or(0);
    if user_headers_len > MAX_USER_HEADERS_SIZE as usize {
        return Err(Box::<dyn std::error::Error>::from(
            "User headers size exceeds MAX_USER_HEADERS_SIZE",
        ));
    }
    let user_headers_length =
        u32::try_from(user_headers_len).map_err(|_| "User headers length exceeds u32::MAX")?;

    let header = RustIggyMessageHeader {
        checksum: message.header.checksum,
        id,
        offset: message.header.offset,
        timestamp: message.header.timestamp,
        origin_timestamp: message.header.origin_timestamp,
        user_headers_length,
        payload_length,
    };

    Ok(RustIggyMessage {
        header,
        payload: Bytes::from(message.payload.clone()),
        user_headers,
    })
}

pub(crate) fn ffi_header_map_to_headers(
    map: &ffi::FfiHeaderMap,
) -> Result<HashMap<RustHeaderKey, RustHeaderValue>, Box<dyn std::error::Error>> {
    let mut headers = HashMap::with_capacity(map.entries.len());
    for entry in &map.entries {
        let key = RustHeaderKey::new(&entry.key)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let kind = ffi_header_kind_to_rust(entry.value.kind)?;
        if entry.value.value.is_empty() || entry.value.value.len() > 255 {
            return Err(Box::<dyn std::error::Error>::from(
                "Header value must be 1..=255 bytes",
            ));
        }
        let value = RustHeaderValue {
            kind,
            value: Bytes::from(entry.value.value.clone()),
        };
        headers.insert(key, value);
    }
    Ok(headers)
}

pub(crate) fn headers_to_ffi_header_map(
    headers: &HashMap<RustHeaderKey, RustHeaderValue>,
) -> Result<ffi::FfiHeaderMap, Box<dyn std::error::Error>> {
    let mut entries = Vec::with_capacity(headers.len());
    for (key, value) in headers {
        if value.value.is_empty() || value.value.len() > 255 {
            return Err(Box::<dyn std::error::Error>::from(
                "Header value must be 1..=255 bytes",
            ));
        }
        entries.push(ffi::FfiHeaderEntry {
            key: key.as_str().to_string(),
            value: ffi::FfiHeaderValue {
                kind: rust_header_kind_to_ffi(value.kind),
                value: value.value.to_vec(),
            },
        });
    }
    Ok(ffi::FfiHeaderMap { entries })
}

pub(crate) fn rust_message_to_ffi(
    message: &RustIggyMessage,
) -> Result<ffi::FfiIggyMessage, Box<dyn std::error::Error>> {
    let headers = match &message.user_headers {
        Some(bytes) => HashMap::<RustHeaderKey, RustHeaderValue>::from_bytes(bytes.clone())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?,
        None => HashMap::new(),
    };
    let headers = headers_to_ffi_header_map(&headers)?;
    let payload_length =
        u32::try_from(message.payload.len()).map_err(|_| "Payload length exceeds u32::MAX")?;
    let user_headers_length = message.user_headers.as_ref().map(|h| h.len()).unwrap_or(0);
    let user_headers_length =
        u32::try_from(user_headers_length).map_err(|_| "User headers length exceeds u32::MAX")?;

    Ok(ffi::FfiIggyMessage {
        header: ffi::FfiIggyMessageHeader {
            checksum: message.header.checksum,
            id: u128_to_ffi_id(message.header.id),
            offset: message.header.offset,
            timestamp: message.header.timestamp,
            origin_timestamp: message.header.origin_timestamp,
            user_headers_length,
            payload_length,
        },
        payload: message.payload.to_vec(),
        headers,
    })
}
