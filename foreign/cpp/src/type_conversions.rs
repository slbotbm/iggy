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
    IggyByteSize as RustIggyByteSize, IggyError as RustIggyError, IggyExpiry as RustIggyExpiry,
    IggyMessage as RustIggyMessage, IggyMessageHeader as RustIggyMessageHeader,
    IggyTimestamp as RustIggyTimestamp, MAX_USER_HEADERS_SIZE, MaxTopicSize as RustMaxTopicSize,
    Partition as RustPartition, PollingStrategy as RustPollingStrategy,
    StreamDetails as RustStreamDetails, Topic as RustTopic, TopicDetails as RustTopicDetails,
};
use iggy_common::{HeaderKind as RustHeaderKind, PollingKind as RustPollingKind};
use std::collections::HashMap;

fn ffi_header_kind_to_rust(kind: ffi::HeaderKind) -> Result<RustHeaderKind, RustIggyError> {
    match kind {
        ffi::HeaderKind::Raw => Ok(RustHeaderKind::Raw),
        ffi::HeaderKind::String => Ok(RustHeaderKind::String),
        ffi::HeaderKind::Bool => Ok(RustHeaderKind::Bool),
        ffi::HeaderKind::Int8 => Ok(RustHeaderKind::Int8),
        ffi::HeaderKind::Int16 => Ok(RustHeaderKind::Int16),
        ffi::HeaderKind::Int32 => Ok(RustHeaderKind::Int32),
        ffi::HeaderKind::Int64 => Ok(RustHeaderKind::Int64),
        ffi::HeaderKind::Int128 => Ok(RustHeaderKind::Int128),
        ffi::HeaderKind::Uint8 => Ok(RustHeaderKind::Uint8),
        ffi::HeaderKind::Uint16 => Ok(RustHeaderKind::Uint16),
        ffi::HeaderKind::Uint32 => Ok(RustHeaderKind::Uint32),
        ffi::HeaderKind::Uint64 => Ok(RustHeaderKind::Uint64),
        ffi::HeaderKind::Uint128 => Ok(RustHeaderKind::Uint128),
        ffi::HeaderKind::Float32 => Ok(RustHeaderKind::Float32),
        ffi::HeaderKind::Float64 => Ok(RustHeaderKind::Float64),
        _ => Err(RustIggyError::InvalidFormat),
    }
}

fn ffi_polling_kind_to_rust(kind: ffi::PollingKind) -> Result<RustPollingKind, RustIggyError> {
    match kind {
        ffi::PollingKind::Offset => Ok(RustPollingKind::Offset),
        ffi::PollingKind::Timestamp => Ok(RustPollingKind::Timestamp),
        ffi::PollingKind::First => Ok(RustPollingKind::First),
        ffi::PollingKind::Last => Ok(RustPollingKind::Last),
        ffi::PollingKind::Next => Ok(RustPollingKind::Next),
        _ => Err(RustIggyError::InvalidFormat),
    }
}

pub(crate) fn ffi_compression_to_rust(
    compression: ffi::CompressionAlgorithm,
) -> Result<RustCompressionAlgorithm, RustIggyError> {
    match compression {
        ffi::CompressionAlgorithm::None => Ok(RustCompressionAlgorithm::None),
        ffi::CompressionAlgorithm::Gzip => Ok(RustCompressionAlgorithm::Gzip),
        _ => Err(RustIggyError::InvalidFormat),
    }
}

fn rust_header_kind_to_ffi(kind: RustHeaderKind) -> Result<ffi::HeaderKind, RustIggyError> {
    match kind {
        RustHeaderKind::Raw => Ok(ffi::HeaderKind::Raw),
        RustHeaderKind::String => Ok(ffi::HeaderKind::String),
        RustHeaderKind::Bool => Ok(ffi::HeaderKind::Bool),
        RustHeaderKind::Int8 => Ok(ffi::HeaderKind::Int8),
        RustHeaderKind::Int16 => Ok(ffi::HeaderKind::Int16),
        RustHeaderKind::Int32 => Ok(ffi::HeaderKind::Int32),
        RustHeaderKind::Int64 => Ok(ffi::HeaderKind::Int64),
        RustHeaderKind::Int128 => Ok(ffi::HeaderKind::Int128),
        RustHeaderKind::Uint8 => Ok(ffi::HeaderKind::Uint8),
        RustHeaderKind::Uint16 => Ok(ffi::HeaderKind::Uint16),
        RustHeaderKind::Uint32 => Ok(ffi::HeaderKind::Uint32),
        RustHeaderKind::Uint64 => Ok(ffi::HeaderKind::Uint64),
        RustHeaderKind::Uint128 => Ok(ffi::HeaderKind::Uint128),
        RustHeaderKind::Float32 => Ok(ffi::HeaderKind::Float32),
        RustHeaderKind::Float64 => Ok(ffi::HeaderKind::Float64),
    }
}

fn ffi_id_to_u128(id: &[u8]) -> Result<u128, RustIggyError> {
    if id.is_empty() {
        return Ok(0);
    }
    if id.len() != 16 {
        return Err(RustIggyError::InvalidFormat);
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(id);

    Ok(u128::from_le_bytes(buf))
}

pub(crate) fn ffi_identifier_to_rust(
    identifier: &ffi::Identifier,
) -> Result<RustIdentifier, RustIggyError> {
    let length = identifier.length as usize;
    if length == 0 {
        return Err(RustIggyError::InvalidIdentifier);
    }
    if identifier.value.len() != length {
        return Err(RustIggyError::InvalidIdentifier);
    }
    if identifier.value.len() > u8::MAX as usize {
        return Err(RustIggyError::InvalidIdentifier);
    }

    let kind_code = match identifier.kind {
        ffi::IdKind::Numeric => 1,
        ffi::IdKind::String => 2,
        _ => {
            return Err(RustIggyError::InvalidIdentifier);
        }
    };
    let mut raw = Vec::with_capacity(2 + identifier.value.len());
    raw.push(kind_code);
    raw.push(identifier.length);
    raw.extend_from_slice(&identifier.value);

    RustIdentifier::from_raw_bytes(&raw)
}

pub(crate) fn ffi_polling_strategy_to_rust(
    strategy: &ffi::PollingStrategy,
) -> Result<RustPollingStrategy, RustIggyError> {
    match ffi_polling_kind_to_rust(strategy.kind)? {
        RustPollingKind::Offset => Ok(RustPollingStrategy::offset(strategy.value)),
        RustPollingKind::Timestamp => Ok(RustPollingStrategy::timestamp(strategy.value.into())),
        RustPollingKind::First => Ok(RustPollingStrategy::first()),
        RustPollingKind::Last => Ok(RustPollingStrategy::last()),
        RustPollingKind::Next => Ok(RustPollingStrategy::next()),
    }
}

pub(crate) fn ffi_expiry_to_rust(
    expiry: &ffi::IggyExpiry,
) -> Result<RustIggyExpiry, RustIggyError> {
    match expiry.kind {
        ffi::IggyExpiryKind::ServerDefault => Ok(RustIggyExpiry::ServerDefault),
        ffi::IggyExpiryKind::ExpireDuration => {
            Ok(RustIggyExpiry::ExpireDuration(expiry.value.into()))
        }
        ffi::IggyExpiryKind::NeverExpire => Ok(RustIggyExpiry::NeverExpire),
        _ => Err(RustIggyError::InvalidFormat),
    }
}

pub(crate) fn ffi_max_topic_size_to_rust(
    size: &ffi::MaxTopicSize,
) -> Result<RustMaxTopicSize, RustIggyError> {
    match size.kind {
        ffi::MaxTopicSizeKind::ServerDefault => Ok(RustMaxTopicSize::ServerDefault),
        ffi::MaxTopicSizeKind::Unlimited => Ok(RustMaxTopicSize::Unlimited),
        ffi::MaxTopicSizeKind::Custom => Ok(RustMaxTopicSize::Custom(RustIggyByteSize::from(
            size.value.value,
        ))),
        _ => Err(RustIggyError::InvalidFormat),
    }
}

fn rust_timestamp_to_ffi(
    timestamp: RustIggyTimestamp,
) -> Result<ffi::IggyTimestamp, RustIggyError> {
    Ok(ffi::IggyTimestamp {
        value: u64::from(timestamp),
    })
}

fn rust_byte_size_to_ffi(size: RustIggyByteSize) -> Result<ffi::IggyByteSize, RustIggyError> {
    Ok(ffi::IggyByteSize {
        value: size.as_bytes_u64(),
    })
}

fn rust_expiry_to_ffi(expiry: RustIggyExpiry) -> Result<ffi::IggyExpiry, RustIggyError> {
    match expiry {
        RustIggyExpiry::ServerDefault => Ok(ffi::IggyExpiry {
            kind: ffi::IggyExpiryKind::ServerDefault,
            value: 0,
        }),
        RustIggyExpiry::ExpireDuration(duration) => Ok(ffi::IggyExpiry {
            kind: ffi::IggyExpiryKind::ExpireDuration,
            value: duration.as_micros(),
        }),
        RustIggyExpiry::NeverExpire => Ok(ffi::IggyExpiry {
            kind: ffi::IggyExpiryKind::NeverExpire,
            value: u64::MAX,
        }),
    }
}

fn rust_max_topic_size_to_ffi(size: RustMaxTopicSize) -> Result<ffi::MaxTopicSize, RustIggyError> {
    match size {
        RustMaxTopicSize::ServerDefault => Ok(ffi::MaxTopicSize {
            kind: ffi::MaxTopicSizeKind::ServerDefault,
            value: ffi::IggyByteSize { value: 0 },
        }),
        RustMaxTopicSize::Unlimited => Ok(ffi::MaxTopicSize {
            kind: ffi::MaxTopicSizeKind::Unlimited,
            value: ffi::IggyByteSize { value: u64::MAX },
        }),
        RustMaxTopicSize::Custom(custom) => Ok(ffi::MaxTopicSize {
            kind: ffi::MaxTopicSizeKind::Custom,
            value: rust_byte_size_to_ffi(custom)?,
        }),
    }
}

fn rust_compression_to_ffi(
    compression: RustCompressionAlgorithm,
) -> Result<ffi::CompressionAlgorithm, RustIggyError> {
    match compression {
        RustCompressionAlgorithm::None => Ok(ffi::CompressionAlgorithm::None),
        RustCompressionAlgorithm::Gzip => Ok(ffi::CompressionAlgorithm::Gzip),
    }
}

fn rust_topic_to_ffi(topic: &RustTopic) -> Result<ffi::Topic, RustIggyError> {
    Ok(ffi::Topic {
        id: topic.id,
        created_at: rust_timestamp_to_ffi(topic.created_at)?,
        name: topic.name.clone(),
        size: rust_byte_size_to_ffi(topic.size)?,
        message_expiry: rust_expiry_to_ffi(topic.message_expiry)?,
        compression_algorithm: rust_compression_to_ffi(topic.compression_algorithm)?,
        max_topic_size: rust_max_topic_size_to_ffi(topic.max_topic_size)?,
        replication_factor: topic.replication_factor,
        messages_count: topic.messages_count,
        partitions_count: topic.partitions_count,
    })
}

pub(crate) fn rust_stream_details_to_ffi(
    details: &RustStreamDetails,
) -> Result<ffi::StreamDetails, RustIggyError> {
    let topics = details
        .topics
        .iter()
        .map(rust_topic_to_ffi)
        .collect::<Result<Vec<_>, RustIggyError>>()?;

    Ok(ffi::StreamDetails {
        id: details.id,
        created_at: rust_timestamp_to_ffi(details.created_at)?,
        name: details.name.clone(),
        size: rust_byte_size_to_ffi(details.size)?,
        messages_count: details.messages_count,
        topics_count: details.topics_count,
        topics,
    })
}

fn rust_partition_to_ffi(partition: &RustPartition) -> Result<ffi::Partition, RustIggyError> {
    Ok(ffi::Partition {
        id: partition.id,
        created_at: rust_timestamp_to_ffi(partition.created_at)?,
        segments_count: partition.segments_count,
        current_offset: partition.current_offset,
        size: rust_byte_size_to_ffi(partition.size)?,
        messages_count: partition.messages_count,
    })
}

pub(crate) fn rust_topic_details_to_ffi(
    details: &RustTopicDetails,
) -> Result<ffi::TopicDetails, RustIggyError> {
    let partitions = details
        .partitions
        .iter()
        .map(rust_partition_to_ffi)
        .collect::<Result<Vec<_>, RustIggyError>>()?;

    Ok(ffi::TopicDetails {
        id: details.id,
        created_at: rust_timestamp_to_ffi(details.created_at)?,
        name: details.name.clone(),
        size: rust_byte_size_to_ffi(details.size)?,
        message_expiry: rust_expiry_to_ffi(details.message_expiry)?,
        compression_algorithm: rust_compression_to_ffi(details.compression_algorithm)?,
        max_topic_size: rust_max_topic_size_to_ffi(details.max_topic_size)?,
        replication_factor: details.replication_factor,
        messages_count: details.messages_count,
        partitions_count: details.partitions_count,
        partitions,
    })
}

pub(crate) fn ffi_header_map_to_rust(
    map: ffi::HeaderMap,
) -> Result<HashMap<RustHeaderKey, RustHeaderValue>, RustIggyError> {
    let mut headers = HashMap::with_capacity(map.entries.len());
    for entry in map.entries {
        let key = RustHeaderKey::new(&entry.key.value)?;
        let kind = ffi_header_kind_to_rust(entry.value.kind)?;
        if entry.value.value.is_empty() || entry.value.value.len() > 255 {
            return Err(RustIggyError::InvalidFormat);
        }
        let value = RustHeaderValue {
            kind,
            value: Bytes::from(entry.value.value),
        };
        headers.insert(key, value);
    }

    Ok(headers)
}

pub(crate) fn rust_headers_to_ffi(
    headers: &HashMap<RustHeaderKey, RustHeaderValue>,
) -> Result<ffi::HeaderMap, RustIggyError> {
    let mut entries = Vec::with_capacity(headers.len());
    for (key, value) in headers {
        if value.value.is_empty() || value.value.len() > 255 {
            return Err(RustIggyError::InvalidFormat);
        }
        entries.push(ffi::HeaderEntry {
            key: ffi::HeaderKey {
                value: key.as_str().to_string(),
            },
            value: ffi::HeaderValue {
                kind: rust_header_kind_to_ffi(value.kind)?,
                value: value.value.to_vec(),
            },
        });
    }
    Ok(ffi::HeaderMap { entries })
}

pub(crate) fn ffi_message_to_rust(
    message: ffi::IggyMessage,
) -> Result<RustIggyMessage, RustIggyError> {
    let id = ffi_id_to_u128(&message.header.id)?;
    let payload_length =
        u32::try_from(message.payload.len()).map_err(|_| RustIggyError::InvalidFormat)?;

    let headers = ffi_header_map_to_rust(message.headers)?;
    let user_headers = if headers.is_empty() {
        None
    } else {
        Some(headers.to_bytes())
    };
    let user_headers_len = user_headers.as_ref().map(|h| h.len()).unwrap_or(0);
    if user_headers_len > MAX_USER_HEADERS_SIZE as usize {
        return Err(RustIggyError::InvalidFormat);
    }
    let user_headers_length =
        u32::try_from(user_headers_len).map_err(|_| RustIggyError::InvalidFormat)?;

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
        payload: Bytes::from(message.payload),
        user_headers,
    })
}

pub(crate) fn rust_message_to_ffi(
    message: &RustIggyMessage,
) -> Result<ffi::IggyMessage, RustIggyError> {
    let headers = match &message.user_headers {
        Some(bytes) => HashMap::<RustHeaderKey, RustHeaderValue>::from_bytes(bytes.clone())?,
        None => HashMap::new(),
    };
    let headers = rust_headers_to_ffi(&headers)?;
    let payload_length =
        u32::try_from(message.payload.len()).map_err(|_| RustIggyError::InvalidFormat)?;
    let user_headers_length = message.user_headers.as_ref().map(|h| h.len()).unwrap_or(0);
    let user_headers_length =
        u32::try_from(user_headers_length).map_err(|_| RustIggyError::InvalidFormat)?;

    Ok(ffi::IggyMessage {
        header: ffi::IggyMessageHeader {
            checksum: message.header.checksum,
            id: message.header.id.to_le_bytes().to_vec(),
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
