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

use anyhow::Result;
use iggy::prelude::{
    CompressionAlgorithm as RustCompressionAlgorithm, IggyByteSize, IggyDuration, IggyExpiry as RustIggyExpiry,
    MaxTopicSize as RustMaxTopicSize,
};

use crate::ffi;

pub fn compression_algorithm_from_ffi(
    algorithm: ffi::CompressionAlgorithm,
) -> Result<RustCompressionAlgorithm> {
    match algorithm {
        ffi::CompressionAlgorithm::None => Ok(RustCompressionAlgorithm::None),
        ffi::CompressionAlgorithm::Gzip => Ok(RustCompressionAlgorithm::Gzip),
        _ => Err(anyhow::anyhow!("unsupported compression algorithm value")),
    }
}

pub fn expiry_from_ffi(expiry: ffi::Expiry) -> Result<RustIggyExpiry> {
    match expiry.kind {
        ffi::ExpiryKind::ServerDefault => Ok(RustIggyExpiry::ServerDefault),
        ffi::ExpiryKind::NeverExpire => Ok(RustIggyExpiry::NeverExpire),
        ffi::ExpiryKind::ExpireDuration => {
            Ok(RustIggyExpiry::ExpireDuration(IggyDuration::from(expiry.value)))
        }
        _ => Err(anyhow::anyhow!("unsupported expiry kind value")),
    }
}

pub fn max_topic_size_from_ffi(size: ffi::MaxTopicSize) -> Result<RustMaxTopicSize> {
    match size.kind {
        ffi::MaxTopicSizeKind::ServerDefault => Ok(RustMaxTopicSize::ServerDefault),
        ffi::MaxTopicSizeKind::Unlimited => Ok(RustMaxTopicSize::Unlimited),
        ffi::MaxTopicSizeKind::Custom => {
            Ok(RustMaxTopicSize::Custom(IggyByteSize::from(size.value)))
        }
        _ => Err(anyhow::anyhow!("unsupported max topic size kind value")),
    }
}
