use serde::{Deserialize, Serialize};

/// Backend-advertised features used to gate conditional operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCapabilities {
    pub range_reads: bool,
    pub stream_reads: bool,
    pub stream_writes: bool,
    pub multipart_upload: bool,
    pub conditional_create: bool,
    pub conditional_replace: bool,
    pub server_side_copy: bool,
    pub version_ids: bool,
}

impl StorageCapabilities {
    pub const LOCAL: Self = Self {
        range_reads: true,
        stream_reads: true,
        stream_writes: true,
        multipart_upload: false,
        conditional_create: true,
        conditional_replace: true,
        server_side_copy: false,
        version_ids: false,
    };

    pub const S3: Self = Self {
        range_reads: true,
        stream_reads: true,
        stream_writes: true,
        multipart_upload: true,
        conditional_create: true,
        conditional_replace: true,
        server_side_copy: true,
        version_ids: true,
    };

    pub const NONE: Self = Self {
        range_reads: false,
        stream_reads: false,
        stream_writes: false,
        multipart_upload: false,
        conditional_create: false,
        conditional_replace: false,
        server_side_copy: false,
        version_ids: false,
    };
}
