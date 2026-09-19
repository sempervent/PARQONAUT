use aws_sdk_s3::primitives::DateTime as SmithyDateTime;
use chrono::{DateTime, Utc};

use crate::location::ObjectLocation;
use crate::metadata::ObjectMetadata;

pub(crate) fn head_to_metadata(
    bucket: &str,
    key: &str,
    size: i64,
    etag: Option<String>,
    version_id: Option<String>,
    last_modified: Option<SmithyDateTime>,
) -> ObjectMetadata {
    ObjectMetadata {
        location: ObjectLocation::S3 { bucket: bucket.to_string(), key: key.to_string() },
        size: size.max(0) as u64,
        etag,
        version_id,
        last_modified: last_modified.and_then(smithy_to_chrono),
    }
}

pub(crate) fn list_object_to_metadata(
    bucket: &str,
    obj: &aws_sdk_s3::types::Object,
) -> ObjectMetadata {
    ObjectMetadata {
        location: ObjectLocation::S3 {
            bucket: bucket.to_string(),
            key: obj.key().unwrap_or_default().to_string(),
        },
        size: obj.size().unwrap_or(0).max(0) as u64,
        etag: obj.e_tag().map(str::to_string),
        version_id: None,
        last_modified: obj.last_modified().cloned().and_then(smithy_to_chrono),
    }
}

fn smithy_to_chrono(dt: SmithyDateTime) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(dt.secs(), dt.subsec_nanos()).map(|d| d.with_timezone(&Utc))
}
