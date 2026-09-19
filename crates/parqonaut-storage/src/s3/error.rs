use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;

use crate::error::StorageError;

pub(crate) fn map_list_error(location: &str, err: SdkError<ListObjectsV2Error>) -> StorageError {
    match &err {
        SdkError::ServiceError(service) => {
            map_service_error(location, service.raw().status().as_u16(), &service.err().to_string())
        }
        SdkError::TimeoutError(_) | SdkError::DispatchFailure { .. } => {
            StorageError::Transient { message: err.to_string() }
        }
        _ => StorageError::Other { message: err.to_string() },
    }
}

pub(crate) fn map_head_error(location: &str, err: SdkError<HeadObjectError>) -> StorageError {
    match &err {
        SdkError::ServiceError(service) => {
            if matches!(service.err(), HeadObjectError::NotFound(_)) {
                return StorageError::NotFound { location: location.into() };
            }
            map_service_error(location, service.raw().status().as_u16(), &service.err().to_string())
        }
        SdkError::TimeoutError(_) | SdkError::DispatchFailure { .. } => {
            StorageError::Transient { message: err.to_string() }
        }
        _ => StorageError::Other { message: err.to_string() },
    }
}

pub(crate) fn map_get_error(location: &str, err: SdkError<GetObjectError>) -> StorageError {
    match &err {
        SdkError::ServiceError(service) => {
            if matches!(service.err(), GetObjectError::NoSuchKey(_)) {
                return StorageError::NotFound { location: location.into() };
            }
            map_service_error(location, service.raw().status().as_u16(), &service.err().to_string())
        }
        SdkError::TimeoutError(_) | SdkError::DispatchFailure { .. } => {
            StorageError::Transient { message: err.to_string() }
        }
        _ => StorageError::Other { message: err.to_string() },
    }
}

pub(crate) fn map_put_error(
    location: &str,
    err: SdkError<PutObjectError>,
    conditional_create: bool,
) -> StorageError {
    match &err {
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            let err_str = service.err().to_string();
            if conditional_create && status == 412 {
                return StorageError::Conflict {
                    message: format!("object already exists: {location}"),
                };
            }
            if status == 412 {
                return StorageError::PreconditionFailed { message: err_str };
            }
            map_service_error(location, status, &err_str)
        }
        SdkError::TimeoutError(_) | SdkError::DispatchFailure { .. } => {
            StorageError::Transient { message: err.to_string() }
        }
        _ => StorageError::Other { message: err.to_string() },
    }
}

pub(crate) fn map_delete_error(location: &str, err: SdkError<DeleteObjectError>) -> StorageError {
    match &err {
        SdkError::ServiceError(service) => {
            map_service_error(location, service.raw().status().as_u16(), &service.err().to_string())
        }
        SdkError::TimeoutError(_) | SdkError::DispatchFailure { .. } => {
            StorageError::Transient { message: err.to_string() }
        }
        _ => StorageError::Other { message: err.to_string() },
    }
}

pub(crate) fn map_multipart_create_error(
    location: &str,
    err: SdkError<CreateMultipartUploadError>,
) -> StorageError {
    match &err {
        SdkError::ServiceError(service) => {
            map_service_error(location, service.raw().status().as_u16(), &service.err().to_string())
        }
        SdkError::TimeoutError(_) | SdkError::DispatchFailure { .. } => {
            StorageError::Transient { message: err.to_string() }
        }
        _ => StorageError::Other { message: err.to_string() },
    }
}

pub(crate) fn map_sdk_error(location: &str, err: impl std::fmt::Display) -> StorageError {
    let msg = err.to_string();
    if msg.contains("Timeout") || msg.contains("timeout") {
        StorageError::Transient { message: msg }
    } else if msg.contains("NotFound") || msg.contains("NoSuchKey") {
        StorageError::NotFound { location: location.into() }
    } else {
        StorageError::Other { message: msg }
    }
}

fn map_service_error(location: &str, status: u16, err_str: &str) -> StorageError {
    match status {
        401 => StorageError::Authentication,
        403 if err_str.contains("AccessDenied") || err_str.contains("Forbidden") => {
            StorageError::PermissionDenied { location: location.into() }
        }
        403 => StorageError::Authentication,
        404 | 410 => StorageError::NotFound { location: location.into() },
        409 => StorageError::Conflict { message: err_str.to_string() },
        412 => StorageError::PreconditionFailed { message: err_str.to_string() },
        429 | 503 => StorageError::Unavailable { message: err_str.to_string() },
        500 | 502 | 504 => StorageError::Transient { message: err_str.to_string() },
        _ if err_str.contains("SlowDown") || err_str.contains("RequestTimeout") => {
            StorageError::Transient { message: err_str.to_string() }
        }
        _ if err_str.contains("NotFound") || err_str.contains("NoSuchKey") => {
            StorageError::NotFound { location: location.into() }
        }
        _ => StorageError::Other { message: err_str.to_string() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RetryClass;

    #[test]
    fn retry_class_for_transient() {
        let err = StorageError::Transient { message: "timeout".into() };
        assert_eq!(err.retry_class(), RetryClass::Retryable);
    }

    #[test]
    fn retry_class_for_not_found() {
        let err = StorageError::NotFound { location: "s3://b/k".into() };
        assert_eq!(err.retry_class(), RetryClass::NeverRetry);
    }
}
