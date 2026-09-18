//! Shared JSON body extractor: maps Axum [`Json`] rejections to [`crate::error::AppError`].

use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Json, Request};
use serde::de::DeserializeOwned;

use crate::error::AppError;

/// JSON body extractor that returns the standard [`crate::error::ErrorBody`] on parse or deserialize failure.
pub struct ApiJson<T>(pub T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned + Send + 'static,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(inner)) => Ok(ApiJson(inner)),
            Err(rejection) => Err(map_json_rejection(rejection)),
        }
    }
}

fn map_json_rejection(rejection: JsonRejection) -> AppError {
    AppError::InvalidJsonRequest { status: rejection.status(), message: rejection.body_text() }
}
