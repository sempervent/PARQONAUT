//! Application service layer and HTTP API for PARQONAUT (HTTP API).
//!
//! HTTP handlers must not call [`parqonaut_core::ScanEngine`] or [`parqonaut_store::StoreBackend`]
//! directly — use [`ParqonautService`].

#![forbid(unsafe_code)]

pub mod api_types;
mod application_http;
pub mod error;
pub mod http;
pub mod observability;
pub mod openapi;
pub mod service;
mod worker;

pub use error::{AppError, ErrorBody, ErrorCode, ErrorEnvelope};
pub use http::{build_router, build_router_with_workers};
pub use observability::metrics_handle;
pub use openapi::openapi_spec;
pub use service::ParqonautService;

pub mod scan_job;
pub mod server;
pub use server::serve;
