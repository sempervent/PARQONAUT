//! Shared PARQONAUT HTTP/SSE operator client and console state model.

#![forbid(unsafe_code)]

pub mod client;
pub mod error;
pub mod sse;
pub mod state;

pub use client::{generate_idempotency_key, OperatorClient};
pub use error::OperatorClientError;
pub use sse::{SseEventItem, SseParser};
pub use state::{
    ConnectionState, ConsoleModel, ConsoleTab, PermittedActions, RepairReviewState,
    WhoAmIResponseSnapshot,
};
