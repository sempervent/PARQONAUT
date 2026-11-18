//! Interactive terminal operator console (`prqnt tui`).

#![forbid(unsafe_code)]

pub mod app;
pub mod runner;
pub mod terminal;
pub mod ui;

pub use app::{DialogMode, TuiAction, TuiApp};
pub use runner::{run_tui, TuiLaunchConfig};
pub use terminal::{enter_tui_terminal, TerminalOps, TerminalRestoreGuard};
