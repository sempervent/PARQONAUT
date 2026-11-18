//! RAII terminal raw-mode & alternate-screen guard with panic restoration.

use std::io::{self, Stdout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossterm::cursor::{Hide, Show};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

pub trait TerminalOps: Send + Sync {
    fn enter_raw(&self) -> io::Result<()>;
    fn leave_raw(&self) -> io::Result<()>;
}

pub struct CrosstermTerminalOps;

impl TerminalOps for CrosstermTerminalOps {
    fn enter_raw(&self) -> io::Result<()> {
        enable_raw_mode()?;
        let mut out = io::stdout();
        execute!(out, EnterAlternateScreen, Hide)?;
        Ok(())
    }

    fn leave_raw(&self) -> io::Result<()> {
        let mut out = io::stdout();
        let _ = disable_raw_mode();
        let _ = execute!(out, LeaveAlternateScreen, Show);
        Ok(())
    }
}

/// Ensures terminal state is always restored on normal exit, error, or panic.
pub struct TerminalRestoreGuard {
    ops: Arc<dyn TerminalOps>,
    restored: Arc<AtomicBool>,
}

impl TerminalRestoreGuard {
    pub fn enter_with_ops(ops: Arc<dyn TerminalOps>) -> io::Result<Self> {
        ops.enter_raw()?;
        let restored = Arc::new(AtomicBool::new(false));
        let guard = Self { ops: Arc::clone(&ops), restored: Arc::clone(&restored) };

        let ops_panic = Arc::clone(&ops);
        let restored_panic = Arc::clone(&restored);
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !restored_panic.swap(true, Ordering::SeqCst) {
                let _ = ops_panic.leave_raw();
            }
            prev_hook(info);
        }));

        Ok(guard)
    }

    pub fn restore_now(&self) {
        if !self.restored.swap(true, Ordering::SeqCst) {
            let _ = self.ops.leave_raw();
        }
    }
}

impl Drop for TerminalRestoreGuard {
    fn drop(&mut self) {
        self.restore_now();
    }
}

pub fn enter_tui_terminal() -> io::Result<(Terminal<CrosstermBackend<Stdout>>, TerminalRestoreGuard)>
{
    let guard = TerminalRestoreGuard::enter_with_ops(Arc::new(CrosstermTerminalOps))?;
    let backend = CrosstermBackend::new(io::stdout());
    let terminal = Terminal::new(backend)?;
    Ok((terminal, guard))
}
