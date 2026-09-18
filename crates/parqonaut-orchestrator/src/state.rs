use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetState {
    Pending,
    PlanningValidated,
    Running,
    RepairComplete,
    Verifying,
    Succeeded,
    Blocked,
    FailedRecoverable,
    FailedPermanent,
    Cancelled,
    StaleSource,
    VerificationFailed,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StateTransitionError {
    #[error("illegal transition {from:?} → {to:?}")]
    Illegal { from: DatasetState, to: DatasetState },
}

impl DatasetState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Blocked
                | Self::FailedPermanent
                | Self::Cancelled
                | Self::VerificationFailed
                | Self::StaleSource
        )
    }

    pub fn is_runnable(self) -> bool {
        matches!(self, Self::Pending | Self::FailedRecoverable)
    }

    pub fn is_resumable(self) -> bool {
        matches!(
            self,
            Self::Pending
                | Self::PlanningValidated
                | Self::Running
                | Self::FailedRecoverable
                | Self::Cancelled
        )
    }

    /// Conservative recovery for datasets left `running` after a crash.
    pub fn recover_after_crash(self) -> Self {
        match self {
            Self::Running | Self::RepairComplete | Self::Verifying | Self::PlanningValidated => {
                Self::FailedRecoverable
            }
            other => other,
        }
    }

    pub fn transition(self, to: DatasetState) -> Result<DatasetState, StateTransitionError> {
        if self.can_transition_to(to) {
            Ok(to)
        } else {
            Err(StateTransitionError::Illegal { from: self, to })
        }
    }

    fn can_transition_to(self, to: DatasetState) -> bool {
        use DatasetState::*;
        if self == to {
            return true;
        }
        matches!(
            (self, to),
            (Pending, PlanningValidated)
                | (PlanningValidated, Running)
                | (Running, RepairComplete)
                | (RepairComplete, Verifying)
                | (Verifying, Succeeded)
                | (Pending | PlanningValidated, StaleSource)
                | (Running | RepairComplete | Verifying, FailedRecoverable)
                | (Running | RepairComplete | Verifying, FailedPermanent)
                | (Running | RepairComplete | Verifying, VerificationFailed)
                | (Pending | PlanningValidated | Running, Cancelled)
                | (PlanningValidated, Blocked)
                | (FailedRecoverable, Running)
                | (PlanningValidated, FailedRecoverable)
                | (Cancelled, FailedRecoverable)
                | (StaleSource, StaleSource)
        )
    }

    pub fn apply_transition(&mut self, to: DatasetState) -> Result<(), StateTransitionError> {
        let from = *self;
        *self = from.transition(to)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn succeeded_cannot_return_to_running() {
        let err = DatasetState::Succeeded.transition(DatasetState::Running).unwrap_err();
        assert_eq!(
            err,
            StateTransitionError::Illegal {
                from: DatasetState::Succeeded,
                to: DatasetState::Running,
            }
        );
    }

    #[test]
    fn recoverable_can_resume() {
        assert!(DatasetState::FailedRecoverable.transition(DatasetState::Running).is_ok());
    }
}
