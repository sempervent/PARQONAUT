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
        )
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
        match (self, to) {
            (Pending, PlanningValidated) => true,
            (PlanningValidated, Running) => true,
            (Running, RepairComplete) => true,
            (RepairComplete, Verifying) => true,
            (Verifying, Succeeded) => true,
            (Pending | PlanningValidated, StaleSource) => true,
            (Running | RepairComplete | Verifying, FailedRecoverable) => true,
            (Running | RepairComplete | Verifying, FailedPermanent) => true,
            (Running | RepairComplete | Verifying, VerificationFailed) => true,
            (Pending | PlanningValidated | Running, Cancelled) => true,
            (PlanningValidated, Blocked) => true,
            (FailedRecoverable, Running) => true,
            _ => false,
        }
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
