use serde::{Deserialize, Serialize};

use super::stages::PipelineStage;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub stages: Vec<PipelineStage>,
}

impl ExecutionPlan {
    pub fn push(&mut self, stage: PipelineStage) {
        self.stages.push(stage);
    }
}
