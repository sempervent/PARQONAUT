use crate::error::Result;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub trait Transform: Send + Sync {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch>;
    fn name(&self) -> &str;
}

pub struct Pipeline {
    transforms: Vec<Box<dyn Transform>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self { transforms: Vec::new() }
    }

    pub fn add_transform<T: Transform + 'static>(&mut self, transform: T) {
        self.transforms.push(Box::new(transform));
    }

    pub fn execute(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut current = batch;
        for transform in &self.transforms {
            current = transform.transform(current)?;
        }
        Ok(current)
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
