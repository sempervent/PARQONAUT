//! In-memory fused transform chain including digest-pinned batch plugins.

use arrow::record_batch::RecordBatch;
use parqonaut_plugin_host::BatchPluginBridge;

use crate::engine::pipeline_from_rewrite_ops;
use crate::engine::Pipeline;
use crate::error::{ParqknifeError, Result};
use crate::spec::plan::{FusedOperation, FusedPlanSegment};
use crate::spec::plugin::{map_plugin_err, plugin_runtime_config, verify_pinned_plugin};

#[derive(Debug, Clone, Copy)]
enum ExecOp {
    Host(usize),
    Plugin(usize),
}

pub struct FusedTransformChain {
    exec_ops: Vec<ExecOp>,
    pipelines: Vec<Pipeline>,
    bridges: Vec<BatchPluginBridge>,
}

impl FusedTransformChain {
    pub fn from_segment(fused: &FusedPlanSegment, execution_id: &str) -> Result<Self> {
        let runtime = plugin_runtime_config();
        let policy = runtime.batch_policy.clone();
        let mut pipelines = Vec::new();
        let mut bridges = Vec::new();
        let mut exec_ops = Vec::new();
        let mut i = 0;
        while i < fused.ops.len() {
            match &fused.ops[i] {
                FusedOperation::Rewrite { projection, filter, rename, cast, .. } => {
                    let (pipeline, consumed) = merge_rewrite_run(&fused.ops[i..])?;
                    exec_ops.push(ExecOp::Host(pipelines.len()));
                    pipelines.push(pipeline);
                    i += consumed;
                }
                FusedOperation::Plugin(pinned) => {
                    let entry = verify_pinned_plugin(pinned)?;
                    let bridge = BatchPluginBridge::start(
                        &entry,
                        pinned.config.clone(),
                        execution_id,
                        &runtime,
                        &pinned.digest,
                        policy.max_inflight_batches,
                    )
                    .map_err(map_plugin_err)?;
                    exec_ops.push(ExecOp::Plugin(bridges.len()));
                    bridges.push(bridge);
                    i += 1;
                }
                FusedOperation::Partition { .. } | FusedOperation::Merge { .. } => break,
            }
        }
        Ok(Self { exec_ops, pipelines, bridges })
    }

    pub fn apply(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut b = batch;
        for op in &self.exec_ops {
            match op {
                ExecOp::Host(idx) => b = self.pipelines[*idx].execute(b)?,
                ExecOp::Plugin(idx) => {
                    b = self.bridges[*idx].transform(b).map_err(map_plugin_err)?;
                }
            }
        }
        Ok(b)
    }

    pub fn finish(&mut self) -> Result<()> {
        while let Some(bridge) = self.bridges.pop() {
            bridge.finish().map_err(map_plugin_err)?;
        }
        Ok(())
    }

    pub fn has_plugin(&self) -> bool {
        !self.bridges.is_empty()
    }

    pub fn host_pipeline_only(ops: &[FusedOperation]) -> Result<Option<Pipeline>> {
        if ops.iter().any(|op| matches!(op, FusedOperation::Plugin(_))) {
            return Ok(None);
        }
        merge_rewrite_run(ops).map(|(p, _)| Some(p))
    }
}

fn merge_rewrite_run(ops: &[FusedOperation]) -> Result<(Pipeline, usize)> {
    let mut projection = None;
    let mut filter = None;
    let mut rename = None;
    let mut cast = None;
    let mut consumed = 0usize;
    for op in ops {
        let FusedOperation::Rewrite { projection: p, filter: f, rename: r, cast: c, .. } = op
        else {
            break;
        };
        if p.is_some() {
            projection = p.clone();
        }
        if f.is_some() {
            filter = f.clone();
        }
        if r.is_some() {
            rename = r.clone();
        }
        if c.is_some() {
            cast = c.clone();
        }
        consumed += 1;
    }
    if consumed == 0 {
        return Err(ParqknifeError::SpecError("expected rewrite operation".into()));
    }
    Ok((pipeline_from_rewrite_ops(projection, filter.as_deref(), rename, cast)?, consumed))
}
