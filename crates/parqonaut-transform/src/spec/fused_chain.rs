//! In-memory fused transform chain including digest-pinned batch plugins.

use arrow::record_batch::RecordBatch;
use parqonaut_plugin_host::{BatchPluginBridge, PluginHostError};

use crate::engine::pipeline_from_rewrite_ops;
use crate::engine::Pipeline;
use crate::error::{Result, TransformError};
use crate::spec::plan::{FusedOperation, FusedPlanSegment};
use crate::spec::plugin::{map_plugin_err, plugin_runtime_config, verify_pinned_plugin};
use crate::spec::plugin_run::TransformRunContext;

#[derive(Debug, Clone, Copy)]
enum ExecOp {
    Host(usize),
    Plugin(usize),
}

pub struct FusedTransformChain {
    exec_ops: Vec<ExecOp>,
    pipelines: Vec<Pipeline>,
    bridges: Vec<BatchPluginBridge>,
    plugin_stage_ids: Vec<usize>,
    run: TransformRunContext,
}

impl FusedTransformChain {
    pub fn from_segment(
        fused: &FusedPlanSegment,
        execution_id: &str,
        run: TransformRunContext,
    ) -> Result<Self> {
        let runtime = plugin_runtime_config();
        let policy = runtime.batch_policy.clone();
        let mut pipelines = Vec::new();
        let mut bridges = Vec::new();
        let mut plugin_stage_ids = Vec::new();
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
                    let stage = run.register_plugin(pinned.clone());
                    let bridge = BatchPluginBridge::start(
                        &entry,
                        pinned.config.clone(),
                        execution_id,
                        &runtime,
                        &pinned.digest,
                        policy.max_inflight_batches,
                        run.cancel_token(),
                    )
                    .map_err(map_plugin_err)?;
                    exec_ops.push(ExecOp::Plugin(bridges.len()));
                    plugin_stage_ids.push(stage);
                    bridges.push(bridge);
                    i += 1;
                }
                FusedOperation::Partition { .. } | FusedOperation::Merge { .. } => break,
            }
        }
        Ok(Self { exec_ops, pipelines, bridges, plugin_stage_ids, run })
    }

    pub fn apply(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut b = batch;
        for op in &self.exec_ops {
            match op {
                ExecOp::Host(idx) => b = self.pipelines[*idx].execute(b)?,
                ExecOp::Plugin(idx) => {
                    let in_rows = b.num_rows() as u64;
                    b = match self.bridges[*idx].transform(b) {
                        Ok(out) => out,
                        Err(e @ PluginHostError::Cancelled) => {
                            self.run.cancel_plugin(self.plugin_stage_ids[*idx]);
                            return Err(map_plugin_err(e));
                        }
                        Err(e) => {
                            self.run.fail_plugin(self.plugin_stage_ids[*idx], "plugin_error");
                            return Err(map_plugin_err(e));
                        }
                    };
                    self.run.on_plugin_batch(
                        self.plugin_stage_ids[*idx],
                        in_rows,
                        b.num_rows() as u64,
                    );
                }
            }
        }
        Ok(b)
    }

    pub fn finish(&mut self) -> Result<()> {
        for (idx, bridge) in self.bridges.iter_mut().enumerate() {
            self.run.record_bridge_metrics(bridge.metrics().snapshot());
            if bridge.finish().map_err(map_plugin_err).is_ok() {
                self.run.complete_plugin(self.plugin_stage_ids[idx]);
            }
        }
        self.bridges.clear();
        self.plugin_stage_ids.clear();
        Ok(())
    }

    pub fn run_context(&self) -> &TransformRunContext {
        &self.run
    }

    pub fn has_plugin(&self) -> bool {
        !self.bridges.is_empty()
    }

    pub fn bridge_metrics_snapshot(
        &self,
    ) -> Vec<parqonaut_plugin_host::BatchBridgeMetricsSnapshot> {
        self.bridges.iter().map(|b| b.metrics().snapshot()).collect()
    }

    pub fn cancel_plugins(&self) {
        for bridge in &self.bridges {
            bridge.cancel();
        }
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
        return Err(TransformError::SpecError("expected rewrite operation".into()));
    }
    Ok((pipeline_from_rewrite_ops(projection, filter.as_deref(), rename, cast)?, consumed))
}
