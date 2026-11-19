//! Deterministic smoke fuzz suite exercising all 9 untrusted input boundary targets in CI.

use bytes::Bytes;
use parqonaut_client::sse::SseParser;
use parqonaut_orchestrator::{BatchConfig, BatchPlan};
use parqonaut_plugin_host::decode_ipc_stream_bytes;
use parqonaut_plugin_protocol::{PluginManifest, PluginRequest, PluginResponse};
use parqonaut_repair::{
    canonical_plan_json, resolve_canonical_schema, ExecutionManifest, FileSchemaView, RepairPlan,
    SchemaPolicy,
};
use parqonaut_service::api_types::{AuthTokenCreateRequest, StartScanRequest};
use parqonaut_service::application_http::{
    BatchConfigBody, BatchRepairJobBody, LocationPolicyBody, PlanBody, RepairJobBody,
};
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parquet::file::reader::{FileReader, SerializedFileReader};

struct XorShift64(u64);
impl XorShift64 {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn gen_bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = (self.next_u64() as usize) % (max_len + 1);
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push((self.next_u64() & 0xff) as u8);
        }
        out
    }
    fn mutate(&mut self, seed: &[u8]) -> Vec<u8> {
        if seed.is_empty() {
            return self.gen_bytes(128);
        }
        let mut v = seed.to_vec();
        let ops = (self.next_u64() % 4) + 1;
        for _ in 0..ops {
            match self.next_u64() % 3 {
                0 if !v.is_empty() => {
                    let idx = (self.next_u64() as usize) % v.len();
                    v[idx] ^= ((self.next_u64() & 0xff) as u8).max(1);
                }
                1 if v.len() > 2 => {
                    let new_len = (self.next_u64() as usize) % v.len();
                    v.truncate(new_len);
                }
                _ => {
                    let idx = (self.next_u64() as usize) % (v.len() + 1);
                    v.insert(idx, (self.next_u64() & 0xff) as u8);
                }
            }
        }
        v
    }
}

fn fuzz_step(target: &str, data: &[u8]) {
    match target {
        "fuzz_parquet_metadata" => {
            if let Ok(r) = SerializedFileReader::new(Bytes::copy_from_slice(data)) {
                let m = r.metadata();
                let _ = m.file_metadata().num_rows();
                let _ = m.num_row_groups();
            }
        }
        "fuzz_arrow_ipc" => {
            let _ = decode_ipc_stream_bytes(data);
        }
        "fuzz_plugin_protocol" => {
            if let Ok(m) = serde_json::from_slice::<PluginManifest>(data) {
                let _ = m.validate();
            }
            let _ = serde_json::from_slice::<PluginRequest>(data);
            let _ = serde_json::from_slice::<PluginResponse>(data);
        }
        "fuzz_http_json" => {
            let _ = serde_json::from_slice::<StartScanRequest>(data);
            let _ = serde_json::from_slice::<AuthTokenCreateRequest>(data);
            let _ = serde_json::from_slice::<LocationPolicyBody>(data);
            let _ = serde_json::from_slice::<PlanBody>(data);
            let _ = serde_json::from_slice::<RepairJobBody>(data);
            let _ = serde_json::from_slice::<BatchConfigBody>(data);
            let _ = serde_json::from_slice::<BatchRepairJobBody>(data);
        }
        "fuzz_sse_stream" => {
            let mut p = SseParser::new();
            let _ = p.push_bytes(data);
            let _ = p.finish();
        }
        "fuzz_dataset_uri" => {
            if let Ok(s) = std::str::from_utf8(data) {
                let _ = DatasetLocation::parse(s);
                let _ = ObjectLocation::parse(s);
            }
        }
        "fuzz_repair_plan" => {
            if let Ok(plan) = RepairPlan::from_json(data) {
                let _ = canonical_plan_json(&plan);
            }
            let _ = serde_json::from_slice::<ExecutionManifest>(data);
        }
        "fuzz_batch_config" => {
            if let Ok(s) = std::str::from_utf8(data) {
                let _ = BatchConfig::from_toml(s);
            }
            let _ = BatchPlan::from_json(data);
        }
        "fuzz_schema_compat" => {
            if let Ok(raw) = serde_json::from_slice::<
                Vec<(String, Vec<parqonaut_repair::FieldDescriptor>)>,
            >(data)
            {
                let views: Vec<FileSchemaView> =
                    raw.into_iter().map(|(path, fields)| FileSchemaView { path, fields }).collect();
                let _ = resolve_canonical_schema(&views, None, &SchemaPolicy::default());
            }
            if let Ok(pol) = serde_json::from_slice::<SchemaPolicy>(data) {
                let _ = resolve_canonical_schema(&[], None, &pol);
            }
        }
        other => panic!("unknown fuzz target {other}"),
    }
}

#[test]
fn smoke_fuzz_all_nine_targets_on_corpora_and_mutations() {
    let corpus_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus");
    let targets = [
        "fuzz_parquet_metadata",
        "fuzz_arrow_ipc",
        "fuzz_plugin_protocol",
        "fuzz_http_json",
        "fuzz_sse_stream",
        "fuzz_dataset_uri",
        "fuzz_repair_plan",
        "fuzz_batch_config",
        "fuzz_schema_compat",
    ];
    let mut rng = XorShift64(0x5eed_0012_cafe_babe);

    for target in targets {
        let dir = corpus_root.join(target);
        assert!(dir.is_dir(), "missing fuzz corpus directory {}", dir.display());
        let mut seeds = Vec::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let p = entry.unwrap().path();
            if p.is_file() {
                seeds.push(std::fs::read(p).unwrap());
            }
        }
        assert!(!seeds.is_empty(), "empty fuzz corpus for {target}");

        for seed in &seeds {
            fuzz_step(target, seed);
            for _ in 0..100 {
                let mutated = rng.mutate(seed);
                fuzz_step(target, &mutated);
            }
        }
        for _ in 0..200 {
            let random = rng.gen_bytes(512);
            fuzz_step(target, &random);
        }
    }
}
