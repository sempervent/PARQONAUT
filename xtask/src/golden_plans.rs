//! Regenerate checked-in canonical repair-plan golden fixtures.
//!
//! Usage: cargo run -p parqonaut-repair --bin generate-golden-plans

use camino::Utf8PathBuf;
use parqonaut_repair::{
    canonical_plan_json, generate_plan, scan_directory, EffectivePolicy, RepairPlan,
};
use std::fs;

fn workspace_root() -> Utf8PathBuf {
    Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn write_canonical(name: &str, plan: &RepairPlan) -> Result<(), Box<dyn std::error::Error>> {
    let out = workspace_root().join(format!("fixtures/plans/{name}.canonical.json"));
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&out, canonical_plan_json(plan)?)?;
    println!("wrote {}", out);
    Ok(())
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let root = workspace_root();

    // safe-only
    let small = root.join("fixtures/repair/small-files");
    if small.exists() {
        let policy = EffectivePolicy::default();
        let scan = scan_directory(&small)?;
        let plan = generate_plan(&small, &scan, &policy, None)?;
        write_canonical("safe-only", &plan)?;
    }

    // mixed (frankenlake v2)
    let franken = root.join("fixtures/schema/frankenlake-v2");
    let policy_toml = fs::read_to_string(root.join("fixtures/schema/policy.toml"))?;
    let policy = EffectivePolicy::from_toml(Some(&policy_toml))?;
    let scan = scan_directory(&franken)?;
    let mixed = generate_plan(&franken, &scan, &policy, None)?;
    write_canonical("mixed", &mixed)?;

    // review-required only: frankenlake plan filtered conceptually via schema-only subset
    // Use rename-map with only rename op (no other defects)
    let rename = root.join("fixtures/schema/rename-map");
    let rename_policy = fs::read_to_string(root.join("fixtures/schema/rename-policy.toml"))?;
    let rename_eff = EffectivePolicy::from_toml(Some(&rename_policy))?;
    let rename_scan = scan_directory(&rename)?;
    let rename_plan = generate_plan(&rename, &rename_scan, &rename_eff, None)?;
    write_canonical("rename", &rename_plan)?;
    write_canonical("review-required", &rename_plan)?;

    // unresolvable
    let explicit = root.join("fixtures/schema/explicit-target");
    let explicit_scan = scan_directory(&explicit)?;
    let unres = generate_plan(&explicit, &explicit_scan, &EffectivePolicy::default(), None)?;
    write_canonical("unresolvable", &unres)?;

    // stale fingerprint case metadata (plan + tampered digest)
    let mut stale = mixed.clone();
    stale.dataset_fingerprint.digest = "deadbeef".repeat(8);
    let stale_meta = serde_json::json!({
        "description": "Repair must reject when source fingerprint no longer matches plan",
        "expected_error": "dataset changed",
        "plan_digest_in_plan": stale.dataset_fingerprint.digest,
        "valid_digest": mixed.dataset_fingerprint.digest,
    });
    fs::write(
        root.join("fixtures/plans/stale-fingerprint.case.json"),
        serde_json::to_string_pretty(&stale_meta)?,
    )?;
    println!("wrote fixtures/plans/stale-fingerprint.case.json");

    // Keep frankenlake alias for backward compatibility
    write_canonical("frankenlake-v2", &mixed)?;

    Ok(())
}
