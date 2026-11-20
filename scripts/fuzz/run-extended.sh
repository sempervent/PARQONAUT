#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FUZZ_DIR="$ROOT_DIR/fuzz"

PRIORITY_SECS="${PRIORITY_SECS:-90}"
STANDARD_SECS="${STANDARD_SECS:-45}"
RSS_LIMIT_MB="${RSS_LIMIT_MB:-2048}"
TIMEOUT_SECS="${TIMEOUT_SECS:-10}"
SEED="${PARQONAUT_FUZZ_SEED:-20261119}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/fuzz-extended}"

PRIORITY_TARGETS=(
  fuzz_parquet_metadata
  fuzz_arrow_ipc
  fuzz_plugin_protocol
  fuzz_dataset_uri
  fuzz_repair_plan
)
STANDARD_TARGETS=(
  fuzz_http_json
  fuzz_sse_stream
  fuzz_batch_config
  fuzz_schema_compat
)

mkdir -p "$OUT_DIR/logs" "$OUT_DIR/corpus" "$OUT_DIR/artifacts"

echo "=== PARQONAUT Extended libFuzzer Campaign ==="
echo "host_arch=$(uname -m) os=$(uname -s)"
echo "rustc=$(rustc --version)"
echo "cargo_fuzz=$(cargo fuzz --version 2>&1 || true)"
echo "seed=$SEED priority_secs=$PRIORITY_SECS standard_secs=$STANDARD_SECS rss_limit_mb=$RSS_LIMIT_MB timeout_secs=$TIMEOUT_SECS"

run_target() {
  local target="$1"
  local budget="$2"
  local tier="$3"
  local seed_dir="$FUZZ_DIR/corpus/$target"
  local work_corpus="$OUT_DIR/corpus/$target"
  local art_dir="$OUT_DIR/artifacts/$target"
  local log_file="$OUT_DIR/logs/$target.log"

  rm -rf "$work_corpus" "$art_dir"
  mkdir -p "$work_corpus" "$art_dir"
  if [[ -d "$seed_dir" ]]; then
    cp -R "$seed_dir/." "$work_corpus/"
  fi
  local initial_seeds
  initial_seeds=$(find "$work_corpus" -type f | wc -l | tr -d ' ')

  local target_triple="${FUZZ_TARGET_TRIPLE:-$(rustc -vV | awk '/^host:/ {print $2}')}"
  echo "--- Running $target ($tier, ${budget}s, initial_seeds=$initial_seeds, target=$target_triple) ---"
  local rc=0
  set +e
  (
    cd "$FUZZ_DIR"
    cargo fuzz run --target "$target_triple" "$target" "$work_corpus" -- \
      -seed="$SEED" \
      -max_total_time="$budget" \
      -rss_limit_mb="$RSS_LIMIT_MB" \
      -timeout="$TIMEOUT_SECS" \
      -print_final_stats=1 \
      -artifact_prefix="$art_dir/"
  ) >"$log_file" 2>&1
  rc=$?
  set -e

  local final_corpus
  final_corpus=$(find "$work_corpus" -type f | wc -l | tr -d ' ')
  local artifact_count
  artifact_count=$(find "$art_dir" -type f | wc -l | tr -d ' ')

  echo "target=$target tier=$tier rc=$rc initial_seeds=$initial_seeds final_corpus=$final_corpus artifacts=$artifact_count"
  tail -n 15 "$log_file" || true

  if [[ "$rc" -ne 0 || "$artifact_count" -ne 0 ]]; then
    echo "ERROR: fuzz target $target failed (rc=$rc, artifacts=$artifact_count)" >&2
    return 1
  fi
}

failures=0
for t in "${PRIORITY_TARGETS[@]}"; do
  run_target "$t" "$PRIORITY_SECS" "priority" || failures=$((failures + 1))
done
for t in "${STANDARD_TARGETS[@]}"; do
  run_target "$t" "$STANDARD_SECS" "standard" || failures=$((failures + 1))
done

python3 - "$OUT_DIR" "$SEED" "$PRIORITY_SECS" "$STANDARD_SECS" "$RSS_LIMIT_MB" "$TIMEOUT_SECS" <<'PY'
import json, pathlib, re, sys

out_dir = pathlib.Path(sys.argv[1])
seed, prio_s, std_s, rss_mb, timeout_s = sys.argv[2:]
logs_dir = out_dir / "logs"
corpus_dir = out_dir / "corpus"
art_dir = out_dir / "artifacts"

priority = {
    "fuzz_parquet_metadata",
    "fuzz_arrow_ipc",
    "fuzz_plugin_protocol",
    "fuzz_dataset_uri",
    "fuzz_repair_plan",
}
targets = [
    "fuzz_parquet_metadata",
    "fuzz_arrow_ipc",
    "fuzz_plugin_protocol",
    "fuzz_http_json",
    "fuzz_sse_stream",
    "fuzz_dataset_uri",
    "fuzz_repair_plan",
    "fuzz_batch_config",
    "fuzz_schema_compat",
]

rows = []
for t in targets:
    log_path = logs_dir / f"{t}.log"
    txt = log_path.read_text(errors="replace") if log_path.exists() else ""
    execs = None
    exec_s = None
    dur_s = None
    cov = None
    ft = None
    rss = None
    m = re.search(r"Done\s+(\d+)\s+runs\s+in\s+(\d+)\s+second", txt)
    if m:
        execs = int(m.group(1))
        dur_s = int(m.group(2))
    m_exec = re.search(r"stat::number_of_executed_units:\s+(\d+)", txt)
    if m_exec:
        execs = int(m_exec.group(1))
    m_rate = re.search(r"stat::average_exec_per_sec:\s+(\d+)", txt)
    if m_rate:
        exec_s = int(m_rate.group(1))
    m_rss = re.search(r"stat::peak_rss_mb:\s+(\d+)", txt)
    if m_rss:
        rss = int(m_rss.group(1))
    cov_matches = re.findall(r"cov:\s+(\d+)\s+ft:\s+(\d+)", txt)
    if cov_matches:
        cov = int(cov_matches[-1][0])
        ft = int(cov_matches[-1][1])
    corp_count = len(list((corpus_dir / t).glob("*"))) if (corpus_dir / t).exists() else 0
    art_count = len(list((art_dir / t).glob("*"))) if (art_dir / t).exists() else 0
    rows.append({
        "target": t,
        "tier": "priority" if t in priority else "standard",
        "budget_secs": int(prio_s) if t in priority else int(std_s),
        "actual_secs": dur_s,
        "execs": execs,
        "exec_per_sec": exec_s,
        "final_corpus_files": corp_count,
        "edge_cov": cov,
        "features": ft,
        "peak_rss_mb": rss,
        "artifacts": art_count,
        "status": "PASS" if (execs and art_count == 0) else "FAIL",
    })

summary = {
    "seed": int(seed),
    "rss_limit_mb": int(rss_mb),
    "timeout_secs": int(timeout_s),
    "targets": rows,
}
(out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
lines = [
    "| Target | Tier | Budget (s) | Actual (s) | Executions | Exec/s | Corpus Files | Edge Cov | Features | Peak RSS (MB) | Artifacts | Status |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
]
for r in rows:
    lines.append(
        f"| `{r['target']}` | {r['tier']} | {r['budget_secs']} | {r['actual_secs']} | {r['execs']} | {r['exec_per_sec']} | {r['final_corpus_files']} | {r['edge_cov']} | {r['features']} | {r['peak_rss_mb']} | {r['artifacts']} | {r['status']} |"
    )
(out_dir / "summary.md").write_text("\n".join(lines) + "\n")
print("\n".join(lines))
PY

exit "$failures"
