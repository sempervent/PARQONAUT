#!/usr/bin/env bash
set -euo pipefail

# Verify a packaged PARQONAUT release archive in an isolated directory.
# Usage: scripts/release/verify-artifact.sh <archive-path> <expected-version-tag> <expected-pkg-version> <expected-target> <expected-git-sha>

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <archive-path> <expected-tag> <expected-pkg-version> <expected-target> <expected-git-sha>" >&2
  exit 2
fi

ARCHIVE_PATH="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
ARCHIVE_DIR="$(dirname "${ARCHIVE_PATH}")"
ARCHIVE_NAME="$(basename "${ARCHIVE_PATH}")"
EXPECTED_TAG="$2"
EXPECTED_PKG_VERSION="$3"
EXPECTED_TARGET="$4"
EXPECTED_SHA="$5"

if [ ! -f "${ARCHIVE_PATH}" ]; then
  echo "ERROR: archive not found: ${ARCHIVE_PATH}" >&2
  exit 1
fi
if [ ! -f "${ARCHIVE_PATH}.sha256" ]; then
  echo "ERROR: checksum file not found: ${ARCHIVE_PATH}.sha256" >&2
  exit 1
fi

echo "==> Verifying archive checksum for ${ARCHIVE_NAME}"
(
  cd "${ARCHIVE_DIR}"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "${ARCHIVE_NAME}.sha256"
  else
    shasum -a 256 -c "${ARCHIVE_NAME}.sha256"
  fi
)

CLEAN_DIR="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -KILL "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${CLEAN_DIR}"
}
trap cleanup EXIT

echo "==> Unpacking into isolated directory ${CLEAN_DIR}"
tar -C "${CLEAN_DIR}" -xzf "${ARCHIVE_PATH}"
UNPACKED_DIR="${CLEAN_DIR}/parqonaut-${EXPECTED_TAG}-${EXPECTED_TARGET}"
BIN="${UNPACKED_DIR}/prqnt"

if [ ! -x "${BIN}" ]; then
  echo "ERROR: expected executable not found at ${BIN}" >&2
  exit 1
fi

echo "==> Verifying BUILD_METADATA.json identity"
python3 - "${UNPACKED_DIR}/BUILD_METADATA.json" "${EXPECTED_TAG}" "${EXPECTED_PKG_VERSION}" "${EXPECTED_TARGET}" "${EXPECTED_SHA}" <<'PY'
import json, sys
path, exp_tag, exp_pkg, exp_target, exp_sha = sys.argv[1:6]
with open(path, "r", encoding="utf-8") as f:
    meta = json.load(f)
assert meta.get("version") == exp_tag, f"version mismatch: {meta.get('version')} != {exp_tag}"
assert meta.get("package_version") == exp_pkg, f"package_version mismatch: {meta.get('package_version')} != {exp_pkg}"
assert meta.get("target") == exp_target, f"target mismatch: {meta.get('target')} != {exp_target}"
assert meta.get("git_sha") == exp_sha, f"git_sha mismatch: {meta.get('git_sha')} != {exp_sha}"
assert meta.get("locked") is True, f"locked must be true, got {meta.get('locked')}"
print("BUILD_METADATA.json OK:", json.dumps(meta))
PY

echo "==> Inspecting binary architecture"
FILE_OUT="$(file "${BIN}")"
echo "file: ${FILE_OUT}"
case "${EXPECTED_TARGET}" in
  x86_64-unknown-linux-gnu)
    echo "${FILE_OUT}" | grep -q "ELF 64-bit"
    echo "${FILE_OUT}" | grep -qiE "x86-64|x86_64"
    if command -v readelf >/dev/null 2>&1; then
      readelf -h "${BIN}" | grep -qi "Advanced Micro Devices X86-64"
    fi
    ;;
  aarch64-unknown-linux-gnu)
    echo "${FILE_OUT}" | grep -q "ELF 64-bit"
    echo "${FILE_OUT}" | grep -qi "aarch64"
    if command -v readelf >/dev/null 2>&1; then
      readelf -h "${BIN}" | grep -qi "AArch64"
    fi
    ;;
  aarch64-apple-darwin)
    echo "${FILE_OUT}" | grep -q "Mach-O 64-bit"
    echo "${FILE_OUT}" | grep -qi "arm64"
    if command -v lipo >/dev/null 2>&1; then
      ARCHS="$(lipo -archs "${BIN}")"
      [ "${ARCHS}" = "arm64" ] || { echo "ERROR: unexpected lipo archs: ${ARCHS}" >&2; exit 1; }
    fi
    ;;
  x86_64-apple-darwin)
    echo "${FILE_OUT}" | grep -q "Mach-O 64-bit"
    echo "${FILE_OUT}" | grep -qi "x86_64"
    if command -v lipo >/dev/null 2>&1; then
      ARCHS="$(lipo -archs "${BIN}")"
      [ "${ARCHS}" = "x86_64" ] || { echo "ERROR: unexpected lipo archs: ${ARCHS}" >&2; exit 1; }
    fi
    ;;
  *)
    echo "ERROR: unsupported target ${EXPECTED_TARGET}" >&2
    exit 1
    ;;
esac

# Run all CLI/server checks from the isolated clean directory with repo/build env stripped.
cd "${CLEAN_DIR}"
export HOME="${CLEAN_DIR}/home"
mkdir -p "${HOME}" "${CLEAN_DIR}/state" "${CLEAN_DIR}/data"
unset PARQONAUT_PLUGIN_ROOTS PARQONAUT_PLUGIN_SDK_PATH PARQONAUT_PLUGIN_PYTHON DATABASE_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY || true

echo "==> Checking --version and --help"
ACTUAL_VERSION="$("${BIN}" --version)"
echo "prqnt --version -> ${ACTUAL_VERSION}"
if [ "${ACTUAL_VERSION}" != "prqnt ${EXPECTED_PKG_VERSION}" ]; then
  echo "ERROR: binary version '${ACTUAL_VERSION}' != 'prqnt ${EXPECTED_PKG_VERSION}'" >&2
  exit 1
fi
"${BIN}" --help >/dev/null

echo "==> Checking plugin list and tui --help"
"${BIN}" plugin list >/dev/null
"${BIN}" tui --help >/dev/null

echo "==> Generating and validating OpenAPI spec"
"${BIN}" serve --print-openapi > "${CLEAN_DIR}/openapi.json"
python3 - "${CLEAN_DIR}/openapi.json" <<'PY'
import json, sys
spec = json.load(open(sys.argv[1], "r", encoding="utf-8"))
paths = spec.get("paths", {})
for req in ("/api/v1/health/live", "/api/v1/health/ready", "/api/v1/scans", "/api/v1/whoami"):
    assert req in paths, f"missing OpenAPI path {req}"
print(f"OpenAPI OK ({len(paths)} paths)")
PY

echo "==> Converting CSV to Parquet and scanning/inspecting"
printf "id,region,val\n1,us,10\n2,eu,20\n3,ap,30\n" > "${CLEAN_DIR}/data/in.csv"
"${BIN}" convert "${CLEAN_DIR}/data/in.csv" -o "${CLEAN_DIR}/data/out.parquet"
"${BIN}" inspect "${CLEAN_DIR}/data/out.parquet" >/dev/null
"${BIN}" --json scan "${CLEAN_DIR}/data/out.parquet" > "${CLEAN_DIR}/scan.json"
python3 - "${CLEAN_DIR}/scan.json" <<'PY'
import json, sys
rep = json.load(open(sys.argv[1], "r", encoding="utf-8"))
assert rep["summary"]["files_scanned"] == 1, rep
assert rep["summary"]["inspected_assets"] == 1, rep
assert rep["summary"]["failed_inspection_assets"] == 0, rep
print("Scan & inspect OK (1 file scanned, 1 inspected)")
PY

echo "==> Starting HTTP server and verifying health, API, and /ui/ assets"
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
TOKEN="rc-smoke-bootstrap-token-1234567890"
PRQNT_BOOTSTRAP_ADMIN_TOKEN="${TOKEN}" "${BIN}" serve \
  --listen "127.0.0.1:${PORT}" \
  --state-dir "${CLEAN_DIR}/state" \
  > "${CLEAN_DIR}/server.log" 2>&1 &
SERVER_PID=$!

python3 - "${PORT}" "${TOKEN}" "${CLEAN_DIR}/server.log" <<'PY'
import json, sys, time, urllib.request, urllib.error

port, token, log_path = sys.argv[1], sys.argv[2], sys.argv[3]
base = f"http://127.0.0.1:{port}"

deadline = time.time() + 15.0
while True:
    try:
        with urllib.request.urlopen(f"{base}/api/v1/health/ready", timeout=2) as r:
            if r.status == 200:
                body = json.loads(r.read().decode())
                assert body.get("status") == "ready", body
                break
    except Exception as exc:
        if time.time() > deadline:
            try:
                print("--- server.log ---", file=sys.stderr)
                print(open(log_path, encoding="utf-8", errors="replace").read(), file=sys.stderr)
            except OSError:
                pass
            raise RuntimeError(f"server did not become ready in time: {exc}") from exc
        time.sleep(0.2)

with urllib.request.urlopen(f"{base}/api/v1/health/live", timeout=5) as r:
    assert r.status == 200
    assert json.loads(r.read().decode()).get("status") == "ok"

for path, expected_sub in (
    ("/ui/", "PARQONAUT"),
    ("/ui/app.js", "ParqonautApi"),
    ("/ui/styles.css", ":root"),
):
    with urllib.request.urlopen(f"{base}{path}", timeout=5) as r:
        assert r.status == 200, (path, r.status)
        text = r.read().decode("utf-8")
        assert expected_sub in text, f"{path} missing {expected_sub}"

# Verify unauthenticated API fails closed (401)
try:
    urllib.request.urlopen(f"{base}/api/v1/whoami", timeout=5)
    raise AssertionError("expected 401 on unauthenticated whoami")
except urllib.error.HTTPError as e:
    assert e.code == 401, f"expected 401, got {e.code}"

# Verify authenticated API endpoints
for path in ("/api/v1/whoami", "/api/v1/overview"):
    req = urllib.request.Request(f"{base}{path}", headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=5) as r:
        assert r.status == 200, (path, r.status)
        data = json.loads(r.read().decode("utf-8"))
        assert isinstance(data, dict), data

print("HTTP API & /ui/ checks OK")
PY

echo "==> Verifying graceful SIGTERM shutdown"
kill -TERM "${SERVER_PID}"
wait "${SERVER_PID}"
EXIT_CODE=$?
SERVER_PID=""
if [ "${EXIT_CODE}" -ne 0 ]; then
  echo "ERROR: prqnt serve exited with ${EXIT_CODE}" >&2
  cat "${CLEAN_DIR}/server.log" >&2
  exit 1
fi

echo "==> Clean-environment artifact verification PASSED for ${ARCHIVE_NAME}"
