import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const BASE_URL = process.env.PARQONAUT_UI_URL || "http://127.0.0.1:3858";
const TOKEN = process.env.PRQNT_BOOTSTRAP_ADMIN_TOKEN;
const FIXTURE_PATH = process.env.CONSOLE_SCAN_TARGET;
const REPAIR_OUT = process.env.CONSOLE_REPAIR_OUT;

assert.ok(TOKEN, "PRQNT_BOOTSTRAP_ADMIN_TOKEN must be set");
assert.ok(FIXTURE_PATH, "CONSOLE_SCAN_TARGET must be set");
assert.ok(REPAIR_OUT, "CONSOLE_REPAIR_OUT must be set");

function findChrome() {
  const candidates = [
    process.env.CHROME_BIN,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
  ].filter(Boolean);
  for (const p of candidates) {
    if (fs.existsSync(p)) return p;
  }
  return null;
}

const chromeBin = findChrome();
if (!chromeBin) {
  if (process.env.UI_BROWSER_REQUIRED === "1") {
    throw new Error("No Chromium/Chrome binary found and UI_BROWSER_REQUIRED=1");
  }
  console.log("ui-browser-smoke: SKIP (no headless Chrome/Chromium binary found)");
  process.exit(0);
}

const profileDir = fs.mkdtempSync(path.join(os.tmpdir(), "parqonaut-chrome-"));
const chrome = spawn(
  chromeBin,
  [
    "--headless=new",
    "--remote-debugging-port=0",
    `--user-data-dir=${profileDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--no-sandbox",
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "about:blank",
  ],
  { stdio: ["ignore", "pipe", "pipe"] }
);

function cleanup() {
  try {
    chrome.kill("SIGKILL");
  } catch {
    // ignore
  }
  try {
    fs.rmSync(profileDir, { recursive: true, force: true });
  } catch {
    // ignore
  }
}
process.on("exit", cleanup);

const browserWsUrl = await new Promise((resolve, reject) => {
  let stderr = "";
  const timer = setTimeout(() => reject(new Error(`Chrome start timeout: ${stderr}`)), 45000);
  chrome.stderr.on("data", (d) => {
    stderr += d.toString();
    const m = stderr.match(/DevTools listening on (ws:\/\/\S+)/);
    if (m) {
      clearTimeout(timer);
      resolve(m[1]);
    }
  });
  chrome.on("exit", (code) => {
    clearTimeout(timer);
    reject(new Error(`Chrome exited early (${code}): ${stderr}`));
  });
});

// Get the page target WS URL from /json/list.
const httpBase = browserWsUrl.replace(/^ws:/, "http:").replace(/\/devtools\/browser\/.*$/, "");
const targets = await (await fetch(`${httpBase}/json/list`)).json();
const pageTarget = targets.find((t) => t.type === "page") || targets[0];
assert.ok(pageTarget?.webSocketDebuggerUrl, "missing page target WS URL");

class CdpClient {
  constructor(url) {
    this.ws = new WebSocket(url);
    this.id = 0;
    this.pending = new Map();
    this.errors = [];
    this.ready = new Promise((res, rej) => {
      this.ws.addEventListener("open", res);
      this.ws.addEventListener("error", rej);
    });
    this.ws.addEventListener("message", (ev) => {
      const msg = JSON.parse(ev.data.toString());
      if (msg.id && this.pending.has(msg.id)) {
        const { resolve, reject } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.error) reject(new Error(msg.error.message));
        else resolve(msg.result);
      } else if (msg.method === "Runtime.exceptionThrown") {
        this.errors.push(JSON.stringify(msg.params.exceptionDetails));
      } else if (msg.method === "Runtime.consoleAPICalled" && msg.params.type === "error") {
        this.errors.push(msg.params.args.map((a) => a.value || a.description).join(" "));
      }
    });
  }

  async send(method, params = {}) {
    await this.ready;
    const id = ++this.id;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async evalJs(expr) {
    const res = await this.send("Runtime.evaluate", {
      expression: expr,
      returnByValue: true,
      awaitPromise: true,
    });
    if (res.exceptionDetails) {
      throw new Error(`JS exception: ${JSON.stringify(res.exceptionDetails)}`);
    }
    return res.result?.value;
  }

  async waitFor(fnExpr, timeoutMs = 15000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const val = await this.evalJs(`Boolean(${fnExpr})`);
      if (val) return;
      await new Promise((r) => setTimeout(r, 100));
    }
    const diag = await this.evalJs(`({
      ready: document.documentElement.dataset.uiReady || null,
      authErr: document.getElementById("auth-error")?.textContent || "",
      globalAlert: document.getElementById("global-alert")?.textContent || "",
      url: location.href
    })`).catch(() => ({}));
    throw new Error(`Timed out waiting for: ${fnExpr} (diag=${JSON.stringify(diag)}, errors=${JSON.stringify(this.errors)})`);
  }

  close() {
    this.ws.close();
  }
}

const cdp = new CdpClient(pageTarget.webSocketDebuggerUrl);
try {
  await cdp.send("Page.enable");
  await cdp.send("Runtime.enable");
  await cdp.send("Page.navigate", { url: `${BASE_URL}/ui/` });
  await cdp.waitFor('document.documentElement.dataset.uiReady === "1"');
  await cdp.waitFor('document.getElementById("auth-form") !== null');

  // Verify auth screen visible and console shell hidden before login.
  const preAuth = await cdp.evalJs(`({
    authHidden: document.getElementById("auth-screen").classList.contains("hidden"),
    shellHidden: document.getElementById("console-shell").classList.contains("hidden")
  })`);
  assert.equal(preAuth.authHidden, false);
  assert.equal(preAuth.shellHidden, true);

  // Connect with token.
  await cdp.evalJs(`(() => {
    document.getElementById("token-input").value = ${JSON.stringify(TOKEN)};
    document.getElementById("auth-form").requestSubmit();
  })()`);
  await cdp.waitFor('!document.getElementById("console-shell").classList.contains("hidden")');
  await cdp.waitFor('document.getElementById("role-badge").textContent.includes("admin")');

  // Verify token is NOT stored in localStorage or sessionStorage.
  const storageCounts = await cdp.evalJs("({ l: localStorage.length, s: sessionStorage.length })");
  assert.equal(storageCounts.l, 0);
  assert.equal(storageCounts.s, 0);

  // Submit scan from Jobs tab and watch live events render.
  await cdp.evalJs('document.querySelector(\'.tab-btn[data-tab="jobs"]\').click()');
  await cdp.evalJs(`(() => {
    document.getElementById("scan-path").value = ${JSON.stringify(FIXTURE_PATH)};
    document.getElementById("scan-form").requestSubmit();
  })()`);
  await cdp.waitFor('document.getElementById("job-detail-wrap").textContent.includes("execution_completed")');

  // Switch to Runs & Findings and verify runs list populated.
  await cdp.evalJs('document.querySelector(\'.tab-btn[data-tab="runs"]\').click()');
  await cdp.waitFor('document.getElementById("runs-list-wrap").textContent.includes("completed")');

  // Switch to Repair Workbench, generate a plan, and open confirmation modal.
  await cdp.evalJs('document.querySelector(\'.tab-btn[data-tab="repair"]\').click()');
  await cdp.evalJs(`(() => {
    document.getElementById("plan-location").value = ${JSON.stringify(FIXTURE_PATH)};
    document.getElementById("repair-output").value = ${JSON.stringify(REPAIR_OUT)};
    document.getElementById("plan-form").requestSubmit();
  })()`);
  await cdp.waitFor('document.getElementById("repair-plan-wrap").textContent.includes("Plan ID:")');
  await cdp.evalJs('document.querySelector("#repair-plan-wrap button").click()');
  await cdp.waitFor('!document.getElementById("confirm-modal").classList.contains("hidden")');
  await cdp.evalJs('document.getElementById("modal-confirm-btn").click()');
  await cdp.waitFor('document.getElementById("job-detail-wrap").textContent.includes("execution_completed")');

  // Verify Plugins tab renders cleanly.
  await cdp.evalJs('document.querySelector(\'.tab-btn[data-tab="plugins"]\').click()');
  await cdp.waitFor('document.getElementById("plugins-wrap").textContent.includes("Server plugin execution enabled")');

  assert.deepEqual(cdp.errors, [], `Browser errors observed: ${cdp.errors.join("; ")}`);
  console.log("ui-browser-smoke PASS (headless Chrome verified login, scan, SSE events, runs, repair modal, plugins)");
} finally {
  cdp.close();
  cleanup();
}
