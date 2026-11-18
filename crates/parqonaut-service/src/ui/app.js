import { ApiError, ParqonautApi, generateIdempotencyKey } from "/ui/api.js";
import { subscribeJobEvents } from "/ui/sse.js";

const api = new ParqonautApi();

const state = {
  tab: "dashboard",
  principal: null,
  overview: null,
  jobs: [],
  selectedJobId: null,
  selectedJobEvents: [],
  selectedBatchStatus: null,
  runs: [],
  selectedRunId: null,
  runSummary: null,
  findings: [],
  assets: [],
  plugins: null,
  repairPlan: null,
  repairSource: "",
  repairOutput: "",
  authorizedIds: new Set(),
  stopSse: null,
  modalConfirm: null,
};

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "className") node.className = v;
    else if (k === "textContent") node.textContent = v;
    else if (k === "disabled") node.disabled = Boolean(v);
    else if (k === "checked") node.checked = Boolean(v);
    else if (k === "type") node.type = v;
    else node.setAttribute(k, String(v));
  }
  for (const ch of children) {
    if (typeof ch === "string") node.appendChild(document.createTextNode(ch));
    else if (ch) node.appendChild(ch);
  }
  return node;
}

function canMutate() {
  return state.principal && (state.principal.role === "operator" || state.principal.role === "admin");
}

function setAlert(msg, kind = "error") {
  const box = document.getElementById("global-alert");
  if (!msg) {
    box.className = "alert hidden";
    box.textContent = "";
    return;
  }
  box.className = kind === "ok" ? "alert alert-ok" : "alert alert-error";
  box.textContent = msg;
}

function handleError(err) {
  if (err instanceof ApiError && err.category === "unauthorized") {
    disconnect();
    const authErr = document.getElementById("auth-error");
    authErr.textContent = err.message;
    authErr.classList.remove("hidden");
    return;
  }
  setAlert(err.message || String(err), "error");
}

function disconnect() {
  if (state.stopSse) {
    state.stopSse();
    state.stopSse = null;
  }
  api.clearToken();
  state.principal = null;
  state.overview = null;
  state.jobs = [];
  state.selectedJobEvents = [];
  state.repairPlan = null;
  state.authorizedIds.clear();
  document.getElementById("console-shell").classList.add("hidden");
  document.getElementById("auth-screen").classList.remove("hidden");
  document.getElementById("disconnect-btn").classList.add("hidden");
  document.getElementById("role-badge").classList.add("hidden");
  document.getElementById("conn-badge").className = "badge badge-muted";
  document.getElementById("conn-badge").textContent = "Disconnected";
}

function openConfirmModal(title, bodyNodes, onConfirm) {
  document.getElementById("modal-title").textContent = title;
  const body = document.getElementById("modal-body");
  body.replaceChildren(...bodyNodes);
  state.modalConfirm = onConfirm;
  document.getElementById("confirm-modal").classList.remove("hidden");
}

function closeConfirmModal() {
  state.modalConfirm = null;
  document.getElementById("confirm-modal").classList.add("hidden");
}

function completionFraction(metrics) {
  if (!metrics) return null;
  if (metrics.datasets_total > 0) {
    return Math.min(1, (metrics.datasets_done || 0) / metrics.datasets_total);
  }
  if (metrics.files_total > 0) {
    return Math.min(1, (metrics.files_done || 0) / metrics.files_total);
  }
  return null;
}

function statusBadge(status) {
  const cls =
    status === "succeeded" ? "badge badge-ok" :
    status === "failed" ? "badge badge-err" :
    status === "running" || status === "queued" ? "badge badge-warn" :
    "badge badge-muted";
  return el("span", { className: cls, textContent: status });
}

function renderOverview() {
  const cards = document.getElementById("overview-cards");
  if (!state.overview) {
    cards.replaceChildren(el("p", { className: "muted", textContent: "Loading overview..." }));
    return;
  }
  const c = state.overview.job_counts || {};
  const specs = [
    ["Service", state.overview.status],
    ["Queued", String(c.queued || 0)],
    ["Running", String(c.running || 0)],
    ["Succeeded", String(c.succeeded || 0)],
    ["Failed", String(c.failed || 0)],
    ["Plugins", state.overview.plugins_enabled ? `${state.overview.allowed_plugins_count} allowed` : "disabled"],
  ];
  cards.replaceChildren(
    ...specs.map(([label, value]) =>
      el("div", { className: "metric-card" }, [
        el("div", { className: "metric-label", textContent: label }),
        el("div", { className: "metric-value", textContent: value }),
      ])
    )
  );

  const dj = document.getElementById("dashboard-jobs");
  dj.replaceChildren(buildJobsTable(state.overview.recent_jobs || []));
  const dr = document.getElementById("dashboard-runs");
  dr.replaceChildren(buildRunsTable(state.overview.recent_runs || []));
}

function buildJobsTable(jobs) {
  if (!jobs.length) {
    return el("p", { className: "muted", textContent: "No jobs found." });
  }
  const tbody = el("tbody", {}, jobs.map((j) => {
    const tr = el("tr", {
      className: `selectable${j.job_id === state.selectedJobId ? " selected" : ""}`,
    }, [
      el("td", { textContent: j.job_kind }),
      el("td", {}, [statusBadge(j.status)]),
      el("td", { textContent: j.normalized_target_key }),
      el("td", { textContent: String(j.attempt_count) }),
    ]);
    tr.addEventListener("click", () => selectJob(j.job_id));
    return tr;
  }));
  return el("table", {}, [
    el("thead", {}, [
      el("tr", {}, [
        el("th", { textContent: "Kind" }),
        el("th", { textContent: "Status" }),
        el("th", { textContent: "Target" }),
        el("th", { textContent: "Attempts" }),
      ]),
    ]),
    tbody,
  ]);
}

function renderJobs() {
  const query = document.getElementById("job-search").value.trim().toLowerCase();
  const filtered = state.jobs.filter((j) =>
    !query ||
    j.normalized_target_key.toLowerCase().includes(query) ||
    j.job_id.toLowerCase().includes(query) ||
    j.job_kind.toLowerCase().includes(query)
  );
  document.getElementById("jobs-table-wrap").replaceChildren(buildJobsTable(filtered));
  document.getElementById("new-scan-panel").classList.toggle("hidden", !canMutate());
  renderJobDetail();
}

function renderJobDetail() {
  const wrap = document.getElementById("job-detail-wrap");
  const job = state.jobs.find((j) => j.job_id === state.selectedJobId);
  if (!job) {
    wrap.replaceChildren(el("p", { className: "muted", textContent: "Select a job to inspect its event timeline." }));
    return;
  }

  const nodes = [
    el("h3", { textContent: `Job ${job.job_id}` }),
    el("div", { className: "row" }, [
      statusBadge(job.status),
      el("span", { className: "muted", textContent: `${job.job_kind} · ${job.normalized_target_key}` }),
    ]),
  ];

  const lastEv = state.selectedJobEvents[state.selectedJobEvents.length - 1];
  const frac = lastEv ? completionFraction(lastEv.metrics) : null;
  if (frac !== null) {
    const pct = Math.round(frac * 100);
    nodes.push(el("div", { className: "muted", textContent: `Progress: ${pct}%` }));
    const fill = el("div", { className: "progress-fill" });
    fill.style.width = `${pct}%`;
    nodes.push(el("div", { className: "progress-track" }, [fill]));
  } else if (job.status === "running" || job.status === "queued") {
    nodes.push(el("div", { className: "badge badge-warn", textContent: "Progress: indeterminate stage" }));
  }

  if (job.failure) {
    nodes.push(el("p", { className: "alert alert-error", textContent: `${job.failure.code}: ${job.failure.message}` }));
  }

  const actions = el("div", { className: "row" });
  if (canMutate() && (job.status === "queued" || job.status === "running")) {
    const cancelBtn = el("button", { type: "button", className: "btn btn-danger", textContent: "Cancel Job" });
    cancelBtn.addEventListener("click", () => {
      openConfirmModal(
        "Confirm Job Cancellation",
        [
          el("p", { textContent: `Cancel job ${job.job_id}?` }),
          el("p", { className: "muted", textContent: `Target: ${job.normalized_target_key}` }),
        ],
        async () => {
          try {
            const resp = await api.cancelJob(job.job_id);
            setAlert(resp.message, "ok");
            await refreshAll();
          } catch (e) {
            handleError(e);
          }
        }
      );
    });
    actions.appendChild(cancelBtn);
  }
  if (job.run_id) {
    const viewRunBtn = el("button", { type: "button", className: "btn btn-secondary", textContent: "Inspect Run & Findings" });
    viewRunBtn.addEventListener("click", async () => {
      state.selectedRunId = job.run_id;
      switchTab("runs");
      await loadRunDetails(job.run_id);
    });
    actions.appendChild(viewRunBtn);
  }
  nodes.push(actions);

  nodes.push(el("h4", { textContent: "Ordered Events" }));
  if (!state.selectedJobEvents.length) {
    nodes.push(el("p", { className: "muted", textContent: "No events recorded yet." }));
  } else {
    const rows = state.selectedJobEvents.map((e) =>
      el("tr", {}, [
        el("td", { textContent: String(e.seq) }),
        el("td", { textContent: e.stage }),
        el("td", { textContent: e.kind }),
        el("td", { textContent: e.message || "" }),
      ])
    );
    nodes.push(
      el("table", {}, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { textContent: "Seq" }),
            el("th", { textContent: "Stage" }),
            el("th", { textContent: "Kind" }),
            el("th", { textContent: "Message" }),
          ]),
        ]),
        el("tbody", {}, rows),
      ])
    );
  }
  wrap.replaceChildren(...nodes);
}

async function selectJob(jobId) {
  state.selectedJobId = jobId;
  state.selectedJobEvents = [];
  if (state.stopSse) {
    state.stopSse();
    state.stopSse = null;
  }
  switchTab("jobs");
  try {
    const page = await api.getJobEvents(jobId, 0, 200);
    state.selectedJobEvents = page.items || [];
    renderJobs();
    const lastSeq = state.selectedJobEvents.length
      ? state.selectedJobEvents[state.selectedJobEvents.length - 1].seq
      : 0;
    state.stopSse = subscribeJobEvents(api, jobId, lastSeq, {
      onEvent: (ev) => {
        if (!state.selectedJobEvents.some((x) => x.seq === ev.seq)) {
          state.selectedJobEvents.push(ev);
          state.selectedJobEvents.sort((a, b) => a.seq - b.seq);
          renderJobDetail();
        }
      },
      onTerminal: async () => {
        await refreshJobs();
        renderJobs();
      },
    });
  } catch (e) {
    handleError(e);
  }
}

function buildRunsTable(runs) {
  if (!runs.length) {
    return el("p", { className: "muted", textContent: "No persisted runs found." });
  }
  const tbody = el("tbody", {}, runs.map((r) => {
    const tr = el("tr", {
      className: `selectable${r.run_id === state.selectedRunId ? " selected" : ""}`,
    }, [
      el("td", { textContent: r.run_outcome }),
      el("td", { textContent: r.target_identity.normalized_key }),
      el("td", { textContent: r.completed_at }),
    ]);
    tr.addEventListener("click", async () => {
      state.selectedRunId = r.run_id;
      switchTab("runs");
      await loadRunDetails(r.run_id);
    });
    return tr;
  }));
  return el("table", {}, [
    el("thead", {}, [
      el("tr", {}, [
        el("th", { textContent: "Outcome" }),
        el("th", { textContent: "Target" }),
        el("th", { textContent: "Completed" }),
      ]),
    ]),
    tbody,
  ]);
}

async function loadRunDetails(runId) {
  try {
    const sev = document.getElementById("finding-severity-filter").value;
    const code = document.getElementById("finding-code-filter").value.trim();
    const [sum, fnd, ast] = await Promise.all([
      api.getRun(runId),
      api.listFindings(runId, { severity: sev, code, limit: 100, offset: 0 }),
      api.listAssets(runId, 100, 0),
    ]);
    state.runSummary = sum;
    state.findings = fnd.items || [];
    state.assets = ast.items || [];
    renderRuns();
  } catch (e) {
    handleError(e);
  }
}

function renderRuns() {
  document.getElementById("runs-list-wrap").replaceChildren(buildRunsTable(state.runs));
  const sumWrap = document.getElementById("run-summary-wrap");
  if (!state.runSummary) {
    sumWrap.replaceChildren(el("p", { className: "muted", textContent: "Select a run to inspect findings and assets." }));
  } else {
    const s = state.runSummary.summary;
    sumWrap.replaceChildren(
      el("div", { className: "subpanel" }, [
        el("div", { textContent: `Run: ${state.runSummary.run_id} (${state.runSummary.run_outcome})` }),
        el("div", { className: "muted", textContent: `Assets: ${s.discovered_assets} · Datasets: ${s.dataset_count} · Findings: ${s.findings_total}` }),
      ])
    );
  }

  const fWrap = document.getElementById("findings-table-wrap");
  if (!state.findings.length) {
    fWrap.replaceChildren(el("p", { className: "muted", textContent: "No findings for this run/filter." }));
  } else {
    fWrap.replaceChildren(
      el("table", {}, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { textContent: "Severity" }),
            el("th", { textContent: "Code" }),
            el("th", { textContent: "Asset" }),
          ]),
        ]),
        el("tbody", {}, state.findings.map((f) =>
          el("tr", {}, [
            el("td", { textContent: f.severity }),
            el("td", {}, [el("code", { textContent: f.code })]),
            el("td", { textContent: f.asset_path || "-" }),
          ])
        )),
      ])
    );
  }

  const aWrap = document.getElementById("assets-table-wrap");
  if (!state.assets.length) {
    aWrap.replaceChildren(el("p", { className: "muted", textContent: "No assets loaded." }));
  } else {
    aWrap.replaceChildren(
      el("table", {}, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { textContent: "Path" }),
            el("th", { textContent: "Format" }),
            el("th", { textContent: "Status" }),
          ]),
        ]),
        el("tbody", {}, state.assets.map((a) =>
          el("tr", {}, [
            el("td", { textContent: a.path }),
            el("td", { textContent: a.format }),
            el("td", { textContent: a.inspection_status }),
          ])
        )),
      ])
    );
  }
}

function renderRepair() {
  const wrap = document.getElementById("repair-plan-wrap");
  const plan = state.repairPlan;
  if (!plan) {
    wrap.replaceChildren(el("p", { className: "muted", textContent: "Generate a plan above to review repair operations." }));
    return;
  }
  const header = el("div", { className: "subpanel" }, [
    el("div", { textContent: `Plan ID: ${plan.plan_id}` }),
    el("div", { className: "muted", textContent: `Source: ${state.repairSource} · Dataset FP: ${plan.dataset_fingerprint.digest}` }),
    el("div", { className: "muted", textContent: `Policy FP: ${plan.policy_fingerprint}` }),
  ]);

  const rows = (plan.operations || []).map((op) => {
    const authCell = el("td");
    if (op.safety === "safe") {
      authCell.appendChild(el("span", { className: "badge badge-ok", textContent: "auto (safe)" }));
    } else if (op.safety === "review_required") {
      const cb = el("input", {
        type: "checkbox",
        checked: state.authorizedIds.has(op.operation_id),
        disabled: !canMutate(),
        "aria-label": `Authorize ${op.operation_id}`,
      });
      cb.addEventListener("change", () => {
        if (cb.checked) state.authorizedIds.add(op.operation_id);
        else state.authorizedIds.delete(op.operation_id);
        renderRepair();
      });
      authCell.appendChild(cb);
    } else {
      authCell.appendChild(el("span", { className: "badge badge-err", textContent: "blocked" }));
    }
    return el("tr", {}, [
      authCell,
      el("td", {}, [el("code", { textContent: op.operation_id })]),
      el("td", { textContent: op.safety }),
      el("td", { textContent: (op.depends_on || []).join(", ") || "-" }),
      el("td", { textContent: op.rationale || "" }),
    ]);
  });

  const submitBtn = el("button", {
    type: "button",
    className: "btn btn-primary",
    textContent: "Review & Confirm Repair...",
    disabled: !canMutate(),
  });
  submitBtn.addEventListener("click", () => {
    const output = document.getElementById("repair-output").value.trim() || state.repairOutput;
    if (!output) {
      setAlert("Provide a separate repair output directory or S3 prefix before submitting.", "error");
      return;
    }
    const authList = Array.from(state.authorizedIds).sort();
    openConfirmModal(
      "Confirm Repair Execution",
      [
        el("p", { textContent: `Plan ID: ${plan.plan_id}` }),
        el("p", { textContent: `Source: ${state.repairSource}` }),
        el("p", { textContent: `Output: ${output}` }),
        el("p", { textContent: `Authorized ReviewRequired IDs: ${authList.length ? authList.join(", ") : "<none>"}` }),
      ],
      async () => {
        try {
          const sub = await api.submitRepair(
            { source: state.repairSource, output, plan, authorize: authList },
            generateIdempotencyKey("ui-repair")
          );
          setAlert(`Repair job queued: ${sub.job_id}`, "ok");
          await refreshAll();
          await selectJob(sub.job_id);
        } catch (e) {
          handleError(e);
        }
      }
    );
  });

  wrap.replaceChildren(
    header,
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { textContent: "Authorize" }),
          el("th", { textContent: "Op ID" }),
          el("th", { textContent: "Safety" }),
          el("th", { textContent: "Depends On" }),
          el("th", { textContent: "Rationale" }),
        ]),
      ]),
      el("tbody", {}, rows),
    ]),
    el("div", { className: "row", style: "margin-top: 0.75rem" }, [submitBtn])
  );
}

function renderBatches() {
  document.getElementById("batch-submit-form").classList.toggle("hidden", !canMutate());
  const batchJobs = state.jobs.filter((j) => j.job_kind === "batch_repair" || j.job_kind === "batch_resume");
  const wrap = document.getElementById("batch-jobs-wrap");
  if (!batchJobs.length) {
    wrap.replaceChildren(el("p", { className: "muted", textContent: "No batch jobs found." }));
  } else {
    const tbody = el("tbody", {}, batchJobs.map((j) => {
      const tr = el("tr", { className: "selectable" }, [
        el("td", {}, [statusBadge(j.status)]),
        el("td", { textContent: j.normalized_target_key }),
      ]);
      tr.addEventListener("click", async () => {
        try {
          const resp = await api.getJobBatch(j.job_id);
          state.selectedBatchStatus = resp.status;
          renderBatchDetail();
        } catch (e) {
          handleError(e);
        }
      });
      return tr;
    }));
    wrap.replaceChildren(
      el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", { textContent: "Status" }), el("th", { textContent: "Plan / Run" })])]),
        tbody,
      ])
    );
  }
  renderBatchDetail();
}

function renderBatchDetail() {
  const wrap = document.getElementById("batch-detail-wrap");
  const st = state.selectedBatchStatus;
  if (!st) {
    wrap.replaceChildren(el("p", { className: "muted", textContent: "Select a batch job to inspect per-dataset status." }));
    return;
  }
  const c = st.state_counts;
  const rows = (st.datasets || []).map((d) =>
    el("tr", {}, [
      el("td", { textContent: d.dataset_id }),
      el("td", { textContent: d.state }),
      el("td", { textContent: String(d.attempts) }),
      el("td", { textContent: d.error_message || "-" }),
    ])
  );
  wrap.replaceChildren(
    el("h3", { textContent: `${st.batch_name} (${st.run_id})` }),
    el("p", {
      className: "muted",
      textContent: `Succeeded=${c.succeeded} · Failed(Rec)=${c.failed_recoverable} · Failed(Perm)=${c.failed_permanent} · Blocked=${c.blocked} · Running=${c.running} · Cancelled=${c.cancelled}`,
    }),
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { textContent: "Dataset" }),
          el("th", { textContent: "State" }),
          el("th", { textContent: "Attempts" }),
          el("th", { textContent: "Error" }),
        ]),
      ]),
      el("tbody", {}, rows),
    ])
  );
}

function renderPlugins() {
  const wrap = document.getElementById("plugins-wrap");
  if (!state.plugins) {
    wrap.replaceChildren(el("p", { className: "muted", textContent: "Loading plugins..." }));
    return;
  }
  const status = el("p", {
    textContent: `Server plugin execution enabled: ${state.plugins.plugins_enabled ? "yes" : "no (disabled by default)"}`,
  });
  const items = state.plugins.plugins || [];
  if (!items.length) {
    wrap.replaceChildren(status, el("p", { className: "muted", textContent: "No allowlisted plugins available." }));
    return;
  }
  const rows = items.map((p) =>
    el("tr", {}, [
      el("td", { textContent: p.name }),
      el("td", { textContent: p.version }),
      el("td", { textContent: String(p.protocol_version) }),
      el("td", { textContent: p.compatible ? "compatible" : "incompatible" }),
      el("td", {}, [el("code", { textContent: p.digest.slice(0, 16) + "..." })]),
    ])
  );
  wrap.replaceChildren(
    status,
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { textContent: "Name" }),
          el("th", { textContent: "Version" }),
          el("th", { textContent: "Protocol" }),
          el("th", { textContent: "Compatibility" }),
          el("th", { textContent: "Digest" }),
        ]),
      ]),
      el("tbody", {}, rows),
    ])
  );
}

function switchTab(tab) {
  state.tab = tab;
  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === tab);
  });
  document.querySelectorAll(".tab-panel").forEach((p) => {
    p.classList.toggle("hidden", p.id !== `tab-${tab}`);
  });
}

async function refreshJobs() {
  const status = document.getElementById("job-status-filter").value;
  const kind = document.getElementById("job-kind-filter").value;
  const res = await api.listJobs({ status, kind, limit: 50, offset: 0 });
  state.jobs = res.items || [];
}

async function refreshAll() {
  try {
    const [ov, runs, plugins] = await Promise.all([
      api.overview(),
      api.listRuns(30, 0),
      api.listPlugins(),
      refreshJobs(),
    ]);
    state.overview = ov;
    state.runs = runs.items || [];
    state.plugins = plugins;
    renderOverview();
    renderJobs();
    renderRuns();
    renderRepair();
    renderBatches();
    renderPlugins();
  } catch (e) {
    handleError(e);
  }
}

document.getElementById("auth-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const tokenInput = document.getElementById("token-input");
  const authErr = document.getElementById("auth-error");
  authErr.classList.add("hidden");
  api.setToken(tokenInput.value);
  tokenInput.value = "";
  try {
    const who = await api.whoami();
    state.principal = who;
    document.getElementById("auth-screen").classList.add("hidden");
    document.getElementById("console-shell").classList.remove("hidden");
    document.getElementById("disconnect-btn").classList.remove("hidden");
    const roleBadge = document.getElementById("role-badge");
    roleBadge.textContent = `${who.label} (${who.role})`;
    roleBadge.className = "badge badge-ok";
    roleBadge.classList.remove("hidden");
    document.getElementById("conn-badge").className = "badge badge-ok";
    document.getElementById("conn-badge").textContent = "Connected";
    await refreshAll();
  } catch (err) {
    api.clearToken();
    authErr.textContent = err.message || "Authentication failed";
    authErr.classList.remove("hidden");
  }
});

document.getElementById("disconnect-btn").addEventListener("click", disconnect);
document.getElementById("theme-toggle").addEventListener("click", () => {
  const root = document.documentElement;
  const cur = root.getAttribute("data-theme");
  root.setAttribute("data-theme", cur === "dark" ? "light" : "dark");
});
document.getElementById("refresh-all-btn").addEventListener("click", refreshAll);

document.querySelectorAll(".tab-btn").forEach((btn) => {
  btn.addEventListener("click", () => switchTab(btn.dataset.tab));
});

document.getElementById("job-search").addEventListener("input", renderJobs);
document.getElementById("job-status-filter").addEventListener("change", () => refreshJobs().then(renderJobs).catch(handleError));
document.getElementById("job-kind-filter").addEventListener("change", () => refreshJobs().then(renderJobs).catch(handleError));
document.getElementById("finding-filter-btn").addEventListener("click", () => {
  if (state.selectedRunId) loadRunDetails(state.selectedRunId);
});

document.getElementById("scan-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const path = document.getElementById("scan-path").value.trim();
  const profile = document.getElementById("scan-profile").value;
  const pluginsRaw = document.getElementById("scan-plugins").value.trim();
  const plugins = pluginsRaw ? pluginsRaw.split(",").map((s) => s.trim()).filter(Boolean) : [];
  const target = path.endsWith(".parquet") || path.endsWith(".csv") || path.endsWith(".json")
    ? { type: "local_file", path }
    : { type: "local_directory", path };
  try {
    const sub = await api.submitScan({ target, profile, plugins }, generateIdempotencyKey("ui-scan"));
    setAlert(`Scan queued: ${sub.job_id}`, "ok");
    await refreshAll();
    await selectJob(sub.job_id);
  } catch (err) {
    handleError(err);
  }
});

document.getElementById("plan-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const location = document.getElementById("plan-location").value.trim();
  const output = document.getElementById("repair-output").value.trim();
  const policyToml = document.getElementById("plan-policy-toml").value.trim();
  try {
    const resp = await api.generatePlan({
      location,
      policy_toml: policyToml || null,
      target_schema_path: null,
    });
    state.repairPlan = resp.plan;
    state.repairSource = location;
    state.repairOutput = output;
    state.authorizedIds.clear();
    setAlert(`Generated plan ${resp.plan.plan_id}. Review operations before authorizing.`, "ok");
    renderRepair();
  } catch (err) {
    handleError(err);
  }
});

document.getElementById("batch-submit-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const planPath = document.getElementById("batch-plan-path").value.trim();
  const jobsRaw = document.getElementById("batch-jobs-num").value.trim();
  const jobs = jobsRaw ? Number(jobsRaw) : null;
  openConfirmModal(
    "Confirm Batch Repair",
    [el("p", { textContent: `Submit batch repair for plan: ${planPath}?` })],
    async () => {
      try {
        const sub = await api.submitBatchRepair(
          { plan_path: planPath, jobs, dry_run: false },
          generateIdempotencyKey("ui-batch")
        );
        setAlert(`Batch repair queued: ${sub.job_id}`, "ok");
        await refreshAll();
      } catch (err) {
        handleError(err);
      }
    }
  );
});

document.getElementById("modal-cancel-btn").addEventListener("click", closeConfirmModal);
document.getElementById("modal-confirm-btn").addEventListener("click", async () => {
  const fn = state.modalConfirm;
  closeConfirmModal();
  if (fn) await fn();
});
