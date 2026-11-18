//! Typed HTTP/SSE operator client for `/api/v1` (shared contracts, header auth only).

use std::time::Duration;

use futures_util::StreamExt;
use parqonaut_repair::RepairSafety;
use parqonaut_service::api_types::{
    HealthResponse, JobEventListQuery, JobEventListResponse, JobListQuery, OverviewResponse,
    PageQuery, PagedAssetsResponse, PagedFindingsResponse, PluginCatalogEntryView,
    PluginCatalogListResponse, RunListQuery, RunListResponse, RunSummaryView, ScanJobListResponse,
    ScanJobSubmissionResponse, ScanJobView, StartScanRequest, WhoAmIResponse,
};
use parqonaut_service::application_http::{
    BatchConfigBody, BatchPlanResponse, BatchRepairJobBody, BatchResumeBody, BatchStatusResponse,
    DiagnoseResponse, JobCancelResponse, LocationPolicyBody, PlanBody, PlanResponse, RepairJobBody,
    VerifyBody, VerifyResponse,
};
use parqonaut_types::{JobId, RunId, ScanReport};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Method, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use url::Url;
use uuid::Uuid;

use crate::error::OperatorClientError;
use crate::sse::{SseEventItem, SseParser};

#[derive(Debug, Clone)]
pub struct OperatorClient {
    base_url: Url,
    token: Option<String>,
    http: Client,
}

pub fn generate_idempotency_key(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

impl OperatorClient {
    pub fn new(base_url: &str, token: Option<String>) -> Result<Self, OperatorClientError> {
        let mut parsed = Url::parse(base_url.trim())
            .map_err(|e| OperatorClientError::InvalidInput(format!("invalid server URL: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(OperatorClientError::InvalidInput(
                "server URL scheme must be http or https".into(),
            ));
        }
        if !parsed.path().ends_with('/') {
            let p = format!("{}/", parsed.path().trim_end_matches('/'));
            parsed.set_path(&p);
        }
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| OperatorClientError::Connection(e.to_string()))?;
        Ok(Self {
            base_url: parsed,
            token: token.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
            http,
        })
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        self
    }

    fn endpoint(&self, rel: &str) -> Result<Url, OperatorClientError> {
        self.base_url
            .join(rel.trim_start_matches('/'))
            .map_err(|e| OperatorClientError::InvalidInput(e.to_string()))
    }

    async fn send_json<B: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: Method,
        rel: &str,
        body: Option<&B>,
        idempotency_key: Option<&str>,
    ) -> Result<R, OperatorClientError> {
        let url = self.endpoint(rel)?;
        let mut req = self.http.request(method, url);
        if let Some(tok) = &self.token {
            req = req.header(AUTHORIZATION, format!("Bearer {tok}"));
        }
        if let Some(key) = idempotency_key {
            req = req.header("Idempotency-Key", key);
        }
        if let Some(b) = body {
            req = req.header(CONTENT_TYPE, "application/json").json(b);
        }
        let resp = req.send().await.map_err(map_reqwest_err)?;
        decode_response(resp).await
    }

    async fn get_json<R: DeserializeOwned>(&self, rel: &str) -> Result<R, OperatorClientError> {
        self.send_json::<(), R>(Method::GET, rel, None, None).await
    }

    pub async fn health(&self) -> Result<HealthResponse, OperatorClientError> {
        self.get_json("api/v1/health").await
    }

    pub async fn health_ready(&self) -> Result<HealthResponse, OperatorClientError> {
        self.get_json("api/v1/health/ready").await
    }

    pub async fn whoami(&self) -> Result<WhoAmIResponse, OperatorClientError> {
        self.get_json("api/v1/whoami").await
    }

    pub async fn overview(&self) -> Result<OverviewResponse, OperatorClientError> {
        self.get_json("api/v1/overview").await
    }

    pub async fn list_plugins(&self) -> Result<PluginCatalogListResponse, OperatorClientError> {
        self.get_json("api/v1/plugins").await
    }

    pub async fn get_plugin(
        &self,
        name: &str,
    ) -> Result<PluginCatalogEntryView, OperatorClientError> {
        let encoded = urlencoding::encode(name);
        self.get_json(&format!("api/v1/plugins/{encoded}")).await
    }

    pub async fn list_jobs(
        &self,
        q: &JobListQuery,
    ) -> Result<ScanJobListResponse, OperatorClientError> {
        let mut url = self.endpoint("api/v1/jobs")?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("limit", &q.limit.to_string());
            qp.append_pair("offset", &q.offset.to_string());
            if let Some(st) = &q.status {
                qp.append_pair("status", st);
            }
            if let Some(kind) = &q.kind {
                qp.append_pair("kind", kind);
            }
        }
        self.get_raw_json(url).await
    }

    pub async fn get_job(&self, job_id: JobId) -> Result<ScanJobView, OperatorClientError> {
        self.get_json(&format!("api/v1/jobs/{}", job_id.0)).await
    }

    pub async fn get_job_events(
        &self,
        job_id: JobId,
        q: &JobEventListQuery,
    ) -> Result<JobEventListResponse, OperatorClientError> {
        let mut url = self.endpoint(&format!("api/v1/jobs/{}/events", job_id.0))?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("after_seq", &q.after_seq.to_string());
            qp.append_pair("limit", &q.limit.to_string());
            if let Some(k) = &q.kind {
                qp.append_pair("kind", k);
            }
            if let Some(s) = &q.stage {
                qp.append_pair("stage", s);
            }
        }
        self.get_raw_json(url).await
    }

    pub async fn get_job_batch_status(
        &self,
        job_id: JobId,
    ) -> Result<BatchStatusResponse, OperatorClientError> {
        self.get_json(&format!("api/v1/jobs/{}/batch", job_id.0)).await
    }

    pub async fn list_runs(
        &self,
        q: &RunListQuery,
    ) -> Result<RunListResponse, OperatorClientError> {
        let mut url = self.endpoint("api/v1/runs")?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("limit", &q.limit.to_string());
            qp.append_pair("offset", &q.offset.to_string());
        }
        self.get_raw_json(url).await
    }

    pub async fn get_run(&self, run_id: RunId) -> Result<RunSummaryView, OperatorClientError> {
        self.get_json(&format!("api/v1/runs/{}", run_id.0)).await
    }

    pub async fn get_run_report(&self, run_id: RunId) -> Result<ScanReport, OperatorClientError> {
        self.get_json(&format!("api/v1/runs/{}/report", run_id.0)).await
    }

    pub async fn list_assets(
        &self,
        run_id: RunId,
        q: &PageQuery,
    ) -> Result<PagedAssetsResponse, OperatorClientError> {
        let mut url = self.endpoint(&format!("api/v1/runs/{}/assets", run_id.0))?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("limit", &q.limit.to_string());
            qp.append_pair("offset", &q.offset.to_string());
            if let Some(st) = &q.inspection_status {
                qp.append_pair("inspection_status", st);
            }
        }
        self.get_raw_json(url).await
    }

    pub async fn list_findings(
        &self,
        run_id: RunId,
        q: &PageQuery,
    ) -> Result<PagedFindingsResponse, OperatorClientError> {
        let mut url = self.endpoint(&format!("api/v1/runs/{}/findings", run_id.0))?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("limit", &q.limit.to_string());
            qp.append_pair("offset", &q.offset.to_string());
            if let Some(sev) = &q.severity {
                qp.append_pair("severity", sev);
            }
            if let Some(code) = &q.code {
                qp.append_pair("code", code);
            }
        }
        self.get_raw_json(url).await
    }

    pub async fn submit_scan(
        &self,
        body: &StartScanRequest,
        idempotency_key: Option<&str>,
    ) -> Result<ScanJobSubmissionResponse, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/jobs/scans", Some(body), idempotency_key).await
    }

    pub async fn diagnose(
        &self,
        body: &LocationPolicyBody,
    ) -> Result<DiagnoseResponse, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/diagnose", Some(body), None).await
    }

    pub async fn generate_plan(
        &self,
        body: &PlanBody,
    ) -> Result<PlanResponse, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/plans", Some(body), None).await
    }

    pub async fn check(
        &self,
        body: &LocationPolicyBody,
    ) -> Result<serde_json::Value, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/checks", Some(body), None).await
    }

    pub async fn verify(&self, body: &VerifyBody) -> Result<VerifyResponse, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/verifications", Some(body), None).await
    }

    pub async fn submit_repair(
        &self,
        body: &RepairJobBody,
        idempotency_key: Option<&str>,
    ) -> Result<ScanJobSubmissionResponse, OperatorClientError> {
        // Defense-in-depth client invariant check (server also revalidates).
        for auth_id in &body.authorize {
            if let Some(op) = body.plan.operations.iter().find(|o| &o.operation_id == auth_id) {
                if matches!(op.safety, RepairSafety::Blocked | RepairSafety::Destructive) {
                    return Err(OperatorClientError::InvalidInput(format!(
                        "cannot authorize blocked operation {auth_id}"
                    )));
                }
            }
        }
        self.send_json(Method::POST, "api/v1/repairs", Some(body), idempotency_key).await
    }

    pub async fn cancel_job(
        &self,
        job_id: JobId,
    ) -> Result<JobCancelResponse, OperatorClientError> {
        self.send_json::<(), JobCancelResponse>(
            Method::POST,
            &format!("api/v1/jobs/{}/cancel", job_id.0),
            None,
            None,
        )
        .await
    }

    pub async fn batch_check(
        &self,
        body: &BatchConfigBody,
    ) -> Result<serde_json::Value, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/batches/check", Some(body), None).await
    }

    pub async fn batch_plan(
        &self,
        body: &BatchConfigBody,
    ) -> Result<BatchPlanResponse, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/batches/plans", Some(body), None).await
    }

    pub async fn submit_batch_repair(
        &self,
        body: &BatchRepairJobBody,
        idempotency_key: Option<&str>,
    ) -> Result<ScanJobSubmissionResponse, OperatorClientError> {
        self.send_json(Method::POST, "api/v1/batches/repairs", Some(body), idempotency_key).await
    }

    pub async fn get_batch_status(
        &self,
        run_dir: &str,
    ) -> Result<BatchStatusResponse, OperatorClientError> {
        let encoded = urlencoding::encode(run_dir);
        self.get_json(&format!("api/v1/batches/{encoded}")).await
    }

    pub async fn submit_batch_resume(
        &self,
        run_dir: &str,
        body: &BatchResumeBody,
        idempotency_key: Option<&str>,
    ) -> Result<ScanJobSubmissionResponse, OperatorClientError> {
        let encoded = urlencoding::encode(run_dir);
        self.send_json(
            Method::POST,
            &format!("api/v1/batches/{encoded}/resume"),
            Some(body),
            idempotency_key,
        )
        .await
    }

    pub async fn batch_verify(
        &self,
        run_dir: &str,
    ) -> Result<serde_json::Value, OperatorClientError> {
        let encoded = urlencoding::encode(run_dir);
        self.send_json::<(), serde_json::Value>(
            Method::POST,
            &format!("api/v1/batches/{encoded}/verify"),
            None,
            None,
        )
        .await
    }

    /// Streams ordered job events using authenticated HTTP (`Authorization: Bearer` header).
    pub async fn stream_job_events(
        &self,
        job_id: JobId,
        after_seq: i64,
    ) -> Result<mpsc::Receiver<Result<SseEventItem, OperatorClientError>>, OperatorClientError>
    {
        let mut url = self.endpoint(&format!("api/v1/jobs/{}/events/stream", job_id.0))?;
        url.query_pairs_mut().append_pair("after_seq", &after_seq.to_string());
        let stream_client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| OperatorClientError::Connection(e.to_string()))?;
        let mut req = stream_client.get(url).header("Last-Event-ID", after_seq.to_string());
        if let Some(tok) = &self.token {
            req = req.header(AUTHORIZATION, format!("Bearer {tok}"));
        }
        let resp = req.send().await.map_err(map_reqwest_err)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(decode_error_status(resp).await);
        }

        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let mut stream = resp.bytes_stream();
            let mut parser = SseParser::new();
            while let Some(chunk_res) = stream.next().await {
                match chunk_res {
                    Ok(bytes) => match parser.push_bytes(&bytes) {
                        Ok(items) => {
                            for item in items {
                                if tx.send(Ok(item)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    },
                    Err(e) => {
                        let _ = tx
                            .send(Err(OperatorClientError::Transient(format!(
                                "SSE stream disconnected: {e}"
                            ))))
                            .await;
                        return;
                    }
                }
            }
        });
        Ok(rx)
    }

    async fn get_raw_json<R: DeserializeOwned>(&self, url: Url) -> Result<R, OperatorClientError> {
        let mut req = self.http.get(url);
        if let Some(tok) = &self.token {
            req = req.header(AUTHORIZATION, format!("Bearer {tok}"));
        }
        let resp = req.send().await.map_err(map_reqwest_err)?;
        decode_response(resp).await
    }
}

fn map_reqwest_err(err: reqwest::Error) -> OperatorClientError {
    if err.is_timeout() {
        OperatorClientError::Transient(format!("request timed out: {err}"))
    } else if err.is_connect() {
        OperatorClientError::Connection(format!("failed to connect to server: {err}"))
    } else {
        OperatorClientError::Connection(err.to_string())
    }
}

async fn decode_response<R: DeserializeOwned>(resp: Response) -> Result<R, OperatorClientError> {
    let status = resp.status();
    if status.is_success() {
        return resp
            .json::<R>()
            .await
            .map_err(|e| OperatorClientError::InvalidResponse(e.to_string()));
    }
    Err(decode_error_status(resp).await)
}

async fn decode_error_status(resp: Response) -> OperatorClientError {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
        if let Some(err) = v.get("error") {
            let code = err.get("code").and_then(|c| c.as_str()).unwrap_or("unknown").to_string();
            let message = err.get("message").and_then(|m| m.as_str()).unwrap_or(&text).to_string();
            let details = err.get("details").cloned().unwrap_or_else(|| serde_json::json!({}));
            return match status {
                StatusCode::UNAUTHORIZED => OperatorClientError::Unauthorized(message),
                StatusCode::FORBIDDEN => OperatorClientError::Forbidden(message),
                StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
                | StatusCode::REQUEST_TIMEOUT
                | StatusCode::TOO_MANY_REQUESTS => OperatorClientError::Transient(message),
                _ => OperatorClientError::Operation {
                    status: status.as_u16(),
                    code,
                    message,
                    details,
                },
            };
        }
    }
    match status {
        StatusCode::UNAUTHORIZED => OperatorClientError::Unauthorized(text),
        StatusCode::FORBIDDEN => OperatorClientError::Forbidden(text),
        StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT
        | StatusCode::REQUEST_TIMEOUT
        | StatusCode::TOO_MANY_REQUESTS => OperatorClientError::Transient(text),
        _ => OperatorClientError::Operation {
            status: status.as_u16(),
            code: "http_error".into(),
            message: text,
            details: serde_json::json!({}),
        },
    }
}
