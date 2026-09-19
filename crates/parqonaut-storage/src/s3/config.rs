use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;

/// Non-secret S3 client configuration. Credentials load via the standard AWS provider chain.
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    /// Custom endpoint URL (e.g. MinIO at `http://127.0.0.1:9000`).
    pub endpoint: Option<String>,
    /// AWS region. Defaults to `us-east-1` when unset (required for signature calculation).
    pub region: Option<String>,
    /// Force path-style addressing (`http://endpoint/bucket/key`).
    pub path_style: bool,
    /// Named profile from shared AWS config/credentials files.
    pub profile: Option<String>,
}

impl S3Config {
    /// Load non-secret client options from the environment (AWS + MinIO conventions).
    pub fn from_env() -> Self {
        let endpoint =
            std::env::var("MINIO_ENDPOINT").ok().or_else(|| std::env::var("AWS_ENDPOINT_URL").ok());
        let path_style = std::env::var("MINIO_PATH_STYLE")
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
            .unwrap_or_else(|_| endpoint.is_some());
        Self {
            endpoint,
            region: std::env::var("AWS_REGION")
                .ok()
                .or_else(|| std::env::var("MINIO_REGION").ok())
                .or(Some("us-east-1".into())),
            path_style,
            profile: std::env::var("AWS_PROFILE").ok(),
        }
    }

    pub fn minio(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: Some(endpoint.into()),
            region: Some("us-east-1".into()),
            path_style: true,
            profile: None,
        }
    }

    pub async fn build_client(&self) -> Client {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = &self.region {
            loader = loader.region(Region::new(region.clone()));
        }
        if let Some(profile) = &self.profile {
            loader = loader.profile_name(profile);
        }
        let sdk_config = loader.load().await;

        let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        if self.path_style {
            builder = builder.force_path_style(true);
        }
        Client::from_conf(builder.build())
    }
}
