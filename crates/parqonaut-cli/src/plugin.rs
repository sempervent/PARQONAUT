use std::path::PathBuf;

use parqonaut_app::plugins::default_plugin_roots;
use parqonaut_plugin_host::{
    CatalogEntry, PluginCatalog, PluginCompatibility, HOST_PARQONAUT_VERSION,
};
use serde::Serialize;

#[derive(Serialize)]
struct PluginListItem<'a> {
    name: &'a str,
    version: &'a str,
    protocol_version: u32,
    capabilities: &'a parqonaut_plugin_protocol::PluginCapabilities,
    digest: &'a str,
    compatibility: &'static str,
}

pub fn run_list(json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = PluginCatalog::discover_from_roots(&default_plugin_roots())?;
    if json {
        let items: Vec<PluginListItem<'_>> =
            catalog.list().map(|(name, e)| list_item(name, e)).collect();
        println!("{}", serde_json::to_string_pretty(&items)?);
    } else {
        for (name, entry) in catalog.list() {
            let item = list_item(name, entry);
            println!(
                "{} {} protocol={} digest={} compatibility={}",
                item.name, item.version, item.protocol_version, item.digest, item.compatibility
            );
        }
    }
    Ok(())
}

fn list_item<'a>(name: &'a str, entry: &'a CatalogEntry) -> PluginListItem<'a> {
    PluginListItem {
        name,
        version: &entry.manifest.version,
        protocol_version: entry.manifest.protocol_version,
        capabilities: &entry.manifest.capabilities,
        digest: &entry.digest,
        compatibility: compatibility_label(entry.compatibility),
    }
}

fn compatibility_label(c: PluginCompatibility) -> &'static str {
    match c {
        PluginCompatibility::Compatible => "compatible",
        PluginCompatibility::HostVersionMismatch => "host_version_mismatch",
    }
}

pub fn run_inspect(name: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = PluginCatalog::discover_from_roots(&default_plugin_roots())?;
    let entry = catalog.get(name).ok_or_else(|| format!("plugin not found: {name}"))?;
    if json {
        #[derive(Serialize)]
        struct Out<'a> {
            manifest: &'a parqonaut_plugin_protocol::PluginManifest,
            digest: &'a str,
            capabilities: &'a parqonaut_plugin_protocol::PluginCapabilities,
            host_parqonaut_version: &'static str,
            compatibility: &'static str,
        }
        let out = Out {
            manifest: &entry.manifest,
            digest: &entry.digest,
            capabilities: &entry.manifest.capabilities,
            host_parqonaut_version: HOST_PARQONAUT_VERSION,
            compatibility: compatibility_label(entry.compatibility),
        };
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&entry.manifest)?);
        println!("digest: {}", entry.digest);
        println!("host: {HOST_PARQONAUT_VERSION} ({})", compatibility_label(entry.compatibility));
    }
    Ok(())
}

pub fn run_validate(path: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let entry = PluginCatalog::validate_path(&path)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "valid": true,
                "name": entry.manifest.name,
                "digest": entry.digest,
                "compatibility": compatibility_label(entry.compatibility),
            }))?
        );
    } else {
        println!("valid: {}", entry.manifest.name);
        println!("digest: {}", entry.digest);
    }
    Ok(())
}
