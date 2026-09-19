//! Developer-only automation for PARQONAUT (not a product entrypoint).

use clap::{Parser, Subcommand};

mod docs_cli;
mod docs_openapi;
mod fixtures_object_storage;
mod fixtures_orchestration;
mod fixtures_repair;
mod fixtures_schema;
mod golden_plans;

#[derive(Parser, Debug)]
#[command(name = "xtask", about = "PARQONAUT repository developer tasks", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Fixture and golden-plan generators
    Fixtures {
        #[command(subcommand)]
        command: FixturesCommand,
    },
    /// Regenerate checked-in canonical repair-plan JSON under fixtures/plans/
    GoldenPlans,
    /// Documentation generators (CLI reference, OpenAPI golden)
    Docs {
        #[command(subcommand)]
        command: DocsCommand,
    },
}

#[derive(Subcommand, Debug)]
enum DocsCommand {
    /// Regenerate `docs/reference/cli.md` from `prqnt --help`
    Cli {
        #[arg(long, help = "Verify committed reference is current (no write)")]
        check: bool,
    },
    /// Regenerate or verify `fixtures/api/openapi-v1.json`
    Openapi {
        #[arg(long, help = "Verify golden OpenAPI is current (no write)")]
        check: bool,
    },
}

#[derive(Subcommand, Debug)]
enum FixturesCommand {
    Scan {
        #[arg(default_value = "fixtures/scan")]
        output: camino::Utf8PathBuf,
    },
    Repair {
        #[arg(default_value = "fixtures/repair")]
        output: camino::Utf8PathBuf,
    },
    Schema {
        #[arg(default_value = "fixtures/schema")]
        output: camino::Utf8PathBuf,
    },
    Orchestration {
        #[arg(default_value = "fixtures/orchestration/shipwreck")]
        output: camino::Utf8PathBuf,
    },
    ObjectStorage {
        #[arg(default_value = "fixtures/object-storage")]
        output: camino::Utf8PathBuf,
    },
    /// Run repair, schema, orchestration, and object-storage generators
    All,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::GoldenPlans => golden_plans::run(),
        Command::Docs { command } => match command {
            DocsCommand::Cli { check } => docs_cli::run(!check, check),
            DocsCommand::Openapi { check } => docs_openapi::run(!check, check),
        },
        Command::Fixtures { command } => match command {
            FixturesCommand::Scan { output } => {
                eprintln!("scan fixtures: use scripts/fixtures/build_scan_fixtures.py ({output})");
                Ok(())
            }
            FixturesCommand::Repair { output } => fixtures_repair::run(&output),
            FixturesCommand::Schema { output } => fixtures_schema::run(&output),
            FixturesCommand::Orchestration { output } => fixtures_orchestration::run(&output),
            FixturesCommand::ObjectStorage { output } => fixtures_object_storage::run(&output),
            FixturesCommand::All => {
                let root = camino::Utf8Path::new;
                fixtures_repair::run(root("fixtures/repair"))?;
                fixtures_schema::run(root("fixtures/schema"))?;
                fixtures_orchestration::run(root("fixtures/orchestration/shipwreck"))?;
                fixtures_object_storage::run(root("fixtures/object-storage/local"))?;
                Ok(())
            }
        },
    }
}
