use crate::cli::opts::Commands;
use crate::engine::{
    merge_parquet_files, parse_filter, split_parquet_file, FilterTransform, Pipeline,
    ProjectionTransform,
};
use crate::error::{ParqknifeError, Result};
use crate::io::resolve_inputs;
use crate::output::{compression_from_str, ParquetWriter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::io::{BufWriter, Write};
use tracing::{info, warn};

pub async fn run_inspect(cmd: &Commands, input: Option<&str>) -> Result<()> {
    if let Commands::Inspect { stats } = cmd {
        let input_path = input.ok_or_else(|| {
            ParqknifeError::InvalidInput("Input path required for inspect".to_string())
        })?;

        let inputs = resolve_inputs(input_path)?;

        for path in inputs {
            println!("\n=== Inspecting: {} ===", path);
            inspect_file(&path, *stats)?;
        }
    }
    Ok(())
}

fn inspect_file(path: &str, detailed_stats: bool) -> Result<()> {
    let file = File::open(path)?;
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let schema = arrow_reader.schema();
    println!("\nSchema:");
    println!("{schema}");

    let parquet_metadata = arrow_reader.metadata();
    println!("\nMetadata:");
    if let Some(kv) = parquet_metadata.file_metadata().key_value_metadata() {
        for item in kv {
            let value = item.value.as_deref().unwrap_or("");
            println!("  {} = {}", item.key, value);
        }
    }

    let num_row_groups = parquet_metadata.num_row_groups();
    println!("\nRow Groups: {num_row_groups}");

    let mut total_rows = 0;
    for i in 0..num_row_groups {
        let row_group_meta = parquet_metadata.row_group(i);
        let num_rows = row_group_meta.num_rows();
        total_rows += num_rows;

        if detailed_stats {
            println!("\nRow Group {i}:");
            println!("  Rows: {num_rows}");
            println!("  Total byte size: {}", row_group_meta.total_byte_size());
            println!("  Compressed size: {}", row_group_meta.compressed_size());

            for (col_idx, col_chunk) in row_group_meta.columns().iter().enumerate() {
                if col_idx < schema.fields().len() {
                    let field = schema.field(col_idx);
                    println!("  Column {} ({:?}):", field.name(), field.data_type());
                    if let Some(stats) = col_chunk.statistics() {
                        println!("    Statistics: {stats:?}");
                    }
                }
            }
        }
    }

    println!("\nTotal Rows: {total_rows}");
    Ok(())
}

pub async fn run_rewrite(cmd: &Commands, input: Option<&str>, output: Option<&str>) -> Result<()> {
    if let Commands::Rewrite {
        compression,
        row_group_size_mb,
        projection,
        filter,
        rebuild_stats,
        ..
    } = cmd
    {
        let input_path = input.ok_or_else(|| {
            ParqknifeError::InvalidInput("Input path required for rewrite".to_string())
        })?;
        let output_path = output.ok_or_else(|| {
            ParqknifeError::InvalidInput("Output path required for rewrite".to_string())
        })?;

        let inputs = resolve_inputs(input_path)?;
        info!("Processing {} input file(s)", inputs.len());

        let mut pipeline = Pipeline::new();

        if let Some(proj_str) = projection {
            let columns: Vec<String> = proj_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !columns.is_empty() {
                pipeline.add_transform(ProjectionTransform::new(columns));
            }
        }

        if let Some(filter_str) = filter {
            let filter_expr = parse_filter(filter_str)?;
            pipeline.add_transform(FilterTransform::new(filter_expr));
        }

        for input_file in inputs {
            info!("Processing: {input_file}");
            rewrite_file(
                &input_file,
                output_path,
                compression.as_deref(),
                *row_group_size_mb,
                &pipeline,
                *rebuild_stats,
            )?;
        }

        info!("Rewrite complete");
    }
    Ok(())
}

fn rewrite_file(
    input_path: &str,
    output_path: &str,
    compression: Option<&str>,
    row_group_size_mb: Option<u64>,
    pipeline: &Pipeline,
    rebuild_stats: bool,
) -> Result<()> {
    let file = File::open(input_path)?;
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let schema = arrow_reader.schema().clone();
    let comp = compression.map(compression_from_str).transpose()?;

    let output_file = File::create(output_path)?;
    let writer = BufWriter::new(output_file);

    let mut parquet_writer =
        ParquetWriter::new(writer, schema.clone(), comp, row_group_size_mb, rebuild_stats)?;

    let batch_reader = arrow_reader.build()?;
    for batch_result in batch_reader {
        let batch = batch_result?;
        let transformed = pipeline.execute(batch)?;
        parquet_writer.write_batch(transformed)?;
    }

    parquet_writer.close()?;
    info!("Wrote: {output_path}");
    Ok(())
}

pub async fn run_partition(
    cmd: &Commands,
    input: Option<&str>,
    output: Option<&str>,
) -> Result<()> {
    if let Commands::Partition { partition_by } = cmd {
        let input_path = input.ok_or_else(|| {
            ParqknifeError::InvalidInput("Input path required for partition".to_string())
        })?;
        let output_path = output.ok_or_else(|| {
            ParqknifeError::InvalidInput("Output directory required for partition".to_string())
        })?;
        let cols: Vec<String> = partition_by
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let inputs = resolve_inputs(input_path)?;
        if inputs.len() != 1 {
            return Err(ParqknifeError::InvalidInput(
                "partition requires exactly one input Parquet file".into(),
            ));
        }
        crate::engine::partition_parquet_file(
            &inputs[0],
            std::path::Path::new(output_path),
            &cols,
            64,
            None,
            None,
        )?;
    }
    Ok(())
}

pub async fn run_merge(cmd: &Commands, input: Option<&str>, output: Option<&str>) -> Result<()> {
    if let Commands::Merge { row_group_size_mb } = cmd {
        let input_path = input.ok_or_else(|| {
            ParqknifeError::InvalidInput("Input path required for merge".to_string())
        })?;
        let output_path = output.ok_or_else(|| {
            ParqknifeError::InvalidInput("Output path required for merge".to_string())
        })?;
        let inputs = resolve_inputs(input_path)?;
        let out = std::path::Path::new(output_path);
        let (dir, base) = if out.extension().is_some() {
            (
                out.parent().unwrap_or_else(|| std::path::Path::new(".")),
                out.file_stem().and_then(|s| s.to_str()).unwrap_or("merged"),
            )
        } else {
            (out, "merged")
        };
        let target_bytes = 512 * 1024 * 1024;
        let exact = out.extension().is_some().then_some(out);
        merge_parquet_files(
            &inputs.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            dir,
            base,
            target_bytes,
            None,
            *row_group_size_mb,
            false,
            exact,
        )?;
    }
    Ok(())
}

pub async fn run_split(cmd: &Commands, input: Option<&str>, output: Option<&str>) -> Result<()> {
    if let Commands::Split { target_size_mb, target_row_groups: _ } = cmd {
        let input_path = input.ok_or_else(|| {
            ParqknifeError::InvalidInput("Input path required for split".to_string())
        })?;
        let output_path = output.ok_or_else(|| {
            ParqknifeError::InvalidInput("Output directory required for split".to_string())
        })?;
        let inputs = resolve_inputs(input_path)?;
        if inputs.len() != 1 {
            return Err(ParqknifeError::InvalidInput(
                "split requires exactly one input Parquet file".into(),
            ));
        }
        let mb = target_size_mb.unwrap_or(512);
        split_parquet_file(
            &inputs[0],
            std::path::Path::new(output_path),
            "part",
            mb * 1024 * 1024,
            None,
            None,
            false,
        )?;
    }
    Ok(())
}
