use criterion::{criterion_group, criterion_main, Criterion};
use std::fs;
use tempfile::tempdir;

fn create_test_csv_data(rows: usize) -> String {
    let mut data = String::from("id,name,value,score\n");
    for i in 0..rows {
        data.push_str(&format!("{},\"name_{}\",{:.2},{}\n", i, i, i as f64 * 1.5, i % 100));
    }
    data
}

fn benchmark_csv_processing(c: &mut Criterion) {
    let temp_dir = tempdir().unwrap();
    
    // Create test data
    let test_data = create_test_csv_data(100_000);
    let input_file = temp_dir.path().join("input.csv");
    let output_file = temp_dir.path().join("output.csv");
    
    fs::write(&input_file, &test_data).unwrap();
    
    c.bench_function("csv_processing", |b| {
        b.iter(|| {
            // This would run the actual maw command
            // For now, just measure file I/O
            let _data = fs::read_to_string(&input_file).unwrap();
            fs::write(&output_file, &test_data).unwrap();
        })
    });
}

criterion_group!(benches, benchmark_csv_processing);
criterion_main!(benches);
