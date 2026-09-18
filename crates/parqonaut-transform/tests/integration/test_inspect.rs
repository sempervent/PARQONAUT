use parqonaut_transform::error::Result;
use parqonaut_transform::io::resolve_inputs;
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

#[test]
fn test_resolve_inputs_glob() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base = temp_dir.path();
    
    // Create test files
    for i in 1..=3 {
        let path = base.join(format!("test_{}.parquet", i));
        File::create(&path)?;
    }
    
    let pattern = base.join("test_*.parquet").to_string_lossy().to_string();
    let resolved = resolve_inputs(&pattern)?;
    
    assert_eq!(resolved.len(), 3);
    // Should be sorted
    assert!(resolved[0] < resolved[1]);
    assert!(resolved[1] < resolved[2]);
    
    Ok(())
}
