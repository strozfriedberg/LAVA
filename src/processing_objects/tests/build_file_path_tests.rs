use super::super::*;
use crate::basic_objects::{AlertOutputType, ExecutionSettings, TimeDirection};
use std::path::PathBuf;

#[test]
fn test_build_file_path_duplicate() {
    let settings = ExecutionSettings {
        output_dir: PathBuf::from("/tmp/output"),
        ..Default::default()
    };
    let processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let result = processor
        .build_file_path(&AlertOutputType::Duplicate)
        .unwrap();
    assert_eq!(
        result,
        PathBuf::from("/tmp/output/Duplicates/Test_DUPLICATES.csv")
    );
}

#[test]
fn test_build_file_path_duplicate_weird_path() {
    let settings = ExecutionSettings {
        output_dir: PathBuf::from("/tmp/\\output//"),
        ..Default::default()
    };
    let processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let result = processor
        .build_file_path(&AlertOutputType::Duplicate)
        .unwrap();
    assert_eq!(
        result,
        PathBuf::from("/tmp/output/Duplicates/Test_DUPLICATES.csv")
    );
}

#[test]
fn test_build_file_path_redaction() {
    let settings = ExecutionSettings {
        output_dir: PathBuf::from("/tmp/output"),
        ..Default::default()
    };
    let processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let result = processor
        .build_file_path(&AlertOutputType::Redaction)
        .unwrap();
    assert_eq!(
        result,
        PathBuf::from("/tmp/output/Redactions/Test_POSSIBLE_REDACTIONS.csv")
    );
}
