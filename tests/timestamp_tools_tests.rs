#[cfg(test)]
use std::path::PathBuf;
use csv::StringRecord;
use log_checker::basic_objects::{ExecutionSettings, TimeDirection, AlertOutputType};
use log_checker::timestamp_tools::{LogRecordProcessor, TimeDirectionChecker};
use log_checker::helpers::{make_fake_record, dt};

#[test]
fn processes_ascending_records_correctly() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Ascending),&settings, "Test".to_string());

    processor
        .process_timestamp(&make_fake_record(0, "2024-05-01 12:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(1, "2024-05-01 13:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(2, "2024-05-01 15:00:00", StringRecord::from(vec!["test"])))
        .unwrap();

    let results = processor.get_statistics().unwrap();
    assert_eq!(
        results.min_timestamp.unwrap().to_string(),
        "2024-05-01 12:00:00"
    );
    assert_eq!(
        results.max_timestamp.unwrap().to_string(),
        "2024-05-01 15:00:00"
    );
    assert_eq!(processor.num_records, 3);
    assert_eq!(
        results.largest_gap_duration.unwrap(),
        "02:00:00".to_string()
    );
    assert_eq!(
        results.largest_gap.unwrap(),
        "2024-05-01 13:00:00 to 2024-05-01 15:00:00".to_string()
    );
}

#[test]
fn processes_ascending_records_same_time_gap_correctly() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Ascending), &settings, "Test".to_string());

    processor
        .process_timestamp(&make_fake_record(0, "2024-05-01 12:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(1, "2024-05-01 13:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(2, "2024-05-01 14:00:00", StringRecord::from(vec!["test"])))
        .unwrap();

    let results = processor.get_statistics().unwrap();
    assert_eq!(
        results.min_timestamp.unwrap().to_string(),
        "2024-05-01 12:00:00"
    );
    assert_eq!(
        results.max_timestamp.unwrap().to_string(),
        "2024-05-01 14:00:00"
    );
    assert_eq!(processor.num_records, 3);
    assert_eq!(
        results.largest_gap_duration.unwrap(),
        "01:00:00".to_string()
    );
    assert_eq!(
        results.largest_gap.unwrap(),
        "2024-05-01 12:00:00 to 2024-05-01 13:00:00".to_string()
    );
}

#[test]
fn processes_descending_records_correctly() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings, "Test".to_string());

    processor
        .process_timestamp(&make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(1, "2024-05-01 13:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(2, "2024-05-01 11:00:00", StringRecord::from(vec!["test"])))
        .unwrap();

    let results = processor.get_statistics().unwrap();
    assert_eq!(
        results.min_timestamp.unwrap().to_string(),
        "2024-05-01 11:00:00"
    );
    assert_eq!(
        results.max_timestamp.unwrap().to_string(),
        "2024-05-01 14:00:00"
    );
    assert_eq!(processor.num_records, 3);
    assert_eq!(
        results.largest_gap_duration.unwrap(),
        "02:00:00".to_string()
    );
    assert_eq!(
        results.largest_gap.unwrap(),
        "2024-05-01 11:00:00 to 2024-05-01 13:00:00".to_string()
    );
}

#[test]
fn detects_out_of_order_in_ascending() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Ascending), &settings, "Test".to_string());

    processor
        .process_timestamp(&make_fake_record(0, "2024-05-01 12:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    let result = processor.process_timestamp(&make_fake_record(1, "2024-05-01 11:00:00", StringRecord::from(vec!["test"])));

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "File was not sorted on the identified timestamp. Out of order record at index 1"
    );
}

#[test]
fn detects_out_of_order_in_descending() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings, "Test".to_string());

    processor
        .process_timestamp(&make_fake_record(0, "2024-05-01 12:00:00", StringRecord::from(vec!["test"])))
        .unwrap();
    let result = processor.process_timestamp(&make_fake_record(1, "2024-05-01 13:00:00", StringRecord::from(vec!["test"])));

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "File was not sorted on the identified timestamp. Out of order record at index 1"
    );
}

#[test]
fn returns_none_on_first_timestamp() {
    let mut checker = TimeDirectionChecker::default();
    let result = checker.process_timestamp(dt("2024-05-01 12:00:00"));
    assert_eq!(result, None);
    assert_eq!(checker.previous, Some(dt("2024-05-01 12:00:00")));
}

#[test]
fn detects_ascending_order() {
    let mut checker = TimeDirectionChecker {
        previous: Some(dt("2024-05-01 12:00:00")),
    };
    let result = checker.process_timestamp(dt("2024-05-01 13:00:00"));
    assert_eq!(result, Some(TimeDirection::Ascending));
}

#[test]
fn detects_descending_order() {
    let mut checker = TimeDirectionChecker {
        previous: Some(dt("2024-05-01 13:00:00")),
    };
    let result = checker.process_timestamp(dt("2024-05-01 12:00:00"));
    assert_eq!(result, Some(TimeDirection::Descending));
}

#[test]
fn returns_none_when_timestamps_are_equal() {
    let mut checker = TimeDirectionChecker {
        previous: Some(dt("2024-05-01 12:00:00")),
    };
    let result = checker.process_timestamp(dt("2024-05-01 12:00:00"));
    assert_eq!(result, None);
}

#[test]
fn test_build_file_path_duplicate() {
    let settings = ExecutionSettings {
        output_dir: PathBuf::from("/tmp/output"),
        ..Default::default()
    };
    let processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings, "Test".to_string());

    let result = processor.build_file_path(AlertOutputType::Duplicate).unwrap();
    assert_eq!(result, PathBuf::from("/tmp/output/Test_DUPLICATES.csv"));
}

#[test]
fn test_build_file_path_duplicate_weird_path() {
    let settings = ExecutionSettings {
        output_dir: PathBuf::from("/tmp/\\output//"),
        ..Default::default()
    };
    let processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings, "Test".to_string());

    let result = processor.build_file_path(AlertOutputType::Duplicate).unwrap();
    assert_eq!(result, PathBuf::from("/tmp/output/Test_DUPLICATES.csv"));
}

#[test]
fn test_build_file_path_redaction() {
    let settings = ExecutionSettings {
        output_dir: PathBuf::from("/tmp/output"),
        ..Default::default()
    };
    let processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings, "Test".to_string());

    let result = processor.build_file_path(AlertOutputType::Redaction).unwrap();
    assert_eq!(result, PathBuf::from("/tmp/output/Test_POSSIBLE_REDACTIONS.csv"));
}

#[test]
fn test_build_file_path_missing_execution_settings() {
    let processor = LogRecordProcessor {
        file_name: "Test".to_string(),
        execution_settings: None,
        ..Default::default()
    };

    let result = processor.build_file_path(AlertOutputType::Duplicate);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Could not find execution settings"
    );
}
