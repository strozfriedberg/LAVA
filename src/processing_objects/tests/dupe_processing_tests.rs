use super::super::*;
use crate::basic_objects::{ExecutionSettings, TimeDirection};
use crate::test_helpers::*;
use csv::StringRecord;
// Test when the record is not a duplicate
#[test]
fn test_process_record_no_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(Some(TimeDirection::Descending)),
        &settings,
        "Test".to_string(),
        None,
        true
    );

    let record = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test"]),
    );
    let _ = processor.process_record_for_dupes(&record);
    assert_eq!(processor.duplicate_checker_set.len(), 1);
    assert_eq!(processor.num_dupes, 0);
}

#[test]
fn test_process_record_with_one_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(Some(TimeDirection::Descending)),
        &settings,
        "Test".to_string(),
        None,
        true
    );

    let record1 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test"]),
    );
    let record2 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45"]),
    );
    let record3 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45"]),
    );
    let _ = processor.process_record_for_dupes(&record1);
    let _ = processor.process_record_for_dupes(&record2);
    let _ = processor.process_record_for_dupes(&record3);

    assert_eq!(processor.duplicate_checker_set.len(), 2);
    assert_eq!(processor.num_dupes, 1);
}

#[test]
fn test_process_record_with_two_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(Some(TimeDirection::Descending)),
        &settings,
        "Test".to_string(),
        None,
        true
    );

    let record1 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45"]),
    );
    let record2 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45"]),
    );
    let record3 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45"]),
    );
    let _ = processor.process_record_for_dupes(&record1);
    let _ = processor.process_record_for_dupes(&record2);
    let _ = processor.process_record_for_dupes(&record3);

    assert_eq!(processor.duplicate_checker_set.len(), 1);
    assert_eq!(processor.num_dupes, 2);
}

#[test]
fn test_process_record_with_no_dupe_multiple_values() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(Some(TimeDirection::Descending)),
        &settings,
        "Test".to_string(),
        None,
        true
    );

    let record1 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45", "1"]),
    );
    let record2 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45", "2"]),
    );
    let record3 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45", "3"]),
    );
    let _ = processor.process_record_for_dupes(&record1);
    let _ = processor.process_record_for_dupes(&record2);
    let _ = processor.process_record_for_dupes(&record3);

    assert_eq!(processor.duplicate_checker_set.len(), 3);
    assert_eq!(processor.num_dupes, 0);
}

#[test]
fn test_process_record_with_dupe_multiple_values() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(Some(TimeDirection::Descending)),
        &settings,
        "Test".to_string(),
        None,
        true
    );

    let record1 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45", "1"]),
    );
    let record2 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45", "2"]),
    );
    let record3 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["test45", "1"]),
    );
    let _ = processor.process_record_for_dupes(&record1);
    let _ = processor.process_record_for_dupes(&record2);
    let _ = processor.process_record_for_dupes(&record3);

    assert_eq!(processor.duplicate_checker_set.len(), 2);
    assert_eq!(processor.num_dupes, 1);
}
