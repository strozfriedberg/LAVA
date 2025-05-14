use super::super::*;
use crate::basic_objects::{ExecutionSettings, TimeDirection};
use csv::StringRecord;
use crate::test_helpers::*;
// Test when the record is not a duplicate
#[test]
fn test_process_record_no_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let record = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test"]));
    let _ = processor.process_record_for_dupes(&record, false);
    assert_eq!(processor.duplicate_checker_set.len(), 1);
    assert_eq!(processor.num_dupes, 0);
}

#[test]
fn test_process_record_with_one_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let record1 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test"]));
    let record2 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let record3 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let _ = processor.process_record_for_dupes(&record1, false);
    let _ = processor.process_record_for_dupes(&record2, false);
    let _ = processor.process_record_for_dupes(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 2);
    assert_eq!(processor.num_dupes, 1);
}

#[test]
fn test_process_record_with_two_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let record1 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let record2 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let record3 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let _ = processor.process_record_for_dupes(&record1, false);
    let _ = processor.process_record_for_dupes(&record2, false);
    let _ = processor.process_record_for_dupes(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 1);
    assert_eq!(processor.num_dupes, 2);
}

#[test]
fn test_process_record_with_no_dupe_multiple_values() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let record1 = make_fake_record(
        0,
        "2024-05-01 14:00:00",
        StringRecord::from(vec!["test45", "1"]),
    );
    let record2 = make_fake_record(
        0,
        "2024-05-01 14:00:00",
        StringRecord::from(vec!["test45", "2"]),
    );
    let record3 = make_fake_record(
        0,
        "2024-05-01 14:00:00",
        StringRecord::from(vec!["test45", "3"]),
    );
    let _ = processor.process_record_for_dupes(&record1, false);
    let _ = processor.process_record_for_dupes(&record2, false);
    let _ = processor.process_record_for_dupes(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 3);
    assert_eq!(processor.num_dupes, 0);
}

#[test]
fn test_process_record_with_dupe_multiple_values() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let record1 = make_fake_record(
        0,
        "2024-05-01 14:00:00",
        StringRecord::from(vec!["test45", "1"]),
    );
    let record2 = make_fake_record(
        0,
        "2024-05-01 14:00:00",
        StringRecord::from(vec!["test45", "2"]),
    );
    let record3 = make_fake_record(
        0,
        "2024-05-01 14:00:00",
        StringRecord::from(vec!["test45", "1"]),
    );
    let _ = processor.process_record_for_dupes(&record1, false);
    let _ = processor.process_record_for_dupes(&record2, false);
    let _ = processor.process_record_for_dupes(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 2);
    assert_eq!(processor.num_dupes, 1);
}
