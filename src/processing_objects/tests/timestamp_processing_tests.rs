use super::super::*;
use crate::basic_objects::{ExecutionSettings, TimeDirection};
use crate::test_helpers::*;
use csv::StringRecord;

#[test]
fn processes_ascending_records_correctly() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Ascending),
        &settings,
        "Test".to_string(),
        None,
    );

    processor
        .process_timestamp(&make_fake_record(
            0,
            "2024-05-01 12:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(
            1,
            "2024-05-01 13:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(
            2,
            "2024-05-01 15:00:00",
            StringRecord::from(vec!["test"]),
        ))
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
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Ascending),
        &settings,
        "Test".to_string(),
        None,
    );

    processor
        .process_timestamp(&make_fake_record(
            0,
            "2024-05-01 12:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(
            1,
            "2024-05-01 13:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(
            2,
            "2024-05-01 14:00:00",
            StringRecord::from(vec!["test"]),
        ))
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
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    processor
        .process_timestamp(&make_fake_record(
            0,
            "2024-05-01 14:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(
            1,
            "2024-05-01 13:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    processor
        .process_timestamp(&make_fake_record(
            2,
            "2024-05-01 11:00:00",
            StringRecord::from(vec!["test"]),
        ))
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
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Ascending),
        &settings,
        "Test".to_string(),
        None,
    );

    processor
        .process_timestamp(&make_fake_record(
            0,
            "2024-05-01 12:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    let result = processor.process_timestamp(&make_fake_record(
        1,
        "2024-05-01 11:00:00",
        StringRecord::from(vec!["test"]),
    ));

    assert!(processor.errors.len() == 1);

    assert_eq!(
        processor.errors[0].reason.to_string(),
        "File was not sorted on the identified timestamp. Out of order record at index 1"
    );
}

#[test]
fn detects_out_of_order_in_descending() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    processor
        .process_timestamp(&make_fake_record(
            0,
            "2024-05-01 12:00:00",
            StringRecord::from(vec!["test"]),
        ))
        .unwrap();
    let result = processor.process_timestamp(&make_fake_record(
        1,
        "2024-05-01 13:00:00",
        StringRecord::from(vec!["test"]),
    ));

    assert!(processor.errors.len() == 1);

    assert_eq!(
        processor.errors[0].reason.to_string(),
        "File was not sorted on the identified timestamp. Out of order record at index 1"
    );
}
