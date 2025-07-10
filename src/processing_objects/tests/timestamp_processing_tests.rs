use super::super::*;
use crate::basic_objects::{ExecutionSettings, TimeDirection, TimeGap};
use crate::test_helpers::*;
use csv::StringRecord;

#[test]
fn processes_ascending_records_correctly() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(TimeDirection::Ascending),
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

    assert_eq!(
        processor.min_timestamp.unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(
        processor.max_timestamp.unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 15:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    let expected_time_gap = TimeGap::new(
        NaiveDateTime::parse_from_str("2024-05-01 13:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 15:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
    );

    assert_eq!(processor.largest_time_gap.unwrap(), expected_time_gap);
}

#[test]
fn processes_ascending_records_same_time_gap_correctly() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(TimeDirection::Ascending),
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

    assert_eq!(
        processor.min_timestamp.unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(
        processor.max_timestamp.unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 14:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    let expected_time_gap = TimeGap::new(
        NaiveDateTime::parse_from_str("2024-05-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 13:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
    );

    assert_eq!(processor.largest_time_gap.unwrap(), expected_time_gap);
}

#[test]
fn processes_descending_records_correctly() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(TimeDirection::Descending),
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

    assert_eq!(
        processor.min_timestamp.unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 11:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(
        processor.max_timestamp.unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 14:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    let expected_time_gap = TimeGap::new(
        NaiveDateTime::parse_from_str("2024-05-01 11:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        NaiveDateTime::parse_from_str("2024-05-01 13:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
    );

    assert_eq!(processor.largest_time_gap.unwrap(), expected_time_gap);
}

#[test]
fn detects_out_of_order_in_ascending() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(TimeDirection::Ascending),
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
    let _ = processor.process_timestamp(&make_fake_record(
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
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(TimeDirection::Descending),
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
    let _ = processor.process_timestamp(&make_fake_record(
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
