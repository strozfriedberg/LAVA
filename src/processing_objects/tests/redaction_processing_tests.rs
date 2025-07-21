use super::super::*;
use crate::basic_objects::{ExecutionSettings, TimeDirection};
use crate::test_helpers::*;
use csv::StringRecord;

#[test]
fn test_process_record_contains_redaction() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );
    let record2 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec!["********"]),
    );
    let record3 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec![
            "[Jan 11, 2025 3:50:56 PM  ] -  REMOVED HTTP/1.1 200 76 [0]",
        ]),
    );
    let _ = processor.process_record_for_redactions(&record2);
    let _ = processor.process_record_for_redactions(&record3);

    assert_eq!(processor.num_redactions, 2);
}

#[test]
fn test_process_record_contains_redactions_multiple_columns() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new(
        &build_fake_timestamp_hit_from_direction(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );
    let record3 = make_fake_record(
        0,
        Some("2024-05-01 14:00:00"),
        StringRecord::from(vec![
            "Test row",
            "[Jan 11, 2025 3:50:56 PM  ] - ********** HTTP/1.1 200 76 [0]",
        ]),
    );
    let _ = processor.process_record_for_redactions(&record3);

    assert_eq!(processor.num_redactions, 1);
}
