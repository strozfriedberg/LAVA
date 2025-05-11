#[cfg(test)]
use chrono::NaiveDateTime;
use log_checker::basic_objects::{LogFileRecord, TimeDirection};
use log_checker::timestamp_tools::LogRecordProcessor;

fn make_fake_record(index: usize, timestamp_str: &str) -> LogFileRecord {
    LogFileRecord {
        index,
        timestamp: NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S").unwrap(),
        hash_of_entire_record: index as u64, // For simplicity
    }
}

#[test]
fn processes_ascending_records_correctly() {
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Ascending));

    processor
        .process_timestamp(make_fake_record(0, "2024-05-01 12:00:00"))
        .unwrap();
    processor
        .process_timestamp(make_fake_record(1, "2024-05-01 13:00:00"))
        .unwrap();
    processor
        .process_timestamp(make_fake_record(2, "2024-05-01 14:00:00"))
        .unwrap();

    assert_eq!(
        processor.min_timestamp.unwrap().to_string(),
        "2024-05-01 12:00:00"
    );
    assert_eq!(
        processor.max_timestamp.unwrap().to_string(),
        "2024-05-01 14:00:00"
    );
    assert_eq!(processor.num_records, 3);
    assert!(processor.largest_time_gap.is_some());
}

#[test]
fn processes_descending_records_correctly() {
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending));

    processor
        .process_timestamp(make_fake_record(0, "2024-05-01 14:00:00"))
        .unwrap();
    processor
        .process_timestamp(make_fake_record(1, "2024-05-01 13:00:00"))
        .unwrap();
    processor
        .process_timestamp(make_fake_record(2, "2024-05-01 12:00:00"))
        .unwrap();

    assert_eq!(
        processor.max_timestamp.unwrap().to_string(),
        "2024-05-01 14:00:00"
    );
    assert_eq!(
        processor.min_timestamp.unwrap().to_string(),
        "2024-05-01 12:00:00"
    );
    assert_eq!(processor.num_records, 3);
    assert!(processor.largest_time_gap.is_some());
}

#[test]
fn detects_out_of_order_in_ascending() {
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Ascending));

    processor
        .process_timestamp(make_fake_record(0, "2024-05-01 12:00:00"))
        .unwrap();
    let result = processor.process_timestamp(make_fake_record(1, "2024-05-01 11:00:00"));

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "File was not sorted on the identified timestamp. Out of order record at index 1"
    );
}

#[test]
fn detects_out_of_order_in_descending() {
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending));

    processor
        .process_timestamp(make_fake_record(0, "2024-05-01 12:00:00"))
        .unwrap();
    let result = processor.process_timestamp(make_fake_record(1, "2024-05-01 13:00:00"));

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "File was not sorted on the identified timestamp. Out of order record at index 1"
    );
}
