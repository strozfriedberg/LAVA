use csv::StringRecord;
use log_checker::basic_objects::{ExecutionSettings, TimeDirection};
use log_checker::timestamp_tools::LogRecordProcessor;
use log_checker::helpers::make_fake_record;

// Test when the record is not a duplicate
#[test]
fn test_process_record_no_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings);

    let record = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test"]));
    let _ = processor.process_record_for_dupes_and_redactions(&record, false);
    assert_eq!(processor.duplicate_checker_set.len(), 1);
    assert_eq!(processor.num_dupes, 0);
}

#[test]
fn test_process_record_with_one_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings);

    let record1 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test"]));
    let record2 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let record3 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let _ = processor.process_record_for_dupes_and_redactions(&record1, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record2, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 2);
    assert_eq!(processor.num_dupes, 1);
}

#[test]
fn test_process_record_with_two_duplicate() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings);

    let record1 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let record2 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let record3 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let _ = processor.process_record_for_dupes_and_redactions(&record1, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record2, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 1);
    assert_eq!(processor.num_dupes, 2);
}

#[test]
fn test_process_record_with_no_dupe_multiple_values() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings);

    let record1 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45", "1"]));
    let record2 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45", "2"]));
    let record3 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45", "3"]));
    let _ = processor.process_record_for_dupes_and_redactions(&record1, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record2, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 3);
    assert_eq!(processor.num_dupes, 0);
}

#[test]
fn test_process_record_with_dupe_multiple_values() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(Some(TimeDirection::Descending), &settings);

    let record1 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45", "1"]));
    let record2 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45", "2"]));
    let record3 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45", "1"]));
    let _ = processor.process_record_for_dupes_and_redactions(&record1, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record2, false);
    let _ = processor.process_record_for_dupes_and_redactions(&record3, false);

    assert_eq!(processor.duplicate_checker_set.len(), 2);
    assert_eq!(processor.num_dupes, 1);
}

// Test when the record is a duplicate
// #[test]
// fn test_process_record_duplicate() {
//     let mut processor = LogRecordProcessor::new_with_order(None);

//     let record1 = LogFileRecord {
//         index: 1,
//         hash_of_entire_record: 12345,
//         timestamp: NaiveDate::from_ymd(2021, 5, 11).and_hms(12, 0, 0),
//     };
//     let record2 = LogFileRecord {
//         index: 2,
//         hash_of_entire_record: 12345,
//         timestamp: NaiveDate::from_ymd(2021, 5, 11).and_hms(12, 5, 0),
//     };

//     let result1 = processor.process_record(record1);
//     let result2 = processor.process_record(record2);

//     assert!(result1.is_ok());
//     assert!(result2.is_ok());
//     assert_eq!(processor.num_records, 2);
//     assert!(processor.duplicate_checker_set.contains(&12345)); // Ensure it's still in the set

//     // The second record should be detected as a duplicate
//     // We expect the second record not to be inserted as new
// }

// // Test that the function writes to file when a duplicate is found and `write_hits_to_file` is true
// #[test]
// fn test_process_record_duplicate_with_write() {
//     let mut processor = LogRecordProcessor::new_with_order(None);

//     let record1 = LogFileRecord {
//         index: 1,
//         hash_of_entire_record: 12345,
//         timestamp: NaiveDate::from_ymd(2021, 5, 11).and_hms(12, 0, 0),
//     };
//     let record2 = LogFileRecord {
//         index: 2,
//         hash_of_entire_record: 12345,
//         timestamp: NaiveDate::from_ymd(2021, 5, 11).and_hms(12, 5, 0),
//     };

//     // Make sure the println from write_hit_to_file is captured during the test.
//     let result1 = processor.process_record(record1);
//     let result2 = processor.process_record(record2);

//     assert!(result1.is_ok());
//     assert!(result2.is_ok());

//     // Check that the write_hit_to_file function was triggered when a duplicate was found
// }

// // Test that when a record is unique, it does not trigger a duplicate message
// #[test]
// fn test_process_record_unique_record() {
//     let mut processor = LogRecordProcessor::new_with_order(None);

//     let record1 = LogFileRecord {
//         index: 1,
//         hash_of_entire_record: 12345,
//         timestamp: NaiveDate::from_ymd(2021, 5, 11).and_hms(12, 0, 0),
//     };
//     let record2 = LogFileRecord {
//         index: 2,
//         hash_of_entire_record: 67890,
//         timestamp: NaiveDate::from_ymd(2021, 5, 11).and_hms(12, 5, 0),
//     };

//     let result1 = processor.process_record(record1);
//     let result2 = processor.process_record(record2);

//     assert!(result1.is_ok());
//     assert!(result2.is_ok());
//     assert_eq!(processor.num_records, 2);
//     assert!(!processor.duplicate_checker_set.contains(&67890)); // Ensure unique record isn't duplicated
// }
