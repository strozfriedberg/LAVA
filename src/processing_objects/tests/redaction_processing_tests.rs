use super::super::*;
use crate::basic_objects::{ExecutionSettings, TimeDirection};
use crate::test_helpers::*;
use csv::StringRecord;


#[test]
fn test_process_record_contains_redaction() {
    let settings = ExecutionSettings::default();
    let mut processor = LogRecordProcessor::new_with_order(
        Some(TimeDirection::Descending),
        &settings,
        "Test".to_string(),
        None,
    );

    let record1 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test"]));
    let record2 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["********"]));
    let record3 = make_fake_record(0, "2024-05-01 14:00:00", StringRecord::from(vec!["test45"]));
    let _ = processor.process_record_for_redactions(&record1, false);
    let _ = processor.process_record_for_redactions(&record2, false);
    let _ = processor.process_record_for_redactions(&record3, false);

    assert_eq!(processor.num_redactions, 1);
}