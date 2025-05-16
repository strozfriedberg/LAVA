use super::*;
use chrono::NaiveDate;

#[test]
fn test_log_file_record_new() {
    let index = 1;
    let timestamp = NaiveDate::from_ymd_opt(2023, 5, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let raw = StringRecord::from(vec!["value1", "value2"]);

    let record = LogFileRecord::new(index, timestamp, raw.clone());

    assert_eq!(record.index, index);
    assert_eq!(record.timestamp, timestamp);
    assert_eq!(record.raw_record, raw);
    assert_eq!(record.hash_of_entire_record, hash_csv_record(&raw));
}

#[test]
fn test_get_record_to_output_duplicate() {
    let index = 2;
    let timestamp = NaiveDate::from_ymd_opt(2023, 5, 2)
        .unwrap()
        .and_hms_opt(10, 30, 0)
        .unwrap();
    let raw = StringRecord::from(vec!["foo", "bar"]);
    let record = LogFileRecord::new(index, timestamp, raw.clone());

    let output = record.get_record_to_output(&AlertOutputType::Duplicate, None);
    let expected = {
        let mut sr = StringRecord::from(vec![
            index.to_string(),
            format!("{:x}", record.hash_of_entire_record),
        ]);
        sr.extend(raw.iter());
        sr
    };

    assert_eq!(output, expected);
}

#[test]
fn test_get_record_to_output_redaction() {
    let index = 3;
    let timestamp = NaiveDate::from_ymd_opt(2023, 5, 3)
        .unwrap()
        .and_hms_opt(9, 0, 0)
        .unwrap();
    let raw = StringRecord::from(vec!["redact", "this"]);
    let record = LogFileRecord::new(index, timestamp, raw.clone());

    let output = record.get_record_to_output(&AlertOutputType::Redaction, Some("Test Rule".to_string()));
    let expected = {
        let mut sr = StringRecord::from(vec![index.to_string(), "Test Rule".to_string()]);
        sr.extend(raw.iter());
        sr
    };

    assert_eq!(output, expected);
}
