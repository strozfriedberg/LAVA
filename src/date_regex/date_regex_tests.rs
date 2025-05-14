use super::*;
use chrono::{NaiveDate, NaiveTime};
#[test]
fn test_date_regex_from_raw() {
    let raw_re = RawDateRegex {
        pretty_format: "YYYY-MM-DDTHH:MM:SS.SSS".to_string(),
        strftime_format: "%Y-%m-%dT%H:%M:%S%.3f".to_string(),
        regex: "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{1,3}".to_string(),
    };
    let re = DateRegex::new_from_raw_date_regex(raw_re);
    let test_input = "2023-01-01T01:00:00.000";
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let time = NaiveTime::from_hms_milli_opt(1, 0, 0, 0).unwrap();
    let expected_timestamp = NaiveDateTime::new(date, time);
    let actual_timestamp = re
        .get_timestamp_object_from_string_contianing_date(test_input.to_string())
        .unwrap()
        .expect("Failed to get timestamp");
    assert_eq!(expected_timestamp, actual_timestamp);
}
