use crate::basic_objects::LogFileRecord;
use chrono::NaiveDateTime;
use csv::StringRecord;

pub fn make_fake_record(index: usize, timestamp_str: &str, record: StringRecord) -> LogFileRecord {
    LogFileRecord::new(
        index,
        NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S").unwrap(),
        record,
    )
}
pub fn dt(s: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
}
