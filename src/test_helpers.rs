use crate::basic_objects::{IdentifiedTimeInformation, LogFileRecord, TimeDirection};
use crate::date_regex::DateRegex;
use chrono::NaiveDateTime;
use csv::StringRecord;
use regex::Regex;

pub fn make_fake_record(index: usize, timestamp_str: Option<&str>, record: StringRecord) -> LogFileRecord {
    LogFileRecord::new(
        index,
        match timestamp_str {
            Some(timestamp) => Some(NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").unwrap()),
            None => None
        },
        record,
    )
}

pub fn build_fake_timestamp_hit_from_direction(
    direction: TimeDirection,
) -> Option<IdentifiedTimeInformation> {
    let regex = Regex::new(".*").ok()?; // Match anything

    let fake_regex_info = DateRegex {
        pretty_format: "FAKE_TIMESTAMP".to_string(),
        strftime_format: "%s".to_string(), // Epoch timestamp format, or adjust as needed
        regex,
    };
    Some(IdentifiedTimeInformation {
        column_name: None,
        column_index: None,
        regex_info: fake_regex_info, // Assumes DateRegex implements Default
        direction: Some(direction),
    })
}
pub fn dt(s: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
}
