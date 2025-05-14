use crate::date_regex::*;
use crate::helpers::*;
use chrono::{NaiveDateTime, TimeDelta};
use csv::StringRecord;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Ordering;
use std::path::PathBuf;

#[cfg(test)]
mod logfilerecord_tests;

#[derive(Debug, Deserialize)]
struct RawRedactionPattern {
    name: String,
    pattern: String,
}

#[derive(Debug)]
struct RedactionPattern {
    name: String,
    pattern: Regex,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionSettings {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub regexes: Vec<DateRegex>,
    pub timestamp_field: Option<String>,
    pub quick_mode: bool,
}

#[derive(PartialEq, Debug)]
pub enum LogType {
    Csv,
    Json,
    Unstructured,
}

#[derive(PartialEq, Debug, Clone)]
pub enum TimeDirection {
    Ascending,
    Descending,
}

#[derive(PartialEq, Debug, Clone)]
pub enum AlertOutputType {
    Duplicate,
    Redaction,
}

#[derive(PartialEq, Debug)]
pub struct LogFile {
    pub log_type: LogType,
    pub file_path: PathBuf,
}

#[derive(PartialEq, Debug, Serialize, Default)]
pub struct ProcessedLogFile {
    pub sha256hash: Option<String>,
    pub filename: Option<String>,
    pub file_path: Option<String>,
    pub size: Option<u64>,
    pub header_index_used: Option<String>,
    pub time_header: Option<String>,
    pub time_format: Option<String>,
    pub min_timestamp: Option<String>,
    pub max_timestamp: Option<String>,
    pub min_max_duration: Option<String>,
    pub largest_gap: Option<String>,
    pub largest_gap_duration: Option<String>,
    pub num_records: Option<String>,
    pub error: Option<String>,
    pub num_dupes: Option<String>,
    pub num_redactions: Option<String>,
}

#[derive(PartialEq, Debug)]
pub struct LogFileRecord {
    pub hash_of_entire_record: u64,
    raw_record: StringRecord,
    pub timestamp: NaiveDateTime,
    pub index: usize,
}

impl LogFileRecord {
    pub fn new(index: usize, timestamp: NaiveDateTime, record: StringRecord) -> Self {
        let mut record_to_output = StringRecord::from(vec![index.to_string()]);
        record_to_output.extend(record.iter());
        Self {
            hash_of_entire_record: hash_csv_record(&record),
            timestamp: timestamp,
            raw_record: record,
            index: index,
        }
    }
    pub fn get_record_to_output(&self, alert_type: AlertOutputType) -> StringRecord {
        let mut base_record = match alert_type {
            AlertOutputType::Duplicate => StringRecord::from(vec![
                self.index.to_string(),
                self.hash_of_entire_record.to_string(),
            ]),
            AlertOutputType::Redaction => StringRecord::from(vec![self.index.to_string()]),
        };
        base_record.extend(self.raw_record.iter());
        base_record
    }
}

#[derive(PartialEq, Debug, Default)]
pub struct TimeStatisticsFields {
    pub num_records: Option<String>,
    pub min_timestamp: Option<String>,
    pub max_timestamp: Option<String>,
    pub min_max_duration: Option<String>,
    pub largest_gap: Option<String>,
    pub largest_gap_duration: Option<String>,
    pub num_dupes: Option<String>,
    pub num_redactions: Option<String>,
}

#[derive(PartialEq, Debug)]
pub struct FlaggedLogFileRecord {
    pub record: String,
    pub index: usize,
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct TimeGap {
    pub gap: TimeDelta,
    pub beginning_time: NaiveDateTime,
    pub end_time: NaiveDateTime,
}

impl Eq for TimeGap {}

impl Ord for TimeGap {
    fn cmp(&self, other: &Self) -> Ordering {
        self.gap.cmp(&other.gap)
    }
}

impl PartialOrd for TimeGap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TimeGap {
    pub fn new(a: NaiveDateTime, b: NaiveDateTime) -> Self {
        let (beginning_time, end_time) = if a <= b { (a, b) } else { (b, a) };

        let gap = end_time.signed_duration_since(beginning_time);

        Self {
            gap,
            beginning_time,
            end_time,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IdentifiedTimeInformation {
    // Maybe add a date format pretty. and then also the date format that gets used by chrono
    pub header_row: Option<u64>,
    pub headers: Option<StringRecord>,
    pub column_name: Option<String>,
    pub column_index: Option<usize>,
    pub regex_info: DateRegex,
    pub direction: Option<TimeDirection>,
}
