use crate::alerts::Alert;
use crate::date_regex::*;
use crate::errors::LavaError;
use crate::helpers::*;
use chrono::{NaiveDateTime, TimeDelta};
use csv::StringRecord;
use std::cmp::Ordering;
use std::fmt;
use std::path::PathBuf;

#[cfg(test)]
mod logfilerecord_tests;

pub static WELFORD_TIME_SIGNIFIGANCE: TimeSignifigance = TimeSignifigance::Milliseconds; //Is this going to be too big for the welford calc?
pub enum TimeSignifigance {
    Seconds,
    Milliseconds,
}
impl fmt::Display for TimeSignifigance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let as_str = match self {
            TimeSignifigance::Milliseconds => "Milliseconds",
            TimeSignifigance::Seconds => "Seconds",
        };
        write!(f, "{}", as_str)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionSettings {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub regexes: Vec<DateRegex>,
    pub timestamp_field: Option<String>,
    pub quick_mode: bool,
    pub verbose_mode: bool,
    pub actually_write_to_files: bool,
}

impl ExecutionSettings {
    // #[cfg(test)]
    pub fn create_integration_test_object(
        timestamp_field: Option<String>,
        quick_mode: bool,
    ) -> Self {
        use crate::PREBUILT_DATE_REGEXES;
        Self {
            timestamp_field: timestamp_field,
            quick_mode: quick_mode,
            regexes: PREBUILT_DATE_REGEXES.clone(),
            actually_write_to_files: false,
            ..Default::default()
        }
    }
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

#[derive(Debug, Default)]
pub struct ProcessedLogFile {
    pub sha256hash: Option<String>,
    pub filename: Option<String>,
    pub file_path: Option<String>,
    pub size: Option<String>,
    pub first_data_row_used: Option<String>,
    pub time_header: Option<String>,
    pub time_format: Option<String>,
    pub min_timestamp: Option<String>,
    pub max_timestamp: Option<String>,
    pub min_max_duration: Option<String>,
    pub largest_gap: Option<String>,
    pub largest_gap_duration: Option<String>,
    pub mean_time_gap: Option<String>,
    pub std_dev_time_gap: Option<String>,
    pub number_of_std_devs_above: Option<String>,
    pub num_records: Option<String>,
    pub num_dupes: Option<String>,
    pub num_redactions: Option<String>,
    pub errors: Vec<LavaError>,
    pub alerts: Option<Vec<Alert>>,
}

#[derive(Debug, Default)]
pub struct TimeStatisticsFields {
    pub num_records: Option<String>,
    pub min_timestamp: Option<String>,
    pub max_timestamp: Option<String>,
    pub min_max_duration: Option<String>,
    pub largest_gap: Option<String>,
    pub largest_gap_duration: Option<String>,
    pub num_dupes: Option<String>,
    pub num_redactions: Option<String>,
    pub mean_time_gap: Option<String>,
    pub std_dev_time_gap: Option<String>,
    pub number_of_std_devs_above: Option<String>,
}

#[derive(PartialEq, Debug)]
pub struct LogFileRecord {
    pub hash_of_entire_record: u64,
    pub raw_record: StringRecord,
    pub timestamp: Option<NaiveDateTime>,
    pub index: usize,
}

impl LogFileRecord {
    pub fn new(index: usize, timestamp: Option<NaiveDateTime>, record: StringRecord) -> Self {
        let mut record_to_output = StringRecord::from(vec![index.to_string()]);
        record_to_output.extend(record.iter());
        Self {
            hash_of_entire_record: hash_csv_record(&record),
            timestamp: timestamp,
            raw_record: record,
            index: index,
        }
    }
    pub fn get_record_to_output(
        &self,
        alert_type: &AlertOutputType,
        rule_name: Option<String>,
    ) -> StringRecord {
        let mut base_record = match alert_type {
            AlertOutputType::Duplicate => StringRecord::from(vec![
                self.index.to_string(),
                format!("{:x}", self.hash_of_entire_record),
            ]),
            AlertOutputType::Redaction => {
                StringRecord::from(vec![self.index.to_string(), rule_name.unwrap()])
            }
        };
        base_record.extend(self.raw_record.iter());
        base_record
    }
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
    pub fn get_time_duration_number(&self) -> i64 {
        match WELFORD_TIME_SIGNIFIGANCE {
            TimeSignifigance::Milliseconds => self.gap.num_milliseconds(),
            TimeSignifigance::Seconds => self.gap.num_seconds(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IdentifiedTimeInformation {
    pub column_name: Option<String>,
    pub column_index: Option<usize>,
    pub regex_info: DateRegex,
    pub direction: Option<TimeDirection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeaderInfo {
    pub first_data_row: usize,
    pub headers: StringRecord,
}
