use crate::date_regex::*;
use chrono::{NaiveDateTime, TimeDelta};
use serde::Serialize;
use std::cmp::Ordering;
use std::path::PathBuf;


#[derive(Debug)]
pub struct CommandLineArgs {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub provided_regexes: Option<Vec<DateRegex>>,
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
    pub time_header: Option<String>,
    pub time_format: Option<String>,
    pub min_timestamp: Option<String>,
    pub max_timestamp: Option<String>,
    pub min_max_duration: Option<String>,
    pub largest_gap: Option<String>,
    pub largest_gap_duration: Option<String>,
    pub num_records: Option<String>,
    pub error: Option<String>,
}

#[derive(PartialEq, Debug)]
pub struct LogFileRecord {
    pub hash_of_entire_record: u64,
    pub timestamp: NaiveDateTime,
    pub index: usize,
}

#[derive(PartialEq, Debug, Default)]
pub struct TimeStatisticsFields {
    pub num_records: Option<String>,
    pub min_timestamp: Option<String>,
    pub max_timestamp: Option<String>,
    pub min_max_duration: Option<String>,
    pub largest_gap: Option<String>,
    pub largest_gap_duration: Option<String>,
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
    pub column_name: Option<String>,
    pub column_index: Option<usize>,
    pub regex_info: DateRegex,
    pub direction: Option<TimeDirection>,
}
