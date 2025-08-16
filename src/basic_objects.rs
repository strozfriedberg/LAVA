use crate::alerts::Alert;
use crate::date_regex::*;
use crate::errors::LavaError;
use crate::helpers::*;
use chrono::{NaiveDateTime, TimeDelta};
use csv::StringRecord;
use human_time::human_time;
use num_format::{Locale, ToFormattedString};
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
    pub input: PathBuf,
    pub output_dir: PathBuf,
    pub regexes: Vec<DateRegex>,
    pub timestamp_field: Option<String>,
    pub quick_mode: bool,
    pub multipart_mode: bool,
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
    pub min_timestamp: Option<NaiveDateTime>,
    pub max_timestamp: Option<NaiveDateTime>,
    pub largest_gap: Option<TimeGap>,
    pub mean_time_gap: Option<f64>,
    pub variance_time_gap: Option<f64>,
    pub total_num_records: usize,
    pub timestamp_num_records: usize,
    pub num_dupes: Option<usize>,
    pub num_redactions: Option<usize>,
    pub errors: Vec<LavaError>,
    pub alerts: Option<Vec<Alert>>,
}

impl ProcessedLogFile {
    pub fn get_strings_for_file_statistics_output_row(&self) -> Vec<String> {
        let error_message = if self.errors.is_empty() {
            String::new()
        } else {
            if self.errors.len() > 1 {
                format!(
                    "There were {} errors during processing. Check errors.csv for detailed errors.",
                    self.errors.len()
                )
            } else {
                self.errors[0].reason.clone()
            }
        };
        vec![
            self.filename.as_deref().unwrap_or("").to_string(),
            self.file_path.as_deref().unwrap_or("").to_string(),
            self.sha256hash.as_deref().unwrap_or("").to_string(),
            self.size.as_deref().unwrap_or("").to_string(),
            self.first_data_row_used
                .as_deref()
                .unwrap_or("")
                .to_string(),
            self.time_header.as_deref().unwrap_or("").to_string(),
            self.time_format.as_deref().unwrap_or("").to_string(),
            self.total_num_records.to_formatted_string(&Locale::en),
            self.timestamp_num_records.to_formatted_string(&Locale::en),
            match self.min_timestamp {
                None => String::new(),
                Some(timestamp) => timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
            },
            match self.max_timestamp {
                None => String::new(),
                Some(timestamp) => timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
            },
            self.get_min_max_duration(TimestampStringType::Hours)
                .unwrap_or("".to_string()),
            self.get_min_max_duration(TimestampStringType::Human)
                .unwrap_or("".to_string()),
            match self.largest_gap {
                None => String::new(),
                Some(largest_gap) => largest_gap.to_string(),
            },
            self.get_largest_gap_duration(TimestampStringType::Hours)
                .unwrap_or("".to_string()),
            self.get_largest_gap_duration(TimestampStringType::Human)
                .unwrap_or("".to_string()),
            self.mean_time_gap
                .map(|v| v.to_string())
                .unwrap_or_default(),
            self.variance_time_gap
                .map(|v| v.sqrt().to_string())
                .unwrap_or_default(),
            self.get_num_std_devs_above_mean().unwrap_or("".to_string()),
            self.num_dupes
                .map(|v| v.to_formatted_string(&Locale::en))
                .unwrap_or_default(),
            self.num_redactions
                .map(|v| v.to_formatted_string(&Locale::en))
                .unwrap_or_default(),
            error_message,
        ]
    }

    pub fn get_quick_stats(&self) -> Option<QuickStats> {
        Some(QuickStats {
            filename: self.filename.clone()?,
            min_timestamp: self.min_timestamp?.format("%Y-%m-%d %H:%M:%S").to_string(),
            max_timestamp: self.max_timestamp?.format("%Y-%m-%d %H:%M:%S").to_string(),
            largest_gap_duration: self.largest_gap?.gap,
            largest_gap_duration_human: self
                .get_largest_gap_duration(TimestampStringType::Human)?,
            num_records: self.timestamp_num_records.to_formatted_string(&Locale::en),
        })
    }
    
    pub fn get_processed_log_file_combination_essentials(&self) -> Option<ProcessedLogFileComboEssentials> {
        if self.timestamp_num_records == 0 {
            return None;
        }
        else if self.timestamp_num_records == 1 {
            return Some(ProcessedLogFileComboEssentials {
                min_timestamp: self.min_timestamp?,
                max_timestamp: self.max_timestamp?,
                num_time_gaps: 0,
                largest_gap: None,
                time_gap_mean: 0.0,
                time_gap_var: 0.0,
            })
        }else{
            return Some(ProcessedLogFileComboEssentials {
                min_timestamp: self.min_timestamp?,
                max_timestamp: self.max_timestamp?,
                num_time_gaps: self.timestamp_num_records - 1,
                largest_gap: self.largest_gap,
                time_gap_mean: self.mean_time_gap?,
                time_gap_var: self.variance_time_gap?,
            })
        }
    }

    fn get_min_max_duration(&self, time_type: TimestampStringType) -> Option<String> {
        let chrono_duration = self.max_timestamp? - self.min_timestamp?;
        match time_type {
            TimestampStringType::Hours => Some(
                ProcessedLogFile::convert_time_delta_to_number_of_hours(chrono_duration),
            ),
            TimestampStringType::Human => {
                ProcessedLogFile::convert_time_delta_to_human_time(chrono_duration)
            }
        }
    }

    fn get_largest_gap_duration(&self, time_type: TimestampStringType) -> Option<String> {
        let largest_gap = self.largest_gap?;
        let chrono_duration = largest_gap.end_time - largest_gap.beginning_time;
        match time_type {
            TimestampStringType::Hours => Some(
                ProcessedLogFile::convert_time_delta_to_number_of_hours(chrono_duration),
            ),
            TimestampStringType::Human => {
                ProcessedLogFile::convert_time_delta_to_human_time(chrono_duration)
            }
        }
    }

    fn get_num_std_devs_above_mean(&self) -> Option<String> {
        Some(
            ((self.largest_gap?.get_time_duration_number() as f64 - self.mean_time_gap?)
                / self.variance_time_gap?.sqrt())
                .to_string(),
        )
    }

    fn convert_time_delta_to_number_of_hours(tdelta: TimeDelta) -> String {
        let total_seconds = tdelta.num_seconds().abs(); // make it positive for display

        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;

        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    }

    fn convert_time_delta_to_human_time(chrono_duration: TimeDelta) -> Option<String> {
        // Convert chrono::Duration to std::time::Duration
        let std_duration = if let Some(dur) = chrono_duration.to_std().ok() {
            dur
        } else {
            eprintln!("Negative duration not supported by std::time::Duration");
            return None;
        };
        Some(human_time(std_duration))
    }
}

enum TimestampStringType {
    Human,
    Hours,
}

#[derive(Debug, Clone)]
pub struct QuickStats {
    pub filename: String,
    pub min_timestamp: String,
    pub max_timestamp: String,
    pub largest_gap_duration: TimeDelta,
    pub largest_gap_duration_human: String,
    pub num_records: String,
}

#[derive(Debug, Clone)]
pub struct ProcessedLogFileComboEssentials {
    pub min_timestamp: NaiveDateTime,
    pub max_timestamp: NaiveDateTime,
    pub num_time_gaps: usize,
    pub largest_gap: Option<TimeGap>,
    pub time_gap_mean: f64,
    pub time_gap_var: f64,
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

impl fmt::Display for TimeGap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write how the struct should be converted to a string
        write!(
            f,
            "{} to {}",
            self.beginning_time.format("%Y-%m-%d %H:%M:%S"),
            self.end_time.format("%Y-%m-%d %H:%M:%S")
        )
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
