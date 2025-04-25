use clap::builder::Str;
use glob::glob;
use core::time;
use std::path::PathBuf;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use sha2::{Sha256, Digest};
use std::error::Error;
use std::collections::HashSet;
use rayon::{prelude::*, result};
use serde::Serialize;
use regex::Regex;
use once_cell::sync::Lazy;
// use polars::prelude::*;
use csv::{ReaderBuilder, StringRecord, Writer};
use thiserror::Error;
use chrono::{Utc, DateTime, NaiveDateTime, Duration, ParseResult, TimeDelta};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;


type GenericResult<T> = std::result::Result<T, Box<dyn Error>>;
type Result<T> = std::result::Result<T, LogCheckError>;


#[derive(Debug, Clone, Error)]
pub enum PhaseError {
    #[error("Metadata Retreival Error: {0}")]
    MetaDataRetieval(String),
    #[error("Timestamp Discovery Error: {0}")]
    TimeDiscovery(String),
    #[error("Timestamp Order Error: {0}")]
    TimeDirection(String), 
    #[error("File Streaming Error: {0}")]
    FileStreaming(String), 
}// Should prob actually use this for the different stages of processing, Metadata extraction error, File Error, etc

#[derive(Debug, Error)]
#[error("{reason}")]
pub struct LogCheckError {
    pub reason: String,
}


#[derive(PartialEq, Debug)]
pub enum LogType{
    Csv,
    Json,
    Unstructured,
}

#[derive(PartialEq, Debug, Clone)]
pub enum TimeDirection{
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
    pub largest_gap: Option<String>,
    pub largest_gap_duration: Option<String>,
    pub error: Option<String>,
}

#[derive(PartialEq, Debug)]
pub struct LogFileRecord {
    pub hash_of_entire_record: u64,
    pub timestamp:NaiveDateTime,
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

        let gap = end_time
            .signed_duration_since(beginning_time);

        Self {
            gap,
            beginning_time,
            end_time,
        }
    }
}

#[derive(PartialEq, Debug, Default)]
pub struct LogFileStatisticsAndAlerts {
    pub order: Option<TimeDirection>,
    pub min_timestamp: Option<NaiveDateTime>,
    pub max_timestamp: Option<NaiveDateTime>,
    pub previous_timestamp: Option<NaiveDateTime>,
    pub largest_time_gap: Option<TimeGap>, // Eventually maybe make this store the top few?
    pub duplicate_checker_set: HashSet<u64>,
}

impl LogFileStatisticsAndAlerts {
    fn new_with_order(order: Option<TimeDirection>) -> Self {
        Self {
            order,
            ..Default::default()
        }
    }
    pub fn process_record(&mut self, record : LogFileRecord) -> GenericResult<()>{
        //Check for duplicates
        let is_duplicate = !self.duplicate_checker_set.insert(record.hash_of_entire_record);
        if is_duplicate {
            println!("Found duplicate record at index {}", record.index);
        }

        //Update earliest and latest timestamp
        if let Some(previous_datetime) = self.previous_timestamp { // This is where all logic is done if it isn't the first record
            if self.order == Some(TimeDirection::Ascending) {
                if previous_datetime > record.timestamp {
                    return Err("File was not sorted on the identified timestamp".into())
                }
                self.max_timestamp = Some(record.timestamp)
            }
            else if self.order == Some(TimeDirection::Descending) {
                if previous_datetime < record.timestamp {
                    return Err("File was not sorted on the identified timestamp".into())
                }
                self.min_timestamp = Some(record.timestamp)
            }
            let current_time_gap = TimeGap::new(previous_datetime, record.timestamp);
            if let Some(largest_time_gap) = self.largest_time_gap {
                if current_time_gap > largest_time_gap {
                    self.largest_time_gap = Some(TimeGap::new(previous_datetime, record.timestamp));
                }

            } else{ // This is the second row, intialize the time gap 
                self.largest_time_gap = Some(TimeGap::new(previous_datetime, record.timestamp));

            }


        } else { // This is the first row, inialize either the min or max timestamp
            if self.order == Some(TimeDirection::Ascending) {
                self.min_timestamp = Some(record.timestamp)
            }
            else if self.order == Some(TimeDirection::Descending) {
                self.max_timestamp = Some(record.timestamp)
            }
        }
        self.previous_timestamp = Some(record.timestamp);

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StructuredTimeColumnHit { // Maybe add a date format pretty. and then also the date format that gets used by chrono
    pub column_name: String,
    pub column_index: usize,
    pub regex_info: DateRegex,
    pub direction: Option<TimeDirection>,
}

#[derive(Debug, Clone)]
pub struct DateRegex {
    pretty_format: String,
    strftime_format: String,
    regex: Regex,
}

pub static DATE_REGEXES: Lazy<Vec<DateRegex>> = Lazy::new(|| {
    vec![
    DateRegex {
        pretty_format: "MM-DD-YYYY".to_string(),
        regex: Regex::new(r"^\d{2}-\d{2}-\d{4}$").unwrap(),
        strftime_format: "%m-%d-%Y".to_string(),
    },
    DateRegex {
        pretty_format: "YYYY-MM-DD".to_string(),
        regex: Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap(),
        strftime_format: "%Y-%m-%d".to_string(),
    },
    DateRegex {
        pretty_format: "DD-MM-YYYY".to_string(),
        regex: Regex::new(r"^\d{2}-\d{2}-\d{4}$").unwrap(),
        strftime_format: "%d-%m-%Y".to_string(),
    },
    DateRegex {
        pretty_format: "YYYY/MM/DD".to_string(),
        regex: Regex::new(r"^\d{4}/\d{2}/\d{2}$").unwrap(),
        strftime_format: "%Y/%m/%d".to_string(),
    },
    DateRegex {
        pretty_format: "MMM DD YYYY".to_string(), // e.g. Mar 22 2022
        regex: Regex::new(r"^[A-Z][a-z]{2} \d{1,2} \d{4}$").unwrap(),
        strftime_format: "%b %d %Y".to_string(),
    },
    DateRegex {
        pretty_format: "MMMM DD, YYYY".to_string(), // e.g. March 22, 2022
        regex: Regex::new(r"^[A-Z][a-z]+ \d{1,2}, \d{4}$").unwrap(),
        strftime_format: "%B %d, %Y".to_string(),
    },
    DateRegex {
        pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(), // 24-hour datetime
        regex: Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$").unwrap(),
        strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
    },
    DateRegex {
        pretty_format: "YYYY-MM-DDTHH:MM:SSZ".to_string(), // ISO 8601
        regex: Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$").unwrap(),
        strftime_format: "%Y-%m-%dT%H:%M:%SZ".to_string(),
    },
    DateRegex {
        pretty_format: "M/D/YYYY H:MM AM/PM".to_string(), // 12-hour US time
        regex: Regex::new(r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2} (AM|PM|am|pm)$").unwrap(),
        strftime_format: "%-m/%-d/%Y %-I:%M %p".to_string(),
    },
    DateRegex {
        pretty_format: "YYYY-MM-DDTHH:MM:SS.SSS".to_string(),
        regex: Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,3}$").unwrap(),
        strftime_format: "%Y-%m-%dT%H:%M:%S%.3f".to_string(),
    },
]
});

pub fn iterate_through_input_dir(input_dir:String){

    let mut paths: Vec<PathBuf> = Vec::new();

    for entry in glob(input_dir.as_str()).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => paths.push(path),
            Err(e) => println!("{:?}", e),
        }
    }

    let supported_files = categorize_files(&paths);

    let results: Vec<ProcessedLogFile> = supported_files
    .par_iter()
    .map(|path| process_file(path).expect("Error processing file"))
    .collect();

    let _ = write_to_csv(&results).expect("Failed to open output file");
}

fn generate_log_filename() -> String {
    let now = Utc::now();
    let formatted = now.format("%Y-%m-%d_%H-%M-%S_LogCheck_Output.csv");
    formatted.to_string()
}

fn write_to_csv(processed_log_files: &Vec<ProcessedLogFile>) -> GenericResult<()> { // in the final version, maybe have a full version that has tons of fields, and then a simplified version. Could have command line arg to trigger verbose one
    //Add something here to create the 
    let output_filename = generate_log_filename();
    let mut wtr = Writer::from_path(&output_filename)?;
    wtr.write_record(&["Filename", "File Path", "SHA256 Hash", "Size", "Header Used", "Timestamp Format","Earliest Timestamp", "Latest Timestamp","Duration of Largest Time Gap","Largest Time Gap", "Error"])?;
    for log_file in processed_log_files {
        wtr.serialize((
            log_file.filename.as_deref().unwrap_or(""),
            log_file.file_path.as_deref().unwrap_or(""),
            log_file.sha256hash.as_deref().unwrap_or(""),
            log_file.size.unwrap_or(0),
            log_file.time_header.as_deref().unwrap_or(""),
            log_file.time_format.as_deref().unwrap_or(""),
            log_file.min_timestamp.as_deref().unwrap_or(""),
            log_file.max_timestamp.as_deref().unwrap_or(""),
            log_file.largest_gap_duration.as_deref().unwrap_or(""),
            log_file.largest_gap.as_deref().unwrap_or(""),
            log_file.error.as_deref().unwrap_or(""),
        ))?;
    }
    wtr.flush()?; //Is this really needed?
    println!("Data written to {output_filename}");
    Ok(())
}

pub fn categorize_files(file_paths: &Vec<PathBuf>) -> Vec<LogFile>{
    let mut supported_files: Vec<LogFile> = Vec::new();

    for file_path in file_paths{
        if let Some(extension) = file_path.extension() {
            if extension == "csv"{
                supported_files.push(
                    LogFile{
                        log_type:LogType::Csv,
                        file_path:file_path.to_path_buf(),
                    }
                )
            }
        }else{
            println!("Error getting file extension for {}", file_path.to_string_lossy().to_string())
        }
    }
    supported_files
}

fn get_metadata_and_hash(file_path: &PathBuf) -> GenericResult<(String, u64, String, String)> {
    let mut file = File::open(file_path)?;
    let size = file.metadata()?.len();
    let file_name = file_path.file_name().ok_or("Error getting filename")?.to_string_lossy().to_string();

    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 4096];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let result = hasher.finalize();
    let hash_hex = format!("{:x}", result);

    Ok((hash_hex, size, file_name, file_path.to_string_lossy().to_string()))
}

pub fn process_file(log_file: &LogFile) -> GenericResult<ProcessedLogFile>{
    let mut base_processed_file = ProcessedLogFile::default();

    //get hash and metadata. Does not matter what kind of file it is for this function
    let (hash, size, file_name, file_path ) = match get_metadata_and_hash(&log_file.file_path).map_err(|e| PhaseError::MetaDataRetieval(e.to_string())) {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };
    base_processed_file.sha256hash = Some(hash);
    base_processed_file.size = Some(size);
    base_processed_file.filename = Some(file_name);
    base_processed_file.file_path = Some(file_path);


    // get the timestamp field. Will only do this if it is structured (json or csv)
    let mut timestamp_hit = match find_timestamp_field(log_file).map_err(|e| PhaseError::TimeDiscovery(e.to_string())) {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };

    base_processed_file.time_header = Some(timestamp_hit.column_name.clone());
    base_processed_file.time_format = Some(timestamp_hit.regex_info.pretty_format.clone());

    match set_time_direction_by_scanning_file(log_file, &mut timestamp_hit).map_err(|e| PhaseError::TimeDirection(e.to_string())) {
        Ok(_) => {},
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };
    println!(
        "{} appears to be in {:?} order!",
        log_file.file_path.to_string_lossy().to_string(), timestamp_hit.direction.clone().ok_or_else(|| "Index of date field not found")?
    );

    let completed_statistics_object = match stream_csv_file(log_file, timestamp_hit).map_err(|e| PhaseError::FileStreaming(e.to_string())) {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };

    base_processed_file.min_timestamp = Some(
        completed_statistics_object
            .min_timestamp
            .ok_or("No min timestamp found")?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    );
    base_processed_file.max_timestamp = Some(
        completed_statistics_object
            .max_timestamp
            .ok_or("No max timestamp found")?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    );

    let largest_time_gap =     completed_statistics_object
    .largest_time_gap.ok_or("No largest time gap found")?;

    base_processed_file.largest_gap = Some(format!("{} to {}",
    largest_time_gap
    .beginning_time
    .format("%Y-%m-%d %H:%M:%S"),
    largest_time_gap
    .end_time
    .format("%Y-%m-%d %H:%M:%S")),
    );
    base_processed_file.largest_gap_duration = Some(format_timedelta(largest_time_gap.gap));

    Ok(base_processed_file)
}

fn format_timedelta(tdelta: TimeDelta) -> String {
    let total_seconds = tdelta.num_seconds().abs(); // make it positive for display

    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

pub fn find_timestamp_field(log_file: &LogFile) -> GenericResult<StructuredTimeColumnHit> { //This is lazy here
    if log_file.log_type == LogType::Csv {
        let file = File::open(&log_file.file_path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true) // Set to false if there's no header
            .from_reader(file);

        let headers: csv::StringRecord = reader.headers()?.clone(); // this returns a &StringRecord
        let record: csv::StringRecord = reader.records().next().unwrap()?; // This is returning a result, that is why I had to use the question mark below before the iter()
        for (i, field) in record.iter().enumerate() {
            for date_regex in DATE_REGEXES.iter() {
                if date_regex.regex.is_match(field) {
                    println!(
                        "Found match for '{}' time format in the '{}' column of {}",
                        date_regex.pretty_format, headers.get(i).unwrap().to_string(), log_file.file_path.to_string_lossy().to_string()
                    );
                    return Ok(
                        StructuredTimeColumnHit {
                            column_name: headers.get(i).unwrap().to_string(),
                            column_index: i,
                            direction: None,
                            regex_info: date_regex.clone(),
                        }
                    )
                }
            }
        }
    }
    println!("Could not find a supported timestamp in {}", log_file.file_path.to_string_lossy().to_string());
    Err("Could not find a supported timestamp format.".into())
}

pub fn set_time_direction_by_scanning_file(log_file: &LogFile, timestamp_hit: &mut StructuredTimeColumnHit) -> GenericResult<()> {
    let file = File::open(&log_file.file_path)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    let mut previous: Option<NaiveDateTime> = None;
    for result in rdr.records() { // I think I should just include the index in the timestamp hit 
        let record = result?;
        let value = record.get(timestamp_hit.column_index).ok_or_else(|| "Index of date field not found")?;
        let current_datetime: NaiveDateTime = NaiveDateTime::parse_from_str(value, &timestamp_hit.regex_info.strftime_format)?;
        if let Some(previous_datetime) = previous {
            if current_datetime > previous_datetime {
                // println!("Current datetime {} is after previous {}. Order is Ascending!", current_datetime.format("%Y-%m-%d %H:%M:%S").to_string(), previous_datetime.format("%Y-%m-%d %H:%M:%S").to_string());
                timestamp_hit.direction = Some(TimeDirection::Ascending);
                return Ok(())
            } else if current_datetime < previous_datetime {
                // println!("Current datetime {} is before previous {}. Order is Descending!", current_datetime.format("%Y-%m-%d %H:%M:%S").to_string(), previous_datetime.format("%Y-%m-%d %H:%M:%S").to_string());
                timestamp_hit.direction = Some(TimeDirection::Descending);
                return Ok(())
            } else {
                // println!("Lines were equal");
            }
        } else {
            previous = Some(current_datetime);
        }
    }
    Err("Could not determine order, all timestamps may have been equal.".into())
}

fn hash_csv_record(record: &StringRecord) -> u64 {
    let mut hasher = DefaultHasher::new();
    record.iter().for_each(|field| field.hash(&mut hasher));
    hasher.finish()
}

pub fn stream_csv_file(log_file: &LogFile, timestamp_hit: StructuredTimeColumnHit) -> GenericResult<LogFileStatisticsAndAlerts>{ // not sure we want to include the whole hashset in this? Maybe only inlcude results
    let mut processing_object = LogFileStatisticsAndAlerts::new_with_order(timestamp_hit.direction);
    let file = File::open(&log_file.file_path)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    for (index, result) in rdr.records().enumerate() { // I think I should just include the index in the timestamp hit 
        let record = result?;
        let value = record.get(timestamp_hit.column_index).ok_or_else(|| "Index of date field not found")?;
        let current_datetime: NaiveDateTime = NaiveDateTime::parse_from_str(value, &timestamp_hit.regex_info.strftime_format)?;
        let hash_of_record = hash_csv_record(&record);
        processing_object.process_record(LogFileRecord {
            hash_of_entire_record: hash_of_record,
            timestamp: current_datetime,
            index: index,
        })?

    }
    Ok(processing_object)
}
