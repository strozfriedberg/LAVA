use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::*;
use crate::timestamp_tools::*;
use chrono::NaiveDateTime;
use csv::ReaderBuilder;
use csv::Reader;
use std::fs::File;
use crate::date_regex::*;
use std::io::{BufRead, BufReader};


pub fn get_reader_from_certain_header_index(header_index: u16, file: File) -> Result<Reader<BufReader<File>>>{
    let mut buf_reader = BufReader::new(file);
    for _ in 0..header_index {
        let mut dummy = String::new();
        buf_reader.read_line(&mut dummy).map_err(|e| LogCheckError::new(format!("Unable to read file because of {e}")))?;
    }
    let reader = ReaderBuilder::new()
    .has_headers(true) // Set to false if there's no header
    .from_reader(buf_reader);
    Ok(reader)
}

pub fn try_to_get_timestamp_hit_for_csv(log_file: &LogFile, regexes_to_use: &Vec<DateRegex>) -> Result<IdentifiedTimeInformation> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to read csv file because of {e}")))?;

    let mut reader = get_reader_from_certain_header_index(1, file)?;

    let headers: csv::StringRecord = reader
        .headers()
        .map_err(|e| LogCheckError::new(format!("Unable to get headers because of {e}")))?
        .clone(); // this returns a &StringRecord
    let record: csv::StringRecord = reader
        .records()
        .next()
        .unwrap()
        .map_err(|e| LogCheckError::new(format!("Unable to get first row because of {e}")))?; // This is returning a result, that is why I had to use the question mark below before the iter()
    for (i, field) in record.iter().enumerate() {
        for date_regex in regexes_to_use.iter() {
            if date_regex.regex.is_match(field) {
                println!(
                    "Found match for '{}' time format in the '{}' column of {}",
                    date_regex.pretty_format,
                    headers.get(i).unwrap().to_string(),
                    log_file.file_path.to_string_lossy().to_string()
                );
                return Ok(IdentifiedTimeInformation {
                    header_row: None,
                    column_name: Some(headers.get(i).unwrap().to_string()),
                    column_index: Some(i),
                    direction: None,
                    regex_info: date_regex.clone(),
                });
            }
        }
    }
    println!(
        "Could not find a supported timestamp in {}",
        log_file.file_path.to_string_lossy().to_string()
    );
    Err(LogCheckError::new(
        "Could not find a supported timestamp format.",
    ))
}

pub fn set_time_direction_by_scanning_csv_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
) -> Result<()> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open csv file because of {e}")))?;
    let mut rdr = get_reader_from_certain_header_index(1, file)?;
    // let mut previous: Option<NaiveDateTime> = None;
    let mut direction_checker = TimeDirectionChecker::default();
    for result in rdr.records() {
        // I think I should just include the index in the timestamp hit
        let record = result.map_err(|e| {
            LogCheckError::new(format!(
                "Unable to read bytes during hashing because of {e}"
            ))
        })?;
        let value = record
            .get(timestamp_hit.column_index.unwrap())
            .ok_or_else(|| LogCheckError::new("Index of date field not found"))?; // unwrap is safe here because for CSVs, there will always be a column index
        let current_datetime: NaiveDateTime =
            NaiveDateTime::parse_from_str(value, &timestamp_hit.regex_info.strftime_format)
                .map_err(|e| {
                    LogCheckError::new(format!("Issue parsing timestamp because of {e}"))
                })?;
        if let Some(direction) = direction_checker.process_timestamp(current_datetime) {
            timestamp_hit.direction = Some(direction);
            return Ok(());
        }
    }
    Err(LogCheckError::new(
        "Could not determine order, all timestamps may have been equal.",
    ))
}

pub fn stream_csv_file(
    log_file: &LogFile,
    timestamp_hit: &IdentifiedTimeInformation,
) -> Result<LogRecordProcessor> {
    // not sure we want to include the whole hashset in this? Maybe only inlcude results
    let mut processing_object = LogRecordProcessor::new_with_order(timestamp_hit.direction.clone());
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open csv file because of {e}")))?;
    let mut rdr = get_reader_from_certain_header_index(1, file)?;
    for (index, result) in rdr.records().enumerate() {
        // I think I should just include the index in the timestamp hit
        let record = result
            .map_err(|e| LogCheckError::new(format!("Unable to read csv record because of {e}")))?;
        let value = record
            .get(timestamp_hit.column_index.unwrap())
            .ok_or_else(|| LogCheckError::new("Index of date field not found"))?;
        let current_datetime: NaiveDateTime = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_that_is_exact_date(value.to_string())?;
        let hash_of_record = hash_csv_record(&record);
        processing_object.process_record(LogFileRecord {
            hash_of_entire_record: hash_of_record,
            timestamp: current_datetime,
            index: index,
        })?
    }
    Ok(processing_object)
}
