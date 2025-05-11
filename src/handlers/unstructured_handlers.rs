use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::*;
use crate::timestamp_tools::*;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn try_to_get_timestamp_hit_for_unstructured(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
) -> Result<IdentifiedTimeInformation> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to read log file because of {e}")))?;
    let reader = BufReader::new(file);

    for line_result in reader.lines() {
        let line = line_result
            .map_err(|e| LogCheckError::new(format!("Error reading line because of {}", e)))?;
        for date_regex in execution_settings.regexes.iter() {
            if date_regex.string_contains_date(&line) {
                println!(
                    "Found match for '{}' time format in {}",
                    date_regex.pretty_format,
                    log_file.file_path.to_string_lossy().to_string()
                );
                return Ok(IdentifiedTimeInformation {
                    header_row: None,
                    column_name: None,
                    column_index: None,
                    direction: None,
                    regex_info: date_regex.clone(),
                });
            }
        }
    }
    return Err(LogCheckError::new(
        "No regex match found in the log file, try providing your own custom regex",
    ));
}

pub fn set_time_direction_by_scanning_unstructured_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
) -> Result<()> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open the log file because of {e}")))?;
    let reader = BufReader::new(file);
    let mut direction_checker = TimeDirectionChecker::default();
    for line_result in reader.lines() {
        let line = line_result
            .map_err(|e| LogCheckError::new(format!("Error reading line because of {}", e)))?;
        if let Some(current_datetime) = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_contianing_date(line)?
        {
            if let Some(direction) = direction_checker.process_timestamp(current_datetime) {
                timestamp_hit.direction = Some(direction);
                return Ok(());
            }
        };
    }
    Ok(())
}

pub fn stream_unstructured_file(
    log_file: &LogFile,
    timestamp_hit: &IdentifiedTimeInformation,
) -> Result<LogRecordProcessor> {
    let mut processing_object = LogRecordProcessor::new_with_order(timestamp_hit.direction.clone());
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open log file because of {e}")))?;
    let reader = BufReader::new(file);
    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result
            .map_err(|e| LogCheckError::new(format!("Error reading line because of {}", e)))?;
        let hash_of_record = hash_string(&line);
        if let Some(current_datetime) = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_contianing_date(line)?
        {
            processing_object.process_record(LogFileRecord {
                hash_of_entire_record: hash_of_record,
                timestamp: current_datetime,
                index: index,
            })?
        }
    }
    Ok(processing_object)
}
