use std::fs::File;
use std::io::{BufRead, BufReader};
use crate::date_regex::*;
use crate::errors::*;
use crate::basic_objects::*;
use crate::timestamp_tools::*;
use crate::helpers::*;

pub fn try_to_get_timestamp_hit_for_unstructured(
    log_file: &LogFile,
) -> Result<IdentifiedTimeInformation> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to read log file because of {e}")))?;
    let reader = BufReader::new(file);

    if let Some(line_result) = reader.lines().next() {
        let line = line_result
            .map_err(|e| LogCheckError::new(format!("Unable to read log record because of {e}")))?;
        for date_regex in DATE_REGEXES.iter() {
            if date_regex.regex.is_match(&line) {
                println!(
                    "Found match for '{}' time format in {}",
                    date_regex.pretty_format,
                    log_file.file_path.to_string_lossy().to_string()
                );
                return Ok(IdentifiedTimeInformation {
                    column_name: None,
                    column_index: None,
                    direction: None,
                    regex_info: date_regex.clone(),
                });
            }
        }
        return Err(LogCheckError::new(
            "No regex match found in the log file, try providing your own custom regex",
        ));
    } else {
        return Err(LogCheckError::new("No lines in the log file."));
    }
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
        let current_datetime = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_contianing_date(line)?;
        if let Some(direction) = direction_checker.process_timestamp(current_datetime) {
            timestamp_hit.direction = Some(direction);
            return Ok(());
        }
    }
    Ok(())
}

pub fn stream_unstructured_file(
    log_file: &LogFile,
    timestamp_hit: &IdentifiedTimeInformation,
) -> Result<LogFileStatisticsAndAlerts> {
    let mut processing_object =
        LogFileStatisticsAndAlerts::new_with_order(timestamp_hit.direction.clone());
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open log file because of {e}")))?;
    let reader = BufReader::new(file);
    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result
            .map_err(|e| LogCheckError::new(format!("Error reading line because of {}", e)))?;
        let hash_of_record = hash_string(&line);
        let current_datetime = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_contianing_date(line)?;
        processing_object.process_record(LogFileRecord {
            hash_of_entire_record: hash_of_record,
            timestamp: current_datetime,
            index: index,
        })?
    }
    Ok(processing_object)
}
