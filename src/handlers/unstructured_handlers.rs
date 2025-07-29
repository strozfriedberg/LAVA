use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::get_file_stem;
use crate::processing_objects::*;
use csv::StringRecord;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn try_to_get_timestamp_hit_for_unstructured(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
) -> Result<Option<IdentifiedTimeInformation>> {
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to read log file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let reader = BufReader::new(file);

    for line_result in reader.lines() {
        let line = line_result.map_err(|e| {
            LavaError::new(
                format!("Error reading line because of {} at index 0", e),
                LavaErrorLevel::Critical,
            )
        })?;
        for date_regex in execution_settings.regexes.iter() {
            if date_regex.string_contains_date(&line) {
                return Ok(Some(IdentifiedTimeInformation {
                    column_name: None,
                    column_index: None,
                    direction: None,
                    regex_info: date_regex.clone(),
                }));
            }
        }
    }
    return Ok(None);
}

pub fn set_time_direction_by_scanning_unstructured_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
) -> Result<()> {
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to open the log file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let reader = BufReader::new(file);
    let mut direction_checker = TimeDirectionChecker::default();
    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| {
            LavaError::new(
                format!("Error reading line because of {} at index {}", e, index),
                LavaErrorLevel::Critical,
            )
        })?;
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
    timestamp_hit: &Option<IdentifiedTimeInformation>,
    execution_settings: &ExecutionSettings,
) -> Result<LogRecordProcessor> {
    let mut processing_object = LogRecordProcessor::new(
        timestamp_hit,
        execution_settings,
        get_file_stem(log_file)?,
        None,
    );
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to open log file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let reader = BufReader::new(file);
    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| {
            LavaError::new(
                format!("Error reading line because of {} at index {}", e, index),
                LavaErrorLevel::Critical,
            )
        })?;
        let current_datetime = match timestamp_hit {
            None => None,
            Some(timestamp_hit) => timestamp_hit
                .regex_info
                .get_timestamp_object_from_string_contianing_date(line.clone())?,
        };
        processing_object.process_record(LogFileRecord::new(
            index,
            current_datetime,
            StringRecord::from(vec![line]),
        ))?;
    }
    Ok(processing_object)
}
