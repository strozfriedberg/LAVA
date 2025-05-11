use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::*;
use crate::timestamp_tools::*;
use chrono::NaiveDateTime;
use csv::Reader;
use csv::ReaderBuilder;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn get_index_of_header(log_file: &LogFile) -> Result<usize> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to read csv file because of {e}")))?;
    let reader = BufReader::new(file);

    get_index_of_header_functionality(reader)
}

pub fn get_index_of_header_functionality<R: BufRead>(reader: R) -> Result<usize> {
    let mut comma_counts: Vec<(usize, usize)> = Vec::new();

    for (index, line_result) in reader.lines().enumerate().take(7) {
        let line = line_result
            .map_err(|e| LogCheckError::new(format!("Error reading line {}: {}", index, e)))?;
        let count = line.matches(',').count();
        comma_counts.push((index, count));
    }
    // println!("Comma counts {:?}", comma_counts);
    let (_, expected_comma_count) = comma_counts
        .last()
        .ok_or_else(|| LogCheckError::new("Vector of comma counts was empty."))?;
    for (index, comma_count) in comma_counts.iter().rev() {
        if comma_count < expected_comma_count {
            return Ok(index + 1);
        }
    }
    Ok(0)
}

pub fn get_reader_from_certain_header_index(
    header_index: usize,
    log_file: &LogFile,
) -> Result<Reader<BufReader<File>>> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to read csv file because of {e}")))?;
    let mut buf_reader = BufReader::new(file);
    for _ in 0..header_index {
        let mut dummy = String::new();
        buf_reader
            .read_line(&mut dummy)
            .map_err(|e| LogCheckError::new(format!("Unable to read file because of {e}")))?;
    }
    let reader = ReaderBuilder::new()
        .has_headers(true) // Set to false if there's no header
        .from_reader(buf_reader);
    Ok(reader)
}

pub fn try_to_get_timestamp_hit_for_csv(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
) -> Result<IdentifiedTimeInformation> {
    let header_row = get_index_of_header(log_file)?;
    // println!("Using header index {}", header_row);
    let mut reader = get_reader_from_certain_header_index(header_row, log_file)?;
    let headers: csv::StringRecord = reader
        .headers()
        .map_err(|e| LogCheckError::new(format!("Unable to get headers because of {e}")))?
        .clone(); // this returns a &StringRecord

    let record: csv::StringRecord = reader
        .records()
        .next()
        .unwrap()
        .map_err(|e| LogCheckError::new(format!("Unable to get first row because of {e}")))?; // This is returning a result, that is why I had to use the question mark below before the iter()

    let mut response =
        try_to_get_timestamp_hit_for_csv_functionality(headers, record, execution_settings);
    match response {
        Ok(ref mut partial) => {
            partial.header_row = Some(header_row as u64);
            println!(
                "Found match for '{}' time format in the '{}' column of {}",
                partial.regex_info.pretty_format,
                partial
                    .column_name
                    .as_ref()
                    .ok_or_else(|| LogCheckError::new("No column name found."))?,
                log_file.file_path.to_string_lossy().to_string()
            );
        }
        Err(ref _e) => {
            println!(
                "Could not find a supported timestamp in {}",
                log_file.file_path.to_string_lossy().to_string()
            );
        }
    }
    response
}

pub fn try_to_get_timestamp_hit_for_csv_functionality(
    headers: csv::StringRecord,
    record: csv::StringRecord,
    execution_settings: &ExecutionSettings,
) -> Result<IdentifiedTimeInformation> {
    if let Some(field_to_use) = &execution_settings.timestamp_field {
        for (i, field) in headers.iter().enumerate() {
            if field.trim() == field_to_use {
                for date_regex in execution_settings.regexes.iter() {
                    if date_regex.string_contains_date(record.get(i).ok_or_else(|| {
                        LogCheckError::new("Could not get the first field for the selected column.")
                    })?) {
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
        }
        return Err(LogCheckError::new(
            "Could not find the specified column in the header.",
        ));
    } else {
        for (i, field) in record.iter().enumerate() {
            for date_regex in execution_settings.regexes.iter() {
                if date_regex.string_contains_date(field) {
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
    }

    Err(LogCheckError::new("Could not find a supported timestamp."))
}

pub fn set_time_direction_by_scanning_csv_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
) -> Result<()> {
    let header_row = timestamp_hit
        .header_row
        .ok_or_else(|| LogCheckError::new("No header row found."))?;
    let mut rdr = get_reader_from_certain_header_index(header_row as usize, log_file)?;
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

        let current_datetime: NaiveDateTime = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_contianing_date(value.to_string())?
            .ok_or_else(|| LogCheckError::new("No timestamp found when scanning for direction."))?;

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

    let header_row = timestamp_hit
        .header_row
        .ok_or_else(|| LogCheckError::new("No header row found."))?;
    let mut rdr = get_reader_from_certain_header_index(header_row as usize, log_file)?;
    for (index, result) in rdr.records().enumerate() {
        // I think I should just include the index in the timestamp hit
        let record = result
            .map_err(|e| LogCheckError::new(format!("Unable to read csv record because of {e}")))?;
        let value = record
            .get(timestamp_hit.column_index.unwrap())
            .ok_or_else(|| LogCheckError::new("Index of date field not found"))?;
        let current_datetime: NaiveDateTime = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_contianing_date(value.to_string())?
            .ok_or_else(|| {
                LogCheckError::new(format!(
                    "No supported timestamp found timestamp column at index {}",
                    index
                ))
            })?;
        let hash_of_record = hash_csv_record(&record);
        processing_object.process_record(LogFileRecord {
            hash_of_entire_record: hash_of_record,
            timestamp: current_datetime,
            index: index,
        })?
    }
    Ok(processing_object)
}
