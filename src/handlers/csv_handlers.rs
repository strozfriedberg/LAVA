use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::{get_file_stem, print_if_verbose_mode_on};
use crate::processing_objects::*;
use chrono::NaiveDateTime;
use csv::Reader;
use csv::ReaderBuilder;
use csv::StringRecord;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
#[cfg(test)]
mod csv_handler_tests;

pub fn get_header_info(log_file: &LogFile) -> Result<HeaderInfo> {
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to read csv file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let mut reader = BufReader::new(file);

    get_header_info_functionality(&mut reader)
}

pub fn get_header_info_functionality<R: BufRead + Seek>(reader: &mut R) -> Result<HeaderInfo> {
    let header_row = get_index_of_header(reader)?;
    reader.seek(SeekFrom::Start(0)).map_err(|e| {
        LavaError::new(
            format!("Could not seek back to file header because of {}", e),
            LavaErrorLevel::Critical,
        )
    })?;

    let mut csv_reader = csv::ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_reader(reader);

    let record = csv_reader
        .records()
        .nth(header_row)
        .ok_or_else(|| LavaError::new("No input parameter found.", LavaErrorLevel::Critical))?
        .map_err(|e| {
            LavaError::new(
                format!("Failed to parse CSV record at header row because of {}", e),
                LavaErrorLevel::Critical,
            )
        })?;
    Ok(HeaderInfo {
        first_data_row: header_row + 1,
        headers: record,
    })
}

pub fn get_index_of_header<R: BufRead>(reader:&mut R) -> Result<usize> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .has_headers(false) 
        .from_reader(reader);

    let mut field_counts: Vec<(usize, usize)> = Vec::new();

    for (index, result) in rdr.records().enumerate().take(7) {
        let record: StringRecord = result.map_err(|e| {
            LavaError::new(
                format!("Error reading record {}: {}", index, e),
                LavaErrorLevel::Critical,
            )
        })?;
        field_counts.push((index, record.len()));
    }
    println!("{:?}", field_counts);

    let (_, expected_field_count) = field_counts.last().ok_or_else(|| {
        LavaError::new(
            "No records found in first 7 lines.",
            LavaErrorLevel::Critical,
        )
    })?;

    for (index, field_count) in field_counts.iter().rev() {
        if field_count < expected_field_count {
            return Ok(index + 1);
        }
    }
    Ok(0)
}

pub fn get_reader_from_certain_index(
    header_index: usize,
    log_file: &LogFile,
) -> Result<Reader<BufReader<File>>> {
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to read csv file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let mut buf_reader = BufReader::new(file);
    for _ in 0..header_index {
        let mut dummy = String::new();
        buf_reader.read_line(&mut dummy).map_err(|e| {
            LavaError::new(
                format!("Unable to read file because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?;
    }
    let reader = ReaderBuilder::new()
        .has_headers(false) // Set to false if there's no header
        .flexible(true)
        .from_reader(buf_reader);
    Ok(reader)
}

pub fn try_to_get_timestamp_hit_for_csv(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
    header_info: HeaderInfo,
) -> Result<Option<IdentifiedTimeInformation>> {
    print_if_verbose_mode_on(format!("Trying to get hit for {:?}", log_file.file_path));
    let mut reader = get_reader_from_certain_index(header_info.first_data_row, log_file)?;

    let record: csv::StringRecord = reader
        .records()
        .next()
        .ok_or_else(|| LavaError::new("Empty CSV file.", LavaErrorLevel::Critical))?
        .map_err(|e| {
            LavaError::new(
                format!("Unable to get first row because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?; // This is returning a result, that is why I had to use the question mark below before the iter()

    let response = try_to_get_timestamp_hit_for_csv_functionality(
        header_info.headers,
        record,
        execution_settings,
    )?;
    Ok(response)
}

pub fn try_to_get_timestamp_hit_for_csv_functionality(
    headers: csv::StringRecord,
    record: csv::StringRecord,
    execution_settings: &ExecutionSettings,
) -> Result<Option<IdentifiedTimeInformation>> {
    if let Some(field_to_use) = &execution_settings.timestamp_field {
        for (i, field) in headers.iter().enumerate() {
            if field.trim() == field_to_use {
                for date_regex in execution_settings.regexes.iter() {
                    if date_regex.string_contains_date(record.get(i).ok_or_else(|| {
                        LavaError::new(
                            "Could not get the first field for the selected column.",
                            LavaErrorLevel::Critical,
                        )
                    })?) {
                        return Ok(Some(IdentifiedTimeInformation {
                            column_name: Some(headers.get(i).unwrap().to_string()),
                            column_index: Some(i),
                            direction: None,
                            regex_info: date_regex.clone(),
                        }));
                    }
                }
            }
        }
    } else {
        for (i, field) in record.iter().enumerate() {
            for date_regex in execution_settings.regexes.iter() {
                if date_regex.string_contains_date(field) {
                    return Ok(Some(IdentifiedTimeInformation {
                        column_name: Some(headers.get(i).unwrap().to_string()),
                        column_index: Some(i),
                        direction: None,
                        regex_info: date_regex.clone(),
                    }));
                }
            }
        }
    }
    Ok(None)
}

pub fn set_time_direction_by_scanning_csv_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
    header_info: HeaderInfo,
) -> Result<()> {
    let mut rdr = get_reader_from_certain_index(header_info.first_data_row, log_file)?;
    let mut direction_checker = TimeDirectionChecker::default();
    for result in rdr.records() {
        // I think I should just include the index in the timestamp hit
        let record = result.map_err(|e| {
            LavaError::new(
                format!("Unable to read bytes during hashing because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?;
        let value = record
            .get(timestamp_hit.column_index.unwrap())
            .ok_or_else(|| {
                LavaError::new("Index of date field not found", LavaErrorLevel::Critical)
            })?; // unwrap is safe here because for CSVs, there will always be a column index

        let current_datetime: NaiveDateTime = timestamp_hit
            .regex_info
            .get_timestamp_object_from_string_contianing_date(value.to_string())?
            .ok_or_else(|| {
                LavaError::new(
                    "No timestamp found when scanning for direction.",
                    LavaErrorLevel::Critical,
                )
            })?;

        if let Some(direction) = direction_checker.process_timestamp(current_datetime) {
            timestamp_hit.direction = Some(direction);
            return Ok(());
        }
    }
    Ok(())
}

pub fn stream_csv_file(
    log_file: &LogFile,
    timestamp_hit: &Option<IdentifiedTimeInformation>,
    execution_settings: &ExecutionSettings,
    header_info: HeaderInfo,
) -> Result<LogRecordProcessor> {
    // not sure we want to include the whole hashset in this? Maybe only inlcude results
    let mut processing_object = LogRecordProcessor::new(
        timestamp_hit,
        execution_settings,
        get_file_stem(log_file)?,
        Some(header_info.headers),
    );

    let mut rdr = get_reader_from_certain_index(header_info.first_data_row, log_file)?;
    for (index, result) in rdr.records().enumerate() {
        // I think I should just include the index in the timestamp hit
        let record = result.map_err(|e| {
            LavaError::new(
                format!("Unable to read csv record because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?;
        let current_datetime = match timestamp_hit {
            None => None,
            Some(timestamp_hit) => {
                let value = record
                    .get(timestamp_hit.column_index.unwrap())
                    .ok_or_else(|| {
                        LavaError::new("Index of date field not found", LavaErrorLevel::Critical)
                    })?;
                timestamp_hit
                    .regex_info
                    .get_timestamp_object_from_string_contianing_date(value.to_string())?
            }
        };

        processing_object.process_record(LogFileRecord::new(index, current_datetime, record))?
    }
    Ok(processing_object)
}
