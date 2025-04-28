use chrono::NaiveDateTime;
use csv::{ReaderBuilder, StringRecord, Writer};
use glob::glob;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
mod errors;
use errors::*;
mod csv_log;
mod date_regex;
mod helpers;
use date_regex::*;
mod basic_objects;
use basic_objects::*;
mod timestamp_tools;
use timestamp_tools::*;

pub fn iterate_through_input_dir(input_dir: String) {
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

    if let Err(e) = write_to_csv(&results) {
        eprintln!("Failed to write to CSV: {}", e);
    }
}

fn write_to_csv(processed_log_files: &Vec<ProcessedLogFile>) -> Result<()> {
    // in the final version, maybe have a full version that has tons of fields, and then a simplified version. Could have command line arg to trigger verbose one
    //Add something here to create the
    let output_filename = helpers::generate_log_filename();
    let mut wtr = Writer::from_path(&output_filename)
        .map_err(|e| LogCheckError::new(format!("Unable to open ouptut file because of {e}")))?;
    wtr.write_record(&[
        "Filename",
        "File Path",
        "SHA256 Hash",
        "Size",
        "Header Used",
        "Timestamp Format",
        "Earliest Timestamp",
        "Latest Timestamp",
        "Duration of Largest Time Gap",
        "Largest Time Gap",
        "Error",
    ])
    .map_err(|e| LogCheckError::new(format!("Unable to write headers because of {e}")))?;
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
        ))
        .map_err(|e| {
            LogCheckError::new(format!("Issue writing lines of output file because of {e}"))
        })?;
    }
    wtr.flush().map_err(|e| {
        LogCheckError::new(format!("Issue flushing to the ouptut file because of {e}"))
    })?; //Is this really needed?
    println!("Data written to {output_filename}");
    Ok(())
}

pub fn categorize_files(file_paths: &Vec<PathBuf>) -> Vec<LogFile> {
    let mut supported_files: Vec<LogFile> = Vec::new();

    for file_path in file_paths {
        if let Some(extension) = file_path.extension() {
            if extension == "csv" {
                supported_files.push(LogFile {
                    log_type: LogType::Csv,
                    file_path: file_path.to_path_buf(),
                })
            } else if extension == "json" {
                supported_files.push(LogFile {
                    log_type: LogType::Json,
                    file_path: file_path.to_path_buf(),
                })
            } else {
                supported_files.push(LogFile {
                    log_type: LogType::Unstructured,
                    file_path: file_path.to_path_buf(),
                })
            }
        } else {
            // Some unstructured logs might not have file extensions, so might have to work with this
            println!(
                "Error getting file extension for {}",
                file_path.to_string_lossy().to_string()
            )
        }
    }
    supported_files
}

fn get_metadata_and_hash(file_path: &PathBuf) -> Result<(String, u64, String, String)> {
    let mut file = File::open(file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open file because of {e}")))?;
    let size = file
        .metadata()
        .map_err(|e| LogCheckError::new(format!("Unable to get file metadata because of {e}")))?
        .len();
    let file_name = file_path
        .file_name()
        .ok_or("Error getting filename")
        .map_err(|e| LogCheckError::new(format!("Unable to open file because of {e}")))?
        .to_string_lossy()
        .to_string();

    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 4096];
    loop {
        let bytes_read = file.read(&mut buffer).map_err(|e| {
            LogCheckError::new(format!(
                "Unable to read bytes during hashing because of {e}"
            ))
        })?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let result = hasher.finalize();
    let hash_hex = format!("{:x}", result);

    Ok((
        hash_hex,
        size,
        file_name,
        file_path.to_string_lossy().to_string(),
    ))
}

pub fn process_file(log_file: &LogFile) -> Result<ProcessedLogFile> {
    let mut base_processed_file = ProcessedLogFile::default();

    //get hash and metadata. Does not matter what kind of file it is for this function
    let (hash, size, file_name, file_path) = match get_metadata_and_hash(&log_file.file_path)
        .map_err(|e| PhaseError::MetaDataRetieval(e.to_string()))
    {
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

    // get the timestamp field. will do this for all of them, but there will just be some fields that only get filled in for structured datatypes
    let mut timestamp_hit = match try_to_get_timestamp_hit(log_file)
        .map_err(|e| PhaseError::TimeDiscovery(e.to_string()))
    {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };

    base_processed_file.time_header = timestamp_hit.column_name.clone();
    base_processed_file.time_format = Some(timestamp_hit.regex_info.pretty_format.clone());

    match set_time_direction_by_scanning_file(log_file, &mut timestamp_hit)
        .map_err(|e| PhaseError::TimeDirection(e.to_string()))
    {
        Ok(_) => {}
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };
    let direction = timestamp_hit
        .direction
        .clone()
        .ok_or_else(|| LogCheckError::new("Index of date field not found"))?;
    println!(
        "{} appears to be in {:?} order!",
        log_file.file_path.to_string_lossy(),
        direction
    );
    let completed_statistics_object = match stream_file(log_file, &timestamp_hit)
        .map_err(|e| PhaseError::FileStreaming(e.to_string()))
    {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };

    base_processed_file.min_timestamp = Some(
        completed_statistics_object
            .min_timestamp
            .ok_or_else(|| LogCheckError::new("No min timestamp found"))?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
    );
    base_processed_file.max_timestamp = Some(
        completed_statistics_object
            .max_timestamp
            .ok_or_else(|| LogCheckError::new("No min timestamp found"))?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
    );

    let largest_time_gap = completed_statistics_object
        .largest_time_gap
        .ok_or_else(|| LogCheckError::new("No min timestamp found"))?;

    base_processed_file.largest_gap = Some(format!(
        "{} to {}",
        largest_time_gap.beginning_time.format("%Y-%m-%d %H:%M:%S"),
        largest_time_gap.end_time.format("%Y-%m-%d %H:%M:%S")
    ));
    base_processed_file.largest_gap_duration =
        Some(helpers::format_timedelta(largest_time_gap.gap));

    Ok(base_processed_file)
}

pub fn try_to_get_timestamp_hit(log_file: &LogFile) -> Result<IdentifiedTimeInformation> {
    if log_file.log_type == LogType::Csv {
        return csv_log::try_to_get_timestamp_hit_for_csv(log_file);
    } else if log_file.log_type == LogType::Unstructured {
        return try_to_get_timestamp_hit_for_unstructured(log_file);
    }
    Err(LogCheckError::new(
        "Have not implemented scanning for timestam for this file type yet",
    ))
}

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

pub fn set_time_direction_by_scanning_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
) -> Result<()> {
    if log_file.log_type == LogType::Csv {
        return set_time_direction_by_scanning_csv_file(log_file, timestamp_hit);
    }
    if log_file.log_type == LogType::Unstructured {
        return set_time_direction_by_scanning_unstructured_file(log_file, timestamp_hit);
    }
    Err(LogCheckError::new(
        "Have not implemented scanning for directions for this file type yet.",
    ))
}

pub fn set_time_direction_by_scanning_csv_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
) -> Result<()> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open csv file because of {e}")))?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
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

fn hash_csv_record(record: &StringRecord) -> u64 {
    let mut hasher = DefaultHasher::new();
    record.iter().for_each(|field| field.hash(&mut hasher));
    hasher.finish()
}

fn hash_string(input: &String) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher); // Hash the string (dereferenced automatically to &str)
    hasher.finish() // Return the resulting hash
}

pub fn stream_file(
    log_file: &LogFile,
    timestamp_hit: &IdentifiedTimeInformation,
) -> Result<LogFileStatisticsAndAlerts> {
    if log_file.log_type == LogType::Csv {
        return stream_csv_file(log_file, timestamp_hit);
    }
    if log_file.log_type == LogType::Unstructured {
        return stream_unstructured_file(log_file, timestamp_hit);
    }
    Err(LogCheckError::new(
        "Have not implemented streaming for this file type yet",
    ))
}

pub fn stream_csv_file(
    log_file: &LogFile,
    timestamp_hit: &IdentifiedTimeInformation,
) -> Result<LogFileStatisticsAndAlerts> {
    // not sure we want to include the whole hashset in this? Maybe only inlcude results
    let mut processing_object =
        LogFileStatisticsAndAlerts::new_with_order(timestamp_hit.direction.clone());
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to open csv file because of {e}")))?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
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
