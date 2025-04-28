use csv::Writer;
use glob::glob;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
pub mod errors;
use errors::*;
pub mod handlers {
    pub mod csv_handlers;
    pub mod unstructured_handlers;
}
use handlers::csv_handlers::*;
use handlers::unstructured_handlers::*;
mod date_regex;
mod helpers;
pub mod basic_objects;
use basic_objects::*;
mod timestamp_tools;
use timestamp_tools::*;

pub fn process_all_files(input_dir: String) {
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

    if let Err(e) = write_output_to_csv(&results) {
        eprintln!("Failed to write to CSV: {}", e);
    }
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

fn write_output_to_csv(processed_log_files: &Vec<ProcessedLogFile>) -> Result<()> {
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
        return try_to_get_timestamp_hit_for_csv(log_file);
    } else if log_file.log_type == LogType::Unstructured {
        return try_to_get_timestamp_hit_for_unstructured(log_file);
    }
    Err(LogCheckError::new(
        "Have not implemented scanning for timestam for this file type yet",
    ))
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

pub fn stream_file(
    log_file: &LogFile,
    timestamp_hit: &IdentifiedTimeInformation,
) -> Result<LogRecordProcessor> {
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
