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
pub mod date_regex;
pub mod helpers;
use helpers::*;
pub mod basic_objects;
use basic_objects::*;
pub mod timestamp_tools;
use timestamp_tools::*;
include!(concat!(env!("OUT_DIR"), "/generated_regexes.rs"));

#[cfg(test)]
include!(concat!(env!("OUT_DIR"), "/generated_tests.rs"));

pub fn process_all_files(execution_settings: ExecutionSettings) {
    let mut paths: Vec<PathBuf> = Vec::new();
    let pattern = format!("{}/**/*", execution_settings.input_dir.to_string_lossy());
    for entry in glob(&pattern).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => {
                let metadata = std::fs::metadata(&path)
                    .map_err(|e| {
                        LogCheckError::new(format!("Failed to read metadata of file becase of {e}"))
                    })
                    .unwrap();
                if metadata.is_file() {
                    paths.push(path);
                }
            }
            Err(e) => println!("{:?}", e),
        }
    }

    let supported_files = categorize_files(&paths);

    let results: Vec<ProcessedLogFile> = supported_files
        .par_iter()
        .map(|path| process_file(path, &execution_settings).expect("Error processing file"))
        .collect();

    if let Err(e) = write_output_to_csv(&results, &execution_settings) {
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

pub fn process_file(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
) -> Result<ProcessedLogFile> {
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
    let mut timestamp_hit = match try_to_get_timestamp_hit(log_file, execution_settings)
        .map_err(|e| PhaseError::TimeDiscovery(e.to_string()))
    {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };
    base_processed_file.header_index_used = timestamp_hit.header_row.clone().map(|n| n.to_string());
    base_processed_file.time_header = timestamp_hit.column_name.clone();
    base_processed_file.time_format = Some(timestamp_hit.regex_info.pretty_format.clone());

    // Get the direction of time in the file
    match set_time_direction_by_scanning_file(log_file, &mut timestamp_hit)
        .map_err(|e| PhaseError::TimeDirection(e.to_string()))
    {
        Ok(_) => {}
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };

    // Stream the file to find statistics on time and other stuff
    let completed_statistics_object =
        match stream_file(log_file, &timestamp_hit, execution_settings)
            .map_err(|e| PhaseError::FileStreaming(e.to_string()))
        {
            Ok(result) => result,
            Err(e) => {
                base_processed_file.error = Some(e.to_string());
                return Ok(base_processed_file);
            }
        };

    // Get the formatted stats from the stats object
    let formatted_statistics = match completed_statistics_object
        .get_statistics()
        .map_err(|e| PhaseError::Formatting(e.to_string()))
    {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.error = Some(e.to_string());
            return Ok(base_processed_file);
        }
    };
    base_processed_file.largest_gap = formatted_statistics.largest_gap;
    base_processed_file.largest_gap_duration = formatted_statistics.largest_gap_duration;
    base_processed_file.min_timestamp = formatted_statistics.min_timestamp;
    base_processed_file.max_timestamp = formatted_statistics.max_timestamp;
    base_processed_file.min_max_duration = formatted_statistics.min_max_duration;
    base_processed_file.num_records = formatted_statistics.num_records;
    base_processed_file.num_dupes = formatted_statistics.num_dupes;
    base_processed_file.num_redactions = formatted_statistics.num_redactions;

    Ok(base_processed_file)
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

pub fn try_to_get_timestamp_hit(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
) -> Result<IdentifiedTimeInformation> {
    if log_file.log_type == LogType::Csv {
        return try_to_get_timestamp_hit_for_csv(log_file, execution_settings);
    } else if log_file.log_type == LogType::Unstructured {
        return try_to_get_timestamp_hit_for_unstructured(log_file, execution_settings);
    }
    Err(LogCheckError::new(
        "Have not implemented scanning for timestamp for this file type yet",
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
    execution_settings: &ExecutionSettings,
) -> Result<LogRecordProcessor> {
    if log_file.log_type == LogType::Csv {
        return stream_csv_file(log_file, timestamp_hit, execution_settings);
    }
    if log_file.log_type == LogType::Unstructured {
        return stream_unstructured_file(log_file, timestamp_hit, execution_settings);
    }
    Err(LogCheckError::new(
        "Have not implemented streaming for this file type yet",
    ))
}
