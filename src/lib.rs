use glob::glob;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::fs::{File, metadata};
use std::io::Read;
use std::path::PathBuf;
mod errors;
use errors::*;
mod handlers {
    pub mod csv_handlers;
    pub mod json_handlers;
    pub mod unstructured_handlers;
}
use handlers::csv_handlers::*;
use handlers::json_handlers::*;
use handlers::unstructured_handlers::*;
use num_format::{Locale, ToFormattedString};
mod date_regex;
pub mod helpers;
use helpers::*;
pub mod basic_objects;
use basic_objects::*;
mod processing_objects;
use processing_objects::*;
pub mod alerts;
pub mod main_helpers;
mod redaction_regex;
use alerts::generate_alerts;
use once_cell::sync::OnceCell;
use std::time::Instant;
include!(concat!(env!("OUT_DIR"), "/generated_date_regexes.rs"));

static VERBOSE: OnceCell<bool> = OnceCell::new();

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
include!(concat!(env!("OUT_DIR"), "/generated_date_tests.rs"));

#[cfg(test)]
include!(concat!(env!("OUT_DIR"), "/generated_redactions_tests.rs"));

pub fn process_all_files(execution_settings: ExecutionSettings) {
    let start = Instant::now();
    let _ = VERBOSE.set(execution_settings.verbose_mode);
    match metadata(&execution_settings.input) {
        Err(e) => println!(
            "Could not get the metadata of the input path because of {}",
            e
        ),
        Ok(metadata) => {
            let supported_files = match metadata.is_file() {
                false => {
                    // input is a directory
                    let mut paths: Vec<PathBuf> = Vec::new();
                    println!(
                        "Starting to enumerate log files in {:?}",
                        execution_settings.input
                    );
                    let pattern = format!("{}/**/*", execution_settings.input.to_string_lossy());
                    for entry in glob(&pattern).expect("Failed to read glob pattern") {
                        match entry {
                            Ok(path) => {
                                let metadata = std::fs::metadata(&path)
                                    .map_err(|e| {
                                        LavaError::new(
                                            format!(
                                                "Failed to read metadata of file becase of {e}"
                                            ),
                                            LavaErrorLevel::Critical,
                                        )
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
                    println!(
                        "Found {} supported log files. Starting to process now.",
                        supported_files.len()
                    );
                    supported_files
                }
                true => {
                    println!("Starting to process {:?}", execution_settings.input);
                    let supported_files = categorize_files(&vec![execution_settings.input.clone()]);
                    supported_files
                }
            };

            let results: Vec<ProcessedLogFile> = supported_files
                .par_iter()
                .map(|path| process_file(path, &execution_settings).expect("Error processing file"))
                .collect();
            // Make a line here to go through each ProcessedLogFile, and write that to the error log
            if let Err(e) = write_errors_to_error_log(&results, &execution_settings) {
                eprintln!("Failed to write errors to error log {}", e);
            }
            if let Err(e) = write_output_to_csv(&results, &execution_settings) {
                eprintln!("Failed to write to CSV: {}", e);
            }
            if let Err(e) = print_pretty_quick_stats(&results) {
                eprintln!("Failed to print pretty quick stats {}", e);
            }
            if let Err(e) =
                print_pretty_alerts_and_write_to_output_file(&results, &execution_settings)
            {
                eprintln!("Failed to output alerts: {}", e);
            }

            let formatted_total_of_records_with_timestamps = results
                .iter()
                .map(|f| f.timestamp_num_records)
                .sum::<usize>()
                .to_formatted_string(&Locale::en);
            let num_records_processed_for_timestamp_analysis = results.iter().filter(|item| item.largest_gap.is_some()).count();
            
            let duration = start.elapsed();
            let minutes = duration.as_secs_f64() / 60.0;
            println!("Finished in {:.2} minutes", minutes);
            println!(
                "Processed a total of {} records with timestamps across {} log files",
                formatted_total_of_records_with_timestamps,
                num_records_processed_for_timestamp_analysis.to_formatted_string(&Locale::en)
            );
            if num_records_processed_for_timestamp_analysis < results.len(){
                println!("\x1b[31m{} log files could not be processed for timestamp analysis. Check LAVA_Errors.log for reason\x1b[0m", (results.len() - num_records_processed_for_timestamp_analysis).to_formatted_string(&Locale::en));
            }
        }
    };
}

fn categorize_files(file_paths: &Vec<PathBuf>) -> Vec<LogFile> {
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

    //get metadata. Does not matter what kind of file it is for this function
    let (size, file_name, file_path) = match get_metadata(&log_file.file_path) {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.errors.push(e);
            return Ok(base_processed_file);
        }
    };
    base_processed_file.size = Some(size.to_string());
    base_processed_file.filename = Some(file_name);
    base_processed_file.file_path = Some(file_path);

    //Get hash if not quick mode
    if !execution_settings.quick_mode {
        let hash = match get_hash(&log_file.file_path) {
            Ok(result) => result,
            Err(e) => {
                base_processed_file.errors.push(e);
                return Ok(base_processed_file);
            }
        };
        base_processed_file.sha256hash = Some(hash);
    }

    // Get Header Row
    let header_info = match get_header_info(log_file) {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.errors.push(e);
            return Ok(base_processed_file);
        }
    };

    // get the timestamp field, if it doesn't find one the file will still be processed for dupes and redactions
    let potential_timestamp_hit =
        match try_to_get_timestamp_hit(log_file, execution_settings, header_info.clone()) {
            Ok(Some(mut timestamp_hit)) => {
                match timestamp_hit.column_name.as_ref() {
                    None => {
                        println!(
                            "Found match for '{}' time format in {}",
                            timestamp_hit.regex_info.pretty_format,
                            log_file.file_path.to_string_lossy().to_string()
                        );
                    }
                    Some(column_name) => {
                        println!(
                            "Found match for '{}' time format in the '{}' column of {}",
                            timestamp_hit.regex_info.pretty_format,
                            column_name,
                            log_file.file_path.to_string_lossy().to_string()
                        );
                    }
                }
                base_processed_file.time_header = timestamp_hit.column_name.clone();
                base_processed_file.time_format =
                    Some(timestamp_hit.regex_info.pretty_format.clone());

                if let Err(e) = set_time_direction_by_scanning_file(
                    log_file,
                    &mut timestamp_hit,
                    header_info.clone(),
                ) {
                    base_processed_file.errors.push(e);
                    return Ok(base_processed_file);
                }

                Some(timestamp_hit)
            }
            Ok(None) => {
                base_processed_file.errors.push(LavaError::new(
                    "Could not find a supported timestamp, try providing your own custom regex.",
                    LavaErrorLevel::Medium,
                ));
                None
            }
            Err(e) => {
                base_processed_file.errors.push(e);
                return Ok(base_processed_file);
            }
        };

    // Stream the file to find statistics on time and other stuff
    let completed_statistics_object = match stream_file(
        log_file,
        &potential_timestamp_hit,
        execution_settings,
        header_info.clone(),
    ) {
        Ok(result) => result,
        Err(e) => {
            base_processed_file.errors.push(e);
            return Ok(base_processed_file);
        }
    };
    base_processed_file.first_data_row_used = header_info.map(|n| n.first_data_row.to_string());
    let values_to_alert_on = completed_statistics_object.get_possible_alert_values();
    let alerts = generate_alerts(values_to_alert_on);
    base_processed_file.alerts = Some(alerts);


    base_processed_file.largest_gap = completed_statistics_object.largest_time_gap;
    base_processed_file.min_timestamp = completed_statistics_object.min_timestamp;
    base_processed_file.max_timestamp = completed_statistics_object.max_timestamp;
    if completed_statistics_object.largest_time_gap.is_some() {
        let (mean_time_gap, std_dev_time_gap) =
            completed_statistics_object.get_mean_and_standard_deviation();
        base_processed_file.mean_time_gap = Some(mean_time_gap);
        base_processed_file.std_dev_time_gap = Some(std_dev_time_gap);
    }

    base_processed_file.total_num_records = completed_statistics_object.total_num_records;
    base_processed_file.timestamp_num_records = completed_statistics_object.timestamp_num_records;

    if !execution_settings.quick_mode {
        base_processed_file.num_dupes = Some(completed_statistics_object.num_dupes);
        base_processed_file.num_redactions = Some(completed_statistics_object.num_redactions);
    }

    base_processed_file
        .errors
        .extend(completed_statistics_object.errors);

    Ok(base_processed_file)
}

fn get_metadata(file_path: &PathBuf) -> Result<(u64, String, String)> {
    let file = File::open(file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to open file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let size = file
        .metadata()
        .map_err(|e| {
            LavaError::new(
                format!("Unable to get file metadata because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?
        .len();
    let file_name = file_path
        .file_name()
        .ok_or("Error getting filename")
        .map_err(|e| {
            LavaError::new(
                format!("Unable to open file because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?
        .to_string_lossy()
        .to_string();
    Ok((size, file_name, file_path.to_string_lossy().to_string()))
}

fn get_hash(file_path: &PathBuf) -> Result<String> {
    let mut file = File::open(file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to open file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;

    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 4096]; //bump up a little but, might have to change back to 4096 if this breakes with the threading
    loop {
        let bytes_read = file.read(&mut buffer).map_err(|e| {
            LavaError::new(
                format!("Unable to read bytes during hashing because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let result = hasher.finalize();
    let hash_hex = format!("{:x}", result);

    Ok(hash_hex)
}

fn get_header_info(log_file: &LogFile) -> Result<Option<HeaderInfo>> {
    if log_file.log_type == LogType::Csv {
        let header_info = crate::handlers::csv_handlers::get_header_info(log_file)?;
        return Ok(Some(header_info));
    } else {
        return Ok(None);
    }
}

fn try_to_get_timestamp_hit(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
    header_info: Option<HeaderInfo>,
) -> Result<Option<IdentifiedTimeInformation>> {
    if log_file.log_type == LogType::Csv {
        let header_info_unwrapped = header_info.ok_or_else(|| {
            LavaError::new(
                "Did not receive header info for a CSV",
                LavaErrorLevel::Critical,
            )
        })?;
        return try_to_get_timestamp_hit_for_csv(
            log_file,
            execution_settings,
            header_info_unwrapped,
        );
    } else if log_file.log_type == LogType::Unstructured {
        return try_to_get_timestamp_hit_for_unstructured(log_file, execution_settings);
    } else if log_file.log_type == LogType::Json {
        return try_to_get_timestamp_hit_for_json(log_file, execution_settings);
    }
    Err(LavaError::new(
        "Have not implemented scanning for timestamp for this file type yet",
        LavaErrorLevel::Critical,
    ))
}

fn set_time_direction_by_scanning_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
    header_info: Option<HeaderInfo>,
) -> Result<()> {
    if log_file.log_type == LogType::Csv {
        let header_info_unwrapped = header_info.ok_or_else(|| {
            LavaError::new(
                "Did not receive header info for a CSV",
                LavaErrorLevel::Critical,
            )
        })?;
        return set_time_direction_by_scanning_csv_file(
            log_file,
            timestamp_hit,
            header_info_unwrapped,
        );
    } else if log_file.log_type == LogType::Unstructured {
        return set_time_direction_by_scanning_unstructured_file(log_file, timestamp_hit);
    } else if log_file.log_type == LogType::Json {
        return set_time_direction_by_scanning_json_file(log_file, timestamp_hit);
    }
    Err(LavaError::new(
        "Have not implemented scanning for directions for this file type yet.",
        LavaErrorLevel::Critical,
    ))
}

fn stream_file(
    log_file: &LogFile,
    timestamp_hit: &Option<IdentifiedTimeInformation>,
    execution_settings: &ExecutionSettings,
    header_info: Option<HeaderInfo>,
) -> Result<LogRecordProcessor> {
    if log_file.log_type == LogType::Csv {
        if let Some(header_info_unwrapped) = header_info {
            return stream_csv_file(
                log_file,
                timestamp_hit,
                execution_settings,
                header_info_unwrapped,
            );
        } else {
            return Err(LavaError::new(
                "Did Not reveice header info for a CSV",
                LavaErrorLevel::Critical,
            ));
        }
    } else if log_file.log_type == LogType::Unstructured {
        return stream_unstructured_file(log_file, timestamp_hit, execution_settings);
    } else if log_file.log_type == LogType::Json {
        return stream_json_file(log_file, timestamp_hit, execution_settings);
    }
    Err(LavaError::new(
        "Have not implemented streaming for this file type yet",
        LavaErrorLevel::Critical,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn categorizes_csvs() {
        let mut paths: Vec<PathBuf> = Vec::new();
        paths.push(PathBuf::from("/path/to/file1.json"));
        paths.push(PathBuf::from("/path/to/file1.txt"));
        paths.push(PathBuf::from("/path/to/file2.csv"));

        let result = categorize_files(&paths);
        let expected: Vec<LogFile> = vec![
            LogFile {
                log_type: LogType::Json,
                file_path: PathBuf::from("/path/to/file1.json"),
            },
            LogFile {
                log_type: LogType::Unstructured,
                file_path: PathBuf::from("/path/to/file1.txt"),
            },
            LogFile {
                log_type: LogType::Csv,
                file_path: PathBuf::from("/path/to/file2.csv"),
            },
        ];

        assert_eq!(result, expected);
    }
}
