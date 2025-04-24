use clap::builder::Str;
use glob::glob;
use std::path::PathBuf;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use sha2::{Sha256, Digest};
use std::error::Error;
use rayon::prelude::*;
use csv::Writer;
use serde::Serialize;
use regex::Regex;
use once_cell::sync::Lazy;
// use polars::prelude::*;
use csv::ReaderBuilder;
use thiserror::Error;
use chrono::Utc;


type Result<T> = std::result::Result<T, LogCheckError>;

#[derive(Debug, Clone, Error)]
pub enum LogCheckError {
    #[error("LogCheckError: {0}")]
    ForCSVOutput(String),
    #[error("{0}")]
    UnexpectedError(String)
}



#[derive(PartialEq, Debug)]
pub enum LogType{
    Csv,
    Json,
}

#[derive(PartialEq, Debug)]
pub struct LogFile {
    pub log_type: LogType,
    pub file_path: PathBuf,
}

#[derive(PartialEq, Debug, Serialize)]
pub struct ProcessedLogFile {
    pub sha256hash: String,
    pub filename: String,
    pub file_path: String,
    pub size: u64,
    // pub error: Option<String>,
}

#[derive(Debug)]
pub struct DateRegex {
    pub date_format: String,
    pub date_regex: Regex,
}

pub static DATE_REGEXES: Lazy<Vec<DateRegex>> = Lazy::new(|| {
    vec![
        DateRegex {
            date_format: "MM-DD-YYYY".to_string(),
            date_regex: Regex::new(r"^\d{2}-\d{2}-\d{4}$").unwrap(),
        },
        DateRegex {
            date_format: "YYYY-MM-DD".to_string(),
            date_regex: Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap(),
        },
        DateRegex {
            date_format: "DD-MM-YYYY".to_string(),
            date_regex: Regex::new(r"^\d{2}-\d{2}-\d{4}$").unwrap(),
        },
        DateRegex {
            date_format: "YYYY/MM/DD".to_string(),
            date_regex: Regex::new(r"^\d{4}/\d{2}/\d{2}$").unwrap(),
        },
        DateRegex {
            date_format: "MMM DD YYYY".to_string(), // e.g. Mar 22 2022
            date_regex: Regex::new(r"^[A-Z][a-z]{2} \d{1,2} \d{4}$").unwrap(),
        },
        DateRegex {
            date_format: "MMMM DD, YYYY".to_string(), // e.g. March 22, 2022
            date_regex: Regex::new(r"^[A-Z][a-z]+ \d{1,2}, \d{4}$").unwrap(),
        },
        DateRegex {
            date_format: "YYYY-MM-DD HH:MM:SS".to_string(), // 24-hour datetime
            date_regex: Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$").unwrap(),
        },
        DateRegex {
            date_format: "YYYY-MM-DDTHH:MM:SSZ".to_string(), // ISO 8601
            date_regex: Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$").unwrap(),
        },
        DateRegex {
            date_format: "M/D/YYYY H:MM AM/PM".to_string(), // 12-hour US time
            date_regex: Regex::new(r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2} (AM|PM|am|pm)$").unwrap(),
        },
        DateRegex {
            date_format: "YYYY-MM-DDTHH:MM:SS.SSS".to_string(),
            date_regex: Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,3}$").unwrap(),
        },
    ]
});

pub fn iterate_through_input_dir(input_dir:String){

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

    let _ = write_to_csv(&results).expect("Failed to open output file");
}

fn generate_log_filename() -> String {
    let now = Utc::now();
    let formatted = now.format("%Y-%m-%d_%H-%M-%S_LogCheck_Output.csv");
    formatted.to_string()
}

fn write_to_csv(processed_log_files: &Vec<ProcessedLogFile>) -> Result<()> {
    //Add something here to create the 
    let mut wtr = Writer::from_path(generate_log_filename()).map_err(|e| LogCheckError::UnexpectedError(format!("Error opening the output file. {e}")))?;
    wtr.write_record(&["Filename", "File Path", "SHA256 Hash", "Size"]).map_err(|e| LogCheckError::UnexpectedError(format!("Error writing header of output file. {e}")))?;
    for log_file in processed_log_files {
        wtr.serialize((
            &log_file.filename,
            &log_file.file_path,
            &log_file.sha256hash,
            &log_file.size,
        )).map_err(|e| LogCheckError::UnexpectedError(format!("Error writing line of output file.")))?;
    }
    wtr.flush().map_err(|e| LogCheckError::UnexpectedError(format!("Error flushing output file.")))?; //Is this really needed?
    println!("Data written to output.csv");
    Ok(())
}

pub fn categorize_files(file_paths: &Vec<PathBuf>) -> Vec<LogFile>{
    let mut supported_files: Vec<LogFile> = Vec::new();

    for file_path in file_paths{
        if let Some(extension) = file_path.extension() {
            if extension == "csv"{
                supported_files.push(
                    LogFile{
                        log_type:LogType::Csv,
                        file_path:file_path.to_path_buf(),
                    }
                )
            }
        }else{
            println!("Error getting file extension for {}", file_path.to_string_lossy().to_string())
        }
    }
    supported_files
}

fn get_hash_and_size(file_path: &PathBuf) -> Result<(String, u64)> {
    let mut file = File::open(file_path).map_err(|e| LogCheckError::ForCSVOutput(format!("Error opening file, it may have been in use.")))?;
    let size = file.metadata().map_err(|e| LogCheckError::ForCSVOutput(format!("Error getting size of file.")))?.len();
    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 4096];
    loop {
        let bytes_read = file.read(&mut buffer).map_err(|e| LogCheckError::ForCSVOutput(format!("Error reading file during hashing operation.")))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let result = hasher.finalize();
    let hash_hex = format!("{:x}", result);

    Ok((hash_hex, size))
}

pub fn process_file(log_file: &LogFile) -> Result<ProcessedLogFile>{

    let (hash, size) = get_hash_and_size(&log_file.file_path)?; // The question mark here will propogate any possible error up. DONT WANT TO DO THIS, WANT TO SOMEHOW HANDLE ERROR FROM ONE
    let file_name = log_file.file_path.file_name().expect("Error getting file name");

    let (time_header, time_format) = find_timestamp_field(log_file)?;
    println!(
        "Match found Column '{}' matches the '{}' format in {}",
        time_header, time_format, log_file.file_path.to_string_lossy().to_string()
    );
    Ok(
        ProcessedLogFile{
            sha256hash: hash,
            filename: file_name.to_string_lossy().to_string(),
            file_path: log_file.file_path.to_string_lossy().to_string(),
            size: size,
        }
    )
}


pub fn find_timestamp_field(log_file: &LogFile) -> Result<(String, String)> { //This is lazy here
    if log_file.log_type == LogType::Csv {
        let file = File::open(&log_file.file_path).map_err(|e| LogCheckError::ForCSVOutput("Error reading file to find timestamp.".into()))?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true) // Set to false if there's no header
            .from_reader(file);

        let headers: csv::StringRecord = reader.headers().map_err(|e| LogCheckError::ForCSVOutput("Error reading file headers.".into()))?.clone(); // this returns a &StringRecord
        let record: csv::StringRecord = reader.records().next().unwrap().map_err(|e| LogCheckError::ForCSVOutput(format!("Error reading first line of file. {e}")))?; // This is returning a result, that is why I had to use the question mark below before the iter()
        for (i, field) in record.iter().enumerate() {
            for date_regex in DATE_REGEXES.iter() {
                if date_regex.date_regex.is_match(field) {
                    return Ok((headers.get(i).unwrap().to_string(), date_regex.date_format.clone()));//I know the clone is lazy I am just tired
                }
            }
        }
    }
    println!("Could not find a supported timestamp in {}", log_file.file_path.to_string_lossy().to_string());
    Err(LogCheckError::ForCSVOutput("Could not find a supported timestamp format.".into()))
}
// pub fn process_csv_file(log_file: &LogFile) -> ProcessedLogFile{
    
// }