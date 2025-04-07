use glob::glob;
use std::path::PathBuf;
use std::fs::File;
use std::io::{self, Read};
use sha2::{Sha256, Digest};
use std::error::Error;
use rayon::prelude::*;

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

#[derive(PartialEq, Debug)]
pub struct ProcessedLogFile {
    pub sha256hash: String,
    pub filename: String,
    pub file_path: String,
}

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

    for result in results{
        println!("File: {} - SHA-256 hash: {}", result.filename, result.sha256hash);
    }
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
        }
    }
    supported_files
}

fn calculate_sha256(file_path: &PathBuf) -> io::Result<String> {
    let mut file = File::open(file_path)?;
    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 4096];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let result = hasher.finalize();
    let hash_hex = format!("{:x}", result);

    Ok(hash_hex)
}

pub fn process_file(log_file: &LogFile) -> Result<ProcessedLogFile, Box<dyn Error>>{ // Might be good to specify why type of error?

    let hash: String = calculate_sha256(&log_file.file_path)?; // The question mark here will propogate any possible error up.
    let file_name = log_file.file_path.file_name().expect("Error getting file name");
    Ok(
        ProcessedLogFile{
            sha256hash: hash,
            filename: file_name.to_string_lossy().to_string(),
            file_path: log_file.file_path.to_string_lossy().to_string(),
        }
    )
}
// pub fn process_csv_file(log_file: &LogFile) -> ProcessedLogFile{
    
// }