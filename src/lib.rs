use glob::glob;
use std::path::PathBuf;
use std::fs::File;
use std::io::{self, Read};
use sha2::{Sha256, Digest};

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

    for path in supported_files{
        process_file(path)
        // println!("{}", path.file_path.display())
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

pub fn process_file(log_file: LogFile){

    match calculate_sha256(&log_file.file_path) {
        Ok(hash) => {
            match log_file.file_path.file_name() {
                Some(file_name) => {
                    println!("File: {} - SHA-256 hash: {}", file_name.to_string_lossy(), hash);
                }
                None => println!("No filename found."),
            }
        },
        Err(e) => eprintln!("Error when calculating SHA256 Hash: {}", e),
    }
}
// pub fn process_csv_file(log_file: &LogFile) -> ProcessedLogFile{
    
// }