use chrono::{DateTime, Duration, NaiveDateTime, ParseResult, TimeDelta, Utc};
use log_checker::*;
use std::path::PathBuf;
use log_checker::errors::*;
use log_checker::basic_objects::*;

#[test]
fn categorizes_csvs() {
    let mut paths: Vec<PathBuf> = Vec::new();
    paths.push(PathBuf::from("/path/to/file1.txt"));
    paths.push(PathBuf::from("/path/to/file2.csv"));

    let result = categorize_files(&paths);
    let expected: Vec<LogFile> = vec![LogFile {
        log_type: LogType::Csv,
        file_path: PathBuf::from("/path/to/file2.csv"),
    }];

    assert_eq!(result, expected);
}

#[test]
fn test_to_date() {
    let a: String = "this is a timestamp 2025-08-12 12:23:34".to_string();
    let parsed_datetime = NaiveDateTime::parse_from_str(&a, "%Y-%m-%d %H:%M:%S")
        .map_err(|e| LogCheckError::new(format!("Unable to parse timestamp because {e}")));
    println!(
        "Formatted datetime: {}",
        parsed_datetime.unwrap().format("%Y-%m-%d %H:%M:%S")
    );
    // assert_eq!(result, expected);
}
