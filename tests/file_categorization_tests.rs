use log_checker::*;
use std::path::PathBuf;

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
