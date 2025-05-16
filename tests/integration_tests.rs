use LAVA::{basic_objects::{ExecutionSettings, LogFile, LogType, ProcessedLogFile}, process_file};
use tempfile::NamedTempFile;
use std::fs;
struct TempInputFile {
    log_file_object: LogFile,
    temp_file: NamedTempFile,
}

impl TempInputFile {
    pub fn new(file_type: LogType, content: &str) -> Self {
        let temp_file = NamedTempFile::new().expect("failed to create temp file");
        let file_path = temp_file.path();
        fs::write(file_path, content).expect("Failed to write content to temp file.");
        Self {
            log_file_object: LogFile { log_type: file_type, file_path: file_path.to_path_buf()},
            temp_file: temp_file,
        }
    }

    pub fn get_log_file_object(&self) -> &LogFile{
        &self.log_file_object
    }

    pub fn delete_temp_file(self) {
        self.temp_file.close().expect("Failed to delet demp file");
    }

}


#[test]
fn integration_test_successful_run_no_errors(){
    let data = "\
    id,name,date\n\
    1,John,2025-05-09 10:00:00\n\
    2,Jane,2025-05-10 11:00:00\n\
    4,James,2025-06-01 13:00:00\n";

    let temp_log_file = TempInputFile::new(LogType::Csv, data);
    let log_file = temp_log_file.get_log_file_object();
    let settings = ExecutionSettings::create_integration_test_object(None, false);

    let output = process_file(log_file, &settings);
    let processed = output.expect("Failed to get Proceesed Log File");
    assert_eq!(0, processed.errors.len());
    temp_log_file.delete_temp_file();
}



#[test]
fn integration_test_successful_run_duplicates_and_redactions(){
    let data = "\
    id,name,date\n\
    1,John,2025-05-09 10:00:00\n\
    2,Jane,2025-05-10 11:00:00\n\
    2,Jane,2025-05-10 11:00:00\n\
    4,James,2025-06-01 13:00:00\n\
    4,J*********s,2025-06-01 13:00:00\n";

    let temp_log_file = TempInputFile::new(LogType::Csv, data);
    let log_file = temp_log_file.get_log_file_object();
    let settings = ExecutionSettings::create_integration_test_object(None, false);

    let output = process_file(log_file, &settings);
    let processed = output.expect("Failed to get Proceesed Log File");
    assert_eq!(0, processed.errors.len());
    assert_eq!("1", processed.num_dupes.unwrap());
    assert_eq!("1", processed.num_redactions.unwrap());
    temp_log_file.delete_temp_file();
}

#[test]
fn integration_test_successful_run_duplicates_and_redactions_quick_mode(){
    let data = "\
    id,name,date\n\
    1,John,2025-05-09 10:00:00\n\
    2,Jane,2025-05-10 11:00:00\n\
    2,Jane,2025-05-10 11:00:00\n\
    4,James,2025-06-01 13:00:00\n\
    4,J*********s,2025-06-01 13:00:00\n";

    let temp_log_file = TempInputFile::new(LogType::Csv, data);
    let log_file = temp_log_file.get_log_file_object();
    let settings = ExecutionSettings::create_integration_test_object(None, true);

    let output = process_file(log_file, &settings);
    let processed = output.expect("Failed to get Proceesed Log File");
    assert_eq!(0, processed.errors.len());
    assert_eq!("0", processed.num_dupes.unwrap());
    assert_eq!("0", processed.num_redactions.unwrap());
    assert_eq!(None, processed.sha256hash);
    temp_log_file.delete_temp_file();
}

#[test]
fn integration_test_out_of_order_time_run_duplicates_and_redactions_1(){
    let data = "\
    id,name,date\n\
    1,John,2025-05-09 10:00:00\n\
    2,Jane,2025-05-10 10:30:00\n\
    2,Jane,2025-05-10 10:15:00\n\
    4,James,2025-06-01 13:00:00\n\
    4,J*********s,2025-06-01 13:00:00\n";

    let temp_log_file = TempInputFile::new(LogType::Csv, data);
    let log_file = temp_log_file.get_log_file_object();
    let settings = ExecutionSettings::create_integration_test_object(None, false);

    let output = process_file(log_file, &settings);
    let processed = output.expect("Failed to get Proceesed Log File");
    assert_eq!(0, processed.errors.len());
    assert_eq!("1", processed.num_dupes.unwrap());
    assert_eq!("1", processed.num_redactions.unwrap());
    temp_log_file.delete_temp_file();
}

#[test]
fn integration_test_out_of_order_time_run_duplicates_and_redactions_2(){
    let data = "\
    id,name,date\n\
    1,John,2025-05-09 10:00:00\n\
    2,Jane,2025-05-10 10:30:00\n\
    2,Jane,2025-05-10 10:45:00\n\
    4,James,2025-06-01 13:00:00\n\
    4,J*********s,2025-06-01 10:00:00\n";

    let temp_log_file = TempInputFile::new(LogType::Csv, data);
    let log_file = temp_log_file.get_log_file_object();
    let settings = ExecutionSettings::create_integration_test_object(None, false);

    let output = process_file(log_file, &settings);
    let processed = output.expect("Failed to get Proceesed Log File");
    assert_eq!(0, processed.errors.len());
    assert_eq!("1", processed.num_dupes.unwrap());
    assert_eq!("1", processed.num_redactions.unwrap());
    temp_log_file.delete_temp_file();
}

