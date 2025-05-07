use crate::basic_objects::*;
use crate::date_regex::DateRegex;
use crate::date_regex::RawDateRegex;
use crate::errors::*;
use chrono::{TimeDelta, Utc};
use csv::StringRecord;
use csv::Writer;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::path::Path;
use std::fs;


pub fn generate_log_filename() -> String {
    let now = Utc::now();
    let formatted = now.format("%Y-%m-%d_%H-%M-%S_LogCheck_Output.csv");
    formatted.to_string()
}

pub fn format_timedelta(tdelta: TimeDelta) -> String {
    let total_seconds = tdelta.num_seconds().abs(); // make it positive for display

    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

pub fn hash_csv_record(record: &StringRecord) -> u64 {
    let mut hasher = DefaultHasher::new();
    record.iter().for_each(|field| field.hash(&mut hasher));
    hasher.finish()
}

pub fn hash_string(input: &String) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher); // Hash the string (dereferenced automatically to &str)
    hasher.finish() // Return the resulting hash
}

pub fn write_output_to_csv(processed_log_files: &Vec<ProcessedLogFile>, command_line_args: &CommandLineArgs) -> Result<()> {
    // in the final version, maybe have a full version that has tons of fields, and then a simplified version. Could have command line arg to trigger verbose one
    //Add something here to create the
    let output_filepath = command_line_args.output_dir.join(generate_log_filename());
    let mut wtr = Writer::from_path(&output_filepath)
        .map_err(|e| LogCheckError::new(format!("Unable to open ouptut file because of {e}")))?;
    wtr.write_record(&[
        "Filename",
        "File Path",
        "SHA256 Hash",
        "Size",
        "Header Used",
        "Timestamp Format",
        "Number of Records",
        "Earliest Timestamp",
        "Latest Timestamp",
        "Duration of Entire Log File",
        "Largest Time Gap",
        "Duration of Largest Time Gap",
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
            log_file.num_records.as_deref().unwrap_or(""),
            log_file.min_timestamp.as_deref().unwrap_or(""),
            log_file.max_timestamp.as_deref().unwrap_or(""),
            log_file.min_max_duration.as_deref().unwrap_or(""),
            log_file.largest_gap.as_deref().unwrap_or(""),
            log_file.largest_gap_duration.as_deref().unwrap_or(""),
            log_file.error.as_deref().unwrap_or(""),
        ))
        .map_err(|e| {
            LogCheckError::new(format!("Issue writing lines of output file because of {e}"))
        })?;
    }
    wtr.flush().map_err(|e| {
        LogCheckError::new(format!("Issue flushing to the ouptut file because of {e}"))
    })?; //Is this really needed?
    println!("Data written to {}", output_filepath.to_string_lossy());
    Ok(())
}

pub fn get_user_supplied_regexes_from_command_line(regex_file_path: &PathBuf) -> Result<Vec<DateRegex>>{
    let yaml_path = Path::new(regex_file_path);
    let content = fs::read_to_string(yaml_path).map_err(|e| LogCheckError::new(format!("Failed to read YAML file because of {e}")))?;
    let parsed: Vec<RawDateRegex> = serde_yaml::from_str(&content).map_err(|e| LogCheckError::new(format!("Failed to parse YAML file because of {e}")))?;
    let converted: Vec<DateRegex> = parsed.into_iter()
                                    .map(DateRegex::new_from_raw_date_regex)
                                    .collect();
    Ok(converted)
}
