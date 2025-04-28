use crate::basic_objects::*;
use crate::errors::*;
use chrono::{TimeDelta, Utc};
use csv::StringRecord;
use csv::Writer;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

pub fn write_output_to_csv(processed_log_files: &Vec<ProcessedLogFile>) -> Result<()> {
    // in the final version, maybe have a full version that has tons of fields, and then a simplified version. Could have command line arg to trigger verbose one
    //Add something here to create the
    let output_filename = generate_log_filename();
    let mut wtr = Writer::from_path(&output_filename)
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
    println!("Data written to {output_filename}");
    Ok(())
}
