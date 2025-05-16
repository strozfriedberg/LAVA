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

pub fn get_file_stem(log_file: &LogFile) -> Result<String> {
    let file_name = &log_file
        .file_path
        .file_stem()
        .ok_or_else(|| LavaError::new("Could not get file stem.", LavaErrorLevel::Critical))?;
    Ok(file_name.to_string_lossy().to_string())
}

pub fn write_output_to_csv(
    processed_log_files: &Vec<ProcessedLogFile>,
    execution_settings: &ExecutionSettings,
) -> Result<()> {
    // in the final version, maybe have a full version that has tons of fields, and then a simplified version. Could have command line arg to trigger verbose one
    //Add something here to create the
    let output_filepath = execution_settings.output_dir.join(generate_log_filename());
    let mut wtr = Writer::from_path(&output_filepath).map_err(|e| {
        LavaError::new(
            format!("Unable to open ouptut file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    wtr.write_record(&[
        "Filename",
        "File Path",
        "SHA256 Hash",
        "Size",
        "Header Row Used",
        "Header Used",
        "Timestamp Format",
        "Number of Records",
        "Earliest Timestamp",
        "Latest Timestamp",
        "Duration of Entire Log File",
        "Largest Time Gap",
        "Duration of Largest Time Gap",
        "Duplicate Record Count",
        "Possible Redactions Count",
        "Error",
    ])
    .map_err(|e| {
        LavaError::new(
            format!("Unable to write headers because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    for log_file in processed_log_files {
        let error_message = if log_file.errors.is_empty() {
            String::new()
        } else {
            if log_file.errors.len() > 1{
                format!(
                    "There were {} errors during processing. Check errors.csv for detailed errors.",
                    log_file.errors.len()
                )
            }else{
                log_file.errors[0].reason.clone()
            }

        };
        wtr.serialize((
            log_file.filename.as_deref().unwrap_or(""),
            log_file.file_path.as_deref().unwrap_or(""),
            log_file.sha256hash.as_deref().unwrap_or(""),
            log_file.size.unwrap_or(0),
            log_file.header_index_used.as_deref().unwrap_or(""),
            log_file.time_header.as_deref().unwrap_or(""),
            log_file.time_format.as_deref().unwrap_or(""),
            log_file.num_records.as_deref().unwrap_or(""),
            log_file.min_timestamp.as_deref().unwrap_or(""),
            log_file.max_timestamp.as_deref().unwrap_or(""),
            log_file.min_max_duration.as_deref().unwrap_or(""),
            log_file.largest_gap.as_deref().unwrap_or(""),
            log_file.largest_gap_duration.as_deref().unwrap_or(""),
            log_file.num_dupes.as_deref().unwrap_or(""),
            log_file.num_redactions.as_deref().unwrap_or(""),
            error_message,
        ))
        .map_err(|e| {
            LavaError::new(
                format!("Issue writing lines of output file because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?;
    }
    wtr.flush().map_err(|e| {
        LavaError::new(
            format!("Issue flushing to the ouptut file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?; //Is this really needed?
    println!("Data written to {}", output_filepath.to_string_lossy());
    Ok(())
}
