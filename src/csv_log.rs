use crate::basic_objects::*;
use crate::date_regex::*;
use crate::errors::*;
use csv::ReaderBuilder;
use std::fs::File;

pub fn try_to_get_timestamp_hit_for_csv(log_file: &LogFile) -> Result<IdentifiedTimeInformation> {
    let file = File::open(&log_file.file_path)
        .map_err(|e| LogCheckError::new(format!("Unable to read csv file because of {e}")))?;
    let mut reader = ReaderBuilder::new()
        .has_headers(true) // Set to false if there's no header
        .from_reader(file);

    let headers: csv::StringRecord = reader
        .headers()
        .map_err(|e| LogCheckError::new(format!("Unable to get headers because of {e}")))?
        .clone(); // this returns a &StringRecord
    let record: csv::StringRecord = reader
        .records()
        .next()
        .unwrap()
        .map_err(|e| LogCheckError::new(format!("Unable to get first row because of {e}")))?; // This is returning a result, that is why I had to use the question mark below before the iter()
    for (i, field) in record.iter().enumerate() {
        for date_regex in DATE_REGEXES.iter() {
            if date_regex.regex.is_match(field) {
                println!(
                    "Found match for '{}' time format in the '{}' column of {}",
                    date_regex.pretty_format,
                    headers.get(i).unwrap().to_string(),
                    log_file.file_path.to_string_lossy().to_string()
                );
                return Ok(IdentifiedTimeInformation {
                    column_name: Some(headers.get(i).unwrap().to_string()),
                    column_index: Some(i),
                    direction: None,
                    regex_info: date_regex.clone(),
                });
            }
        }
    }
    println!(
        "Could not find a supported timestamp in {}",
        log_file.file_path.to_string_lossy().to_string()
    );
    Err(LogCheckError::new(
        "Could not find a supported timestamp format.",
    ))
}
