use chrono::{TimeDelta, Utc};
use csv::StringRecord;
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
