use crate::basic_objects::*;
use crate::errors::*;
use crate::processing_objects::*;
use csv::StringRecord;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn parse_json_line_into_json(line: String, index: usize) -> Result<Value> {
    // Parse the JSON
    match serde_json::from_str(&line) {
        Ok(val) => Ok(val),
        Err(e) => Err(LavaError::new(
            format!("Unable to parse JSON at line {} because of {}", index, e),
            LavaErrorLevel::Critical,
        )), // Not valid JSON
    }
}

pub fn try_to_get_timestamp_hit_for_json(
    log_file: &LogFile,
    execution_settings: &ExecutionSettings,
) -> Result<Option<IdentifiedTimeInformation>> {
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to read log file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let reader = BufReader::new(file);

    let mut lines = reader.lines();
    if let Some(line_result) = lines.next() {
        let line = line_result.map_err(|e| {
            LavaError::new(
                format!("Error reading line because of {}", e),
                LavaErrorLevel::Critical,
            )
        })?;
        let serialized_line = parse_json_line_into_json(line, 0);
        println!("{:?}", serialized_line);
        // // Recursively scan the JSON for string values
        // for date_regex in &execution_settings.regexes {
        //     if find_match_in_json(&json_value, date_regex) {
        //         println!(
        //             "Found match for '{}' time format in {}",
        //             date_regex.pretty_format,
        //             log_file.file_path.to_string_lossy()
        //         );
        //         return Ok(Some(IdentifiedTimeInformation {
        //             column_name: None,
        //             column_index: None,
        //             direction: None,
        //             regex_info: date_regex.clone(),
        //         }));
        //     }
        // }
    }

    Ok(None)
}


#[cfg(test)]
mod json_handler_tests {

    use super::*;
    #[test]
    fn test_json_serialize_success(){
        let json_str = r#"
        {
            "user": {
                "id": 42,
                "profile": {
                    "name": "Alice",
                    "email": "alice@example.com"
                }
            }
        }"#;
        let response = parse_json_line_into_json(json_str.to_string(), 1);
        println!("{:?}", response);
        assert!(response.is_ok());
    }

    #[test]
    fn test_json_serialize_fail(){
        let json_str = r#"
        {
            "user": {
                "id": 42,
                "profile": {
                    "name": "Alice",
                    "email": "alice@example.com"
                            }
        }"#;
        let response = parse_json_line_into_json(json_str.to_string(), 1);
        println!("{:?}", response);
        assert!(response.is_err());
    }
}