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

#[derive(Debug, Clone, Default)]
pub struct JsonValue {
    pub path: String,
    pub value: String,
}

pub fn collect_json_values_with_paths(value: &Value) -> Vec<JsonValue> {
    let mut result = Vec::new();
    let mut path = Vec::new();
    collect_helper(value, &mut path, &mut result);
    result
}

fn collect_helper<'a>(
    value: &'a Value,
    path: &mut Vec<String>,
    result: &mut Vec<JsonValue>,
) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                path.push(key.clone());
                collect_helper(val, path, result);
                path.pop();
            }
        }
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                path.push(i.to_string());
                collect_helper(val, path, result);
                path.pop();
            }
        }
        _ => {
            let full_path = format!("/{}", path.join("/"));
            match value {
                Value::String(s) => {
                    result.push(JsonValue{path:full_path, value: s.clone()}); // unquoted string
                }
                _ => {
                    result.push(JsonValue{path: full_path, value: value.to_string()}); // fallback for numbers, bools, etc.
                }
            }
        }
    }
}


pub fn convert_arrow_path_to_json_pointer(input: &str) -> String {
    let parts: Vec<&str> = input.split("->").collect();
    format!("/{}", parts.join("/"))
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

        return try_to_get_timestamp_hit_for_json_functionality(line, execution_settings)
    }

    Ok(None)
}

fn try_to_get_timestamp_hit_for_json_functionality(line: String, execution_settings: &ExecutionSettings) -> Result<Option<IdentifiedTimeInformation>> {
    let serialized_line = parse_json_line_into_json(line, 0)?;
    println!("{:?}", serialized_line);
    if let Some(field_to_use) = &execution_settings.timestamp_field {
        let correct_formatted_path = convert_arrow_path_to_json_pointer(&field_to_use);
        if let Some(found_value) = serialized_line.pointer(&correct_formatted_path) {
            for date_regex in execution_settings.regexes.iter() {
                if date_regex.string_contains_date(found_value.as_str().ok_or_else(|| LavaError::new("The target value was not a string", LavaErrorLevel::Critical))?) {
                    return Ok(Some(IdentifiedTimeInformation {
                        column_name: Some(correct_formatted_path),
                        column_index: None,
                        direction: None,
                        regex_info: date_regex.clone(),
                    }));
                }
            }
        }
    } else {
        let converted_vec = collect_json_values_with_paths(&serialized_line);
        for json_key in converted_vec.iter() {
            for date_regex in execution_settings.regexes.iter() {
                if date_regex.string_contains_date(&json_key.value) {
                    return Ok(Some(IdentifiedTimeInformation {
                        column_name: Some(json_key.path.clone()),
                        column_index: None,
                        direction: None,
                        regex_info: date_regex.clone(),
                    }));
                }
            }
        }

    }
    Ok(None)
}

pub fn set_time_direction_by_scanning_json_file(
    log_file: &LogFile,
    timestamp_hit: &mut IdentifiedTimeInformation,
) -> Result<()> {
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to open the log file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let reader = BufReader::new(file);
    let mut direction_checker = TimeDirectionChecker::default();
    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| {
            LavaError::new(
                format!("Error reading line because of {}", e),
                LavaErrorLevel::Critical,
            )
        })?;
        let serialized_line = parse_json_line_into_json(line, index)?;
        let extracted_timestamp = serialized_line.pointer(timestamp_hit.column_name.as_ref().ok_or_else(|| LavaError::new(
            format!("No JSON path to timestamp field found during scanning for direction phase."),
            LavaErrorLevel::Critical,
        ))?);
        match extracted_timestamp {
            None => return Err(LavaError::new(
                format!("No timestamp field extracted during JSON direction scanning"),
                LavaErrorLevel::Critical,
            )),
            Some(timestamp) =>{
                match timestamp {
                    Value::String(string) =>{
                        if let Some(current_datetime) = timestamp_hit
                        .regex_info
                        .get_timestamp_object_from_string_contianing_date(string.clone())?
                    {
                        if let Some(direction) = direction_checker.process_timestamp(current_datetime) {
                            timestamp_hit.direction = Some(direction);
                            return Ok(());
                        }
                    };
                    },
                    _ => return Err(LavaError::new(
                        format!("Non String timestamp field extracted during JSON direction scanning"),
                        LavaErrorLevel::Critical,
                    )),

                }
            }
        }
    }
    Ok(())
}





#[cfg(test)]
mod json_handler_tests {

    use super::*;
    use crate::date_regex::DateRegex;
    use regex::Regex;
    use std::path::PathBuf;
    
    #[test]
    fn test_json_serialize_success() {
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
    fn test_json_serialize_fail() {
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

    #[test]
    fn test_json_into_vec_converter() {
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
        let response = parse_json_line_into_json(json_str.to_string(), 1).unwrap();
        let converted = collect_json_values_with_paths(&response);
        println!("{:?}", converted);
        assert_eq!(3, converted.len())
    }

    #[test]
    fn test_json_pathgrabber() {
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
        let response = parse_json_line_into_json(json_str.to_string(), 1).unwrap();
        let converted = collect_json_values_with_paths(&response);
        assert_eq!("Alice", response.pointer("/user/profile/name").unwrap())
    }

    #[test]
    fn test_timestamp_field_matches_without_provided() {
        let json_line = r#"
        {
            "first_timestamp": "2024-06-09 12:34:56",
            "second_timestamp": {"test":"2024-06-09 12:34:56"}
        }
        "#;

        let test_args = ExecutionSettings {
            input_dir: PathBuf::from("/dummy/input"),
            output_dir: PathBuf::from("/dummy/output"),
            regexes: vec![DateRegex {
                pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(),
                strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
                regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
            }],
            timestamp_field: None,
            quick_mode: false,
            verbose_mode: true,
            actually_write_to_files: false,
        };

        let result = try_to_get_timestamp_hit_for_json_functionality(
            json_line.to_string(),
            &test_args,
        )
        .unwrap();

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.column_name, Some("/first_timestamp".to_string()));
        assert_eq!(info.regex_info.pretty_format, "YYYY-MM-DD HH:MM:SS");
    }

    #[test]
    fn test_timestamp_field_matches_with_provided() {
        let json_line = r#"
        {
            "first_timestamp": "2024-06-09 12:34:56",
            "second_timestamp": {"test":"2024-06-09 12:34:56"}
        }
        "#;

        let test_args = ExecutionSettings {
            input_dir: PathBuf::from("/dummy/input"),
            output_dir: PathBuf::from("/dummy/output"),
            regexes: vec![DateRegex {
                pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(),
                strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
                regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
            }],
            timestamp_field: Some("second_timestamp->test".to_string()),
            quick_mode: false,
            verbose_mode: true,
            actually_write_to_files: false,
        };

        let result = try_to_get_timestamp_hit_for_json_functionality(
            json_line.to_string(),
            &test_args,
        )
        .unwrap();

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.column_name, Some("/second_timestamp/test".to_string()));
        assert_eq!(info.regex_info.pretty_format, "YYYY-MM-DD HH:MM:SS");
    }
}
