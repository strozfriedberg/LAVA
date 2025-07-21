use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::get_file_stem;
use crate::processing_objects::*;
use csv::StringRecord;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn parse_json_line_into_json(line: &str, index: usize) -> Result<Value> {
    let trimmed = line.trim();
    if trimmed.len() > 0 {
        match serde_json::from_str(line) {
            Ok(val) => Ok(val),
            Err(e) => Err(LavaError::new(
                format!("Unable to parse JSON at line {} because of {}", index, e),
                LavaErrorLevel::Critical,
            )), // Not valid JSON
        }
    } else {
        Err(LavaError::new(
            "Attempted to parse JSON from empty line",
            LavaErrorLevel::Low,
        ))
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

fn collect_helper<'a>(value: &'a Value, path: &mut Vec<String>, result: &mut Vec<JsonValue>) {
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
                    result.push(JsonValue {
                        path: full_path,
                        value: s.clone(),
                    }); // unquoted string
                }
                _ => {
                    result.push(JsonValue {
                        path: full_path,
                        value: value.to_string(),
                    }); // fallback for numbers, bools, etc.
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

        return try_to_get_timestamp_hit_for_json_functionality(line, execution_settings);
    }

    Ok(None)
}

fn try_to_get_timestamp_hit_for_json_functionality(
    line: String,
    execution_settings: &ExecutionSettings,
) -> Result<Option<IdentifiedTimeInformation>> {
    let serialized_line = parse_json_line_into_json(&line, 0);
    match serialized_line {
        Err(e) => return Err(e),
        Ok(serialized_line) => {
            if let Some(field_to_use) = &execution_settings.timestamp_field {
                let correct_formatted_path = convert_arrow_path_to_json_pointer(&field_to_use);
                if let Some(found_value) = serialized_line.pointer(&correct_formatted_path) {
                    for date_regex in execution_settings.regexes.iter() {
                        if date_regex.string_contains_date(found_value.as_str().ok_or_else(
                            || {
                                LavaError::new(
                                    "The target value was not a string",
                                    LavaErrorLevel::Critical,
                                )
                            },
                        )?) {
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
        if line.trim().is_empty() {
            continue;
        }
        let serialized_line = match parse_json_line_into_json(&line, index) {
            Ok(line) => line,
            Err(_) => continue,
        };
        let extracted_timestamp =
            serialized_line.pointer(timestamp_hit.column_name.as_ref().ok_or_else(|| {
                LavaError::new(
                    format!(
                        "No JSON path to timestamp field found during scanning for direction phase."
                    ),
                    LavaErrorLevel::Critical,
                )
            })?);
        match extracted_timestamp {
            None => {
                return Err(LavaError::new(
                    format!("No timestamp field extracted during JSON direction scanning"),
                    LavaErrorLevel::Critical,
                ));
            }
            Some(timestamp) => match timestamp {
                Value::String(string) => {
                    if let Some(current_datetime) = timestamp_hit
                        .regex_info
                        .get_timestamp_object_from_string_contianing_date(string.clone())?
                    {
                        if let Some(direction) =
                            direction_checker.process_timestamp(current_datetime)
                        {
                            timestamp_hit.direction = Some(direction);
                            return Ok(());
                        }
                    };
                }
                _ => {
                    return Err(LavaError::new(
                        format!(
                            "Non String timestamp field extracted during JSON direction scanning"
                        ),
                        LavaErrorLevel::Critical,
                    ));
                }
            },
        }
    }
    Ok(())
}

pub fn stream_json_file(
    log_file: &LogFile,
    timestamp_hit: &Option<IdentifiedTimeInformation>,
    execution_settings: &ExecutionSettings,
) -> Result<LogRecordProcessor> {
    let mut processing_object = LogRecordProcessor::new(
        timestamp_hit,
        execution_settings,
        get_file_stem(log_file)?,
        None,
    );
    let file = File::open(&log_file.file_path).map_err(|e| {
        LavaError::new(
            format!("Unable to open log file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;

    let reader = BufReader::new(file);
    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| {
            LavaError::new(
                format!("Error reading line because of {}", e),
                LavaErrorLevel::Critical,
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let serialized_line = match parse_json_line_into_json(&line, index) {
            Ok(serialized_line) => serialized_line,
            Err(e) => {
                processing_object.process_record(LogFileRecord::new(
                    index,
                    None,
                    StringRecord::from(vec![line]),
                ))?;
                processing_object.add_error(e);
                continue;
            }
        };
        let current_datetime = match timestamp_hit {
            None => None,
            Some(timestamp_hit) => {
                if let Some(value_of_key) =
                    serialized_line.pointer(timestamp_hit.column_name.as_ref().unwrap())
                {
                    match value_of_key {
                        Value::String(string) => timestamp_hit
                            .regex_info
                            .get_timestamp_object_from_string_contianing_date(string.clone())?,
                        _ => {
                            return Err(LavaError::new(
                                format!(
                                    "Non String timestamp field extracted during JSON direction scanning"
                                ),
                                LavaErrorLevel::Critical,
                            ));
                        }
                    }
                } else {
                    None
                }
            }
        };
        processing_object.process_record(LogFileRecord::new(
            index,
            current_datetime,
            StringRecord::from(vec![line]),
        ))?;
    }
    Ok(processing_object)
}

#[cfg(test)]
mod json_handler_tests {

    use super::*;
    use crate::date_regex::DateRegex;
    use regex::Regex;
    use serde_json::json;
    use std::fs::write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    #[test]
    fn test_set_time_direction_by_scanning_json_file_ascending() {
        // Step 1: Create temporary log file with JSON lines
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_path_buf();

        // Create a few JSON lines with timestamps
        let json_lines = vec![
            json!({"timestamp": "2024-01-01T12:00:00Z"}).to_string(),
            json!({"timestamp": "2024-01-01T12:01:00Z"}).to_string(),
            json!({"timestamp": "2024-01-01T12:02:00Z"}).to_string(),
        ]
        .join("\n");

        write(&file_path, json_lines).expect("Failed to write JSON lines to temp file");

        // Step 2: Construct the input structs
        let log_file = LogFile {
            log_type: LogType::Json, // Assuming a variant exists
            file_path: file_path.clone(),
        };

        let mut identified_time_info = try_to_get_timestamp_hit_for_json(
            &log_file,
            &ExecutionSettings::create_integration_test_object(None, false),
        )
        .unwrap()
        .unwrap();

        // Step 3: Call the function
        let result = set_time_direction_by_scanning_json_file(&log_file, &mut identified_time_info);

        // Step 4: Assert success and expected direction
        assert!(result.is_ok());
        assert_eq!(
            identified_time_info.direction,
            Some(TimeDirection::Ascending)
        ); // or whatever is expected
    }

    #[test]
    fn test_set_time_direction_by_scanning_json_file_descending() {
        // Step 1: Create temporary log file with JSON lines
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_path_buf();

        // Create a few JSON lines with timestamps
        let json_lines = vec![
            json!({"timestamp": "2024-01-01T12:05:00Z"}).to_string(),
            json!({"timestamp": "2024-01-01T12:03:00Z"}).to_string(),
            json!({"timestamp": "2024-01-01T12:02:00Z"}).to_string(),
        ]
        .join("\n");

        write(&file_path, json_lines).expect("Failed to write JSON lines to temp file");

        // Step 2: Construct the input structs
        let log_file = LogFile {
            log_type: LogType::Json, // Assuming a variant exists
            file_path: file_path.clone(),
        };

        let mut identified_time_info = try_to_get_timestamp_hit_for_json(
            &log_file,
            &ExecutionSettings::create_integration_test_object(None, false),
        )
        .unwrap()
        .unwrap();

        // Step 3: Call the function
        let result = set_time_direction_by_scanning_json_file(&log_file, &mut identified_time_info);

        // Step 4: Assert success and expected direction
        assert!(result.is_ok());
        assert_eq!(
            identified_time_info.direction,
            Some(TimeDirection::Descending)
        ); // or whatever is expected
    }

    #[test]
    fn test_set_time_direction_by_scanning_json_file_1_line() {
        // Step 1: Create temporary log file with JSON lines
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_path_buf();

        // Create a few JSON lines with timestamps
        let json_lines = vec![json!({"timestamp": "2024-01-01T12:05:00Z"}).to_string()].join("\n");

        write(&file_path, json_lines).expect("Failed to write JSON lines to temp file");

        // Step 2: Construct the input structs
        let log_file = LogFile {
            log_type: LogType::Json, // Assuming a variant exists
            file_path: file_path.clone(),
        };

        let mut identified_time_info = try_to_get_timestamp_hit_for_json(
            &log_file,
            &ExecutionSettings::create_integration_test_object(None, false),
        )
        .unwrap()
        .unwrap();

        // Step 3: Call the function
        let result = set_time_direction_by_scanning_json_file(&log_file, &mut identified_time_info);

        // Step 4: Assert success and expected direction
        assert!(result.is_ok());
        assert_eq!(identified_time_info.direction, None); // if this is None (and all values are equal), this will get set to Descending at the higher level
    }

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
        let response = parse_json_line_into_json(json_str, 1);
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
        let response = parse_json_line_into_json(json_str, 1);
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
        let response = parse_json_line_into_json(json_str, 1).unwrap();
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
        let response = parse_json_line_into_json(json_str, 1).unwrap();
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
            input: PathBuf::from("/dummy/input"),
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

        let result =
            try_to_get_timestamp_hit_for_json_functionality(json_line.to_string(), &test_args)
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
            input: PathBuf::from("/dummy/input"),
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

        let result =
            try_to_get_timestamp_hit_for_json_functionality(json_line.to_string(), &test_args)
                .unwrap();

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.column_name, Some("/second_timestamp/test".to_string()));
        assert_eq!(info.regex_info.pretty_format, "YYYY-MM-DD HH:MM:SS");
    }
}
