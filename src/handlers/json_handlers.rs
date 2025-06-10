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
        if let Some(field_to_use) = &execution_settings.timestamp_field {
            //split the string based on ->
        } else {

        }
    }

    Ok(None)
}

#[cfg(test)]
mod json_handler_tests {

    use super::*;
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
    }
}
