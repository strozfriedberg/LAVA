use super::*;
use crate::basic_objects::HeaderInfo;
use crate::date_regex::DateRegex;
use csv::StringRecord;
use regex::Regex;
use std::io::Cursor;
use std::path::PathBuf;

#[test]
fn test_get_header_info_on_row_0() {
    let data = "\
        id,name,date\n\
        1,John,2025-05-09 10:00:00\n\
        2,Jane,2025-05-10 11:00:00\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);

    let result = get_header_info_functionality(&mut reader);

    let expected = HeaderInfo {
        first_data_row: 1,
        headers: StringRecord::from(vec!["id", "name", "date"]),
    };

    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_get_header_info_on_row_1() {
    let data = "\
        garbage\n\
        id,name,date\n\
        1,John,2025-05-09 10:00:00\n\
        2,Jane,2025-05-10 11:00:00\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);

    let result = get_header_info_functionality(&mut reader);

    let expected = HeaderInfo {
        first_data_row: 2,
        headers: StringRecord::from(vec!["id", "name", "date"]),
    };

    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_get_header_info_on_row_2() {
    let data = "\
        garbage\n\
        more,garbage\n\
        id,name,date\n\
        1,John,2025-05-09 10:00:00\n\
        2,Jane,2025-05-10 11:00:00\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);

    let result = get_header_info_functionality(&mut reader);

    let expected = HeaderInfo {
        first_data_row: 3,
        headers: StringRecord::from(vec!["id", "name", "date"]),
    };

    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_get_header_info_izzy_example_broken() {
    let data = r#"{"criteria":{},"exclusions":{},"query":"device_id:","time_range":{"start":"2024-10-26T14:12:59.042Z","end":"2025-04-24T14:12:59.042Z"},"rows":10000,"fields":["*"],"sort":[{"field":"device_timestamp","order":"DESC"}]}
alert_category,alert_id,blocked_effective_reputation,blocked_hash,blocked_name,backend_timestamp,device_external_ip,device_group,device_group_id,device_id,device_internal_ip,device_name,device_os,device_policy,device_policy_id,device_sensor_version,device_timestamp,enriched,enriched_event_type,legacy,observation_description
,,,,,2025-04-24T14:05:58.867Z,,,0,456774Test,,tsys\tsysdc1,WINDOWS,gpn - svr - win - nonprod - le,189117,,2025-04-24T14:04:22.972Z,TRUE,SYSTEM_API_CALL,,"The application ""c:\windows\system32\cmd.exe"" attempted to invoke the application ""c:\windows\system32\windowspowershell\v1.0\powershell.exe"", by calling the function ""CreateProcess"". The operation was successful."
,,,,,2025-04-24T14:00:42.338Z,,,0,456774Test,,tsys\tsysdc1,WINDOWS,gpn - svr - win - nonprod - le,189117,,2025-04-24T13:59:23.127Z,TRUE,SYSTEM_API_CALL,,"The application ""c:\windows\system32\cmd.exe"" attempted to invoke the application ""c:\windows\system32\windowspowershell\v1.0\powershell.exe"", by calling the function ""CreateProcess"". The operation was successful."
,,,,,2025-04-24T14:00:42.345Z,,,0,456774Test,,tsys\tsysdc1,WINDOWS,gpn - svr - win - nonprod - le,189117,,2025-04-24T13:56:44.911Z,TRUE,SYSTEM_API_CALL,,"The application ""c:\windows\system32\conhost.exe"" attempted to open the process ""c:\program files\confer\bladerunner.exe"", by calling the function ""OpenProcess"". The operation was successful."
,,,,,2025-04-24T14:00:42.699Z,,,0,456774Test,,tsys\tsysdc1,WINDOWS,gpn - svr - win - nonprod - le,189117,,2025-04-24T13:56:18.220Z,TRUE,SYSTEM_API_CALL,,"The application ""c:\windows\system32\conhost.exe"" attempted to open the process ""c:\program files\confer\bladerunner.exe"", by calling the function ""OpenProcess"". The operation was successful."
,,,,,2025-04-24T14:00:42.348Z,,,0,456774Test,,tsys\tsysdc1,WINDOWS,gpn - svr - win - nonprod - le,189117,,2025-04-24T13:56:04.357Z,TRUE,SYSTEM_API_CALL,,"The application ""c:\windows\system32\conhost.exe"" attempted to open the process ""c:\program files\confer\bladerunner.exe"", by calling the function ""OpenProcess"". The operation was successful."
,,,,,2025-04-24T14:00:42.217Z,,,0,456774Test,,tsys\tsysdc1,WINDOWS,gpn - svr - win - nonprod - le,189117,,2025-04-24T13:54:52.753Z,TRUE,SYSTEM_API_CALL,,"The application ""c:\windows\system32\cmd.exe"" attempted to invoke the application ""c:\windows\system32\netstat.exe"", by calling the function ""CreateProcess"". The operation was successful.""#;

    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);

    let result = get_header_info_functionality(&mut reader);

    let expected = HeaderInfo {
        first_data_row: 2,
        headers: StringRecord::from(vec![
            "alert_category",
            "alert_id",
            "blocked_effective_reputation",
            "blocked_hash",
            "blocked_name",
            "backend_timestamp",
            "device_external_ip",
            "device_group",
            "device_group_id",
            "device_id",
            "device_internal_ip",
            "device_name",
            "device_os",
            "device_policy",
            "device_policy_id",
            "device_sensor_version",
            "device_timestamp",
            "enriched",
            "enriched_event_type",
            "legacy",
            "observation_description",
        ]),
    };

    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_get_header_info_no_timestamp() {
    let data = "\
        garbage\n\
        id,name\n\
        1,John\n\
        2,Jane\n\
        4,James\n";

    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);

    let result = get_header_info_functionality(&mut reader);

    let expected = HeaderInfo {
        first_data_row: 2,
        headers: StringRecord::from(vec!["id", "name"]),
    };

    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_get_header_info_timestamp_but_not_consistent() {
    let data = "\
        garbage\n\
        id,name,irrelevant_date\n\
        1,John,\n\
        2,Jane,\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);

    let result = get_header_info_functionality(&mut reader);

    let expected = HeaderInfo {
        first_data_row: 2,
        headers: StringRecord::from(vec!["id", "name", "irrelevant_date"]),
    };

    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_get_header_info_less_than_5_rows() {
    let data = "\
        id,name,irrelevant_date\n\
        1,John,\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let mut reader = BufReader::new(cursor);

    let result = get_header_info_functionality(&mut reader);

    let expected = HeaderInfo {
        first_data_row: 1,
        headers: StringRecord::from(vec!["id", "name", "irrelevant_date"]),
    };

    assert_eq!(expected, result.unwrap());
}

#[test]
fn get_csv_timestamp_hit_finds_valid_timestamp() {
    let headers = StringRecord::from(vec!["id", "timestamp", "message"]);
    let record = StringRecord::from(vec!["1", "2024-05-10 10:23:00", "test log"]);

    let test_args = ExecutionSettings {
        input: PathBuf::from("/dummy/input"),
        output_dir: PathBuf::from("/dummy/output"),
        regexes: vec![DateRegex {
            pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(),
            strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
            regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
            function_to_call: None
        }],
        timestamp_field: None,
        quick_mode: false,
        multipart_mode: false,
        verbose_mode: true,
        actually_write_to_files: false,
    };
    let result =
        try_to_get_timestamp_hit_for_csv_functionality(headers.clone(), record.clone(), &test_args)
            .unwrap()
            .unwrap();

    assert_eq!(result.column_name, Some("timestamp".to_string()));
    assert_eq!(result.column_index, Some(1));
    assert_eq!(result.regex_info.pretty_format, "YYYY-MM-DD HH:MM:SS");
}

#[test]
fn get_csv_timestamp_hit_does_not_find_valid_timestamp() {
    let headers = StringRecord::from(vec!["id", "timestamp", "message"]);
    let record = StringRecord::from(vec!["1", "no timestamp", "test log"]);

    let test_args = ExecutionSettings {
        input: PathBuf::from("/dummy/input"),
        output_dir: PathBuf::from("/dummy/output"),
        regexes: vec![DateRegex {
            pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(),
            strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
            regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
            function_to_call: None
        }],
        timestamp_field: None,
        quick_mode: false,
        multipart_mode: false,
        verbose_mode: true,
        actually_write_to_files: false,
    };
    let result =
        try_to_get_timestamp_hit_for_csv_functionality(headers.clone(), record.clone(), &test_args);

    assert!(result.unwrap().is_none());
}

#[test]
fn get_csv_timestamp_hit_finds_valid_different_timestamp() {
    let headers = StringRecord::from(vec!["id", "timestamp", "message", "second_timestamp"]);
    let record = StringRecord::from(vec![
        "1",
        "2024-05-10 10:23:00",
        "test log",
        "2024-05-10 10:23:00",
    ]);

    let test_args = ExecutionSettings {
        input: PathBuf::from("/dummy/input"),
        output_dir: PathBuf::from("/dummy/output"),
        regexes: vec![DateRegex {
            pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(),
            strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
            regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
            function_to_call: None
        }],
        timestamp_field: Some("second_timestamp".to_string()),
        quick_mode: false,
        multipart_mode: false,
        verbose_mode: true,
        actually_write_to_files: false,
    };
    let result =
        try_to_get_timestamp_hit_for_csv_functionality(headers.clone(), record.clone(), &test_args)
            .unwrap()
            .unwrap();

    assert_eq!(result.column_name, Some("second_timestamp".to_string()));
    assert_eq!(result.column_index, Some(3));
    assert_eq!(result.regex_info.pretty_format, "YYYY-MM-DD HH:MM:SS");
}
