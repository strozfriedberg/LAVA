use log_checker::*;
use std::io::{Cursor, BufReader};
use csv::StringRecord;
use log_checker::handlers::csv_handlers::{get_index_of_header_functionality, try_to_get_timestamp_hit_for_csv_functionality};
include!(concat!(env!("OUT_DIR"), "/generated_regexes.rs"));

#[test]
fn test_get_index_of_header_on_row_0() {
    let data = "\
        id,name,date\n\
        1,John,2025-05-09 10:00:00\n\
        2,Jane,2025-05-10 11:00:00\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    let result = get_index_of_header_functionality(reader);

    assert_eq!(result.unwrap(), 0);
}

#[test]
fn test_get_index_of_header_on_row_1() {
    let data = "\
        garbage\n\
        id,name,date\n\
        1,John,2025-05-09 10:00:00\n\
        2,Jane,2025-05-10 11:00:00\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    let result = get_index_of_header_functionality(reader);

    assert_eq!(result.unwrap(), 1);
}

#[test]
fn test_get_index_of_header_on_row_2() {
    let data = "\
        garbage\n\
        more,garbage\n\
        id,name,date\n\
        1,John,2025-05-09 10:00:00\n\
        2,Jane,2025-05-10 11:00:00\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    let result = get_index_of_header_functionality(reader);

    assert_eq!(result.unwrap(), 2);
}

#[test]
fn test_get_index_of_header_no_timestamp() {
    let data = "\
        garbage\n\
        id,name\n\
        1,John\n\
        2,Jane\n\
        4,James\n";

    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    let result = get_index_of_header_functionality(reader);

    assert_eq!(result.unwrap(), 1);
}

#[test]
fn test_get_index_of_header_timestamp_but_not_consistent() {
    let data = "\
        garbage\n\
        id,name,irrelevant_date\n\
        1,John,\n\
        2,Jane,\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    let result = get_index_of_header_functionality(reader);

    assert_eq!(result.unwrap(), 1);
}

#[test]
fn test_get_index_of_header_less_than_5_rows() {
    let data = "\
        id,name,irrelevant_date\n\
        1,John,\n\
        4,James,2025-06-01 13:00:00\n";

    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    let result = get_index_of_header_functionality(reader);

    assert_eq!(result.unwrap(), 0);
}

#[test]
fn finds_valid_timestamp() {
    let headers = StringRecord::from(vec!["id", "timestamp", "message"]);
    let record = StringRecord::from(vec!["1", "2024-05-10 10:23:00", "test log"]);

    let regexes = 
        vec![
            DateRegex {
                pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(),
                strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
                regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
            },
        ];
    let result = try_to_get_timestamp_hit_for_csv_functionality(headers.clone(), record.clone(), &regexes).unwrap();

    assert_eq!(result.column_name, Some("timestamp".to_string()));
    assert_eq!(result.column_index, Some(1));
    assert_eq!(result.regex_info.pretty_format, "YYYY-MM-DD HH:MM:SS");
}