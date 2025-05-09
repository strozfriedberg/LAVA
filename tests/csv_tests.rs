use log_checker::*;
use std::io::{Cursor, BufReader};
use log_checker::handlers::csv_handlers::get_index_of_header_functionality;
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

    let result = get_index_of_header_functionality(reader, &PREBUILT_DATE_REGEXES);

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

    let result = get_index_of_header_functionality(reader, &PREBUILT_DATE_REGEXES);

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

    let result = get_index_of_header_functionality(reader, &PREBUILT_DATE_REGEXES);

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

    let result = get_index_of_header_functionality(reader, &PREBUILT_DATE_REGEXES);

    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Could not find a supported timestamp format.");
    }
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

    let result = get_index_of_header_functionality(reader, &PREBUILT_DATE_REGEXES);

    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Could not find a supported timestamp format.");
    }
}
