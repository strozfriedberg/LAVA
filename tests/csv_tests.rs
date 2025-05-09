use log_checker::*;
use std::io::{Cursor, BufReader};
// use regex::Regex;
use log_checker::handlers::csv_handlers::get_index_of_header_functionality;
include!(concat!(env!("OUT_DIR"), "/generated_regexes.rs"));
// use your_crate::{get_index_of_header_functionality, DateRegex, LogCheckError};

#[test]
fn test_get_index_of_header_functionality_in_memory() {
    // Create an in-memory CSV data (multiple rows)
    let data = "\
        id,name,date\n\
        1,John,2025-05-09 10:00:00\n\
        2,Jane,2025-05-10 11:00:00\n\
        4,James,2025-06-01 13:00:00\n";

    // Wrap the string data in a Cursor to simulate reading from a file
    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    // Call the function with the reader and regex
    let result = get_index_of_header_functionality(reader, &PREBUILT_DATE_REGEXES);

    // The first date match should occur in row 2 (index 1), which contains "2025-05-09"
    assert_eq!(result.unwrap(), 0); // 1 - 1 = 0 because we adjust by -1
}
