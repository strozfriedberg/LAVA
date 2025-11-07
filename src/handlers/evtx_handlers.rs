use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::get_file_stem;
use crate::processing_objects::*;
use csv::StringRecord;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use crate::date_regex::DateRegex;
use chrono::NaiveDateTime;
use regex::Regex;


pub fn get_fake_timestamp_hit_for_evtx() -> Result<Option<IdentifiedTimeInformation>> {
    let regex = Regex::new(".*").ok().unwrap(); // Match anything. This won't actually get used

    let fake_evtx_regex_info = DateRegex {
        pretty_format: "USING EVTX TIMESTAMP".to_string(),
        strftime_format: "FAKE_STRFTIME".to_string(),
        regex,
        function_to_call: None,
    };
    Ok(Some(IdentifiedTimeInformation {
        column_name: None,
        column_index: None,
        regex_info: fake_evtx_regex_info, 
        direction: Some(TimeDirection::Ascending),
    }))
}


#[cfg(test)]
mod evtx_handler_tests {

    use evtx::EvtxParser;
    use std::path::PathBuf;

    #[test]
    fn test_evtx() {
        // Change this to a path of your .evtx sample.
        let fp = PathBuf::from("C:\\cases\\rust_testing\\evtx\\Security.evtx");

        let mut parser = EvtxParser::from_path(fp).unwrap();
        for record in parser.records().take(5) {
            match record {
                Ok(r) => println!("Record {}\n{}",r.timestamp, r.event_record_id),
                Err(e) => eprintln!("{}", e),
            }
        }
    }
}
