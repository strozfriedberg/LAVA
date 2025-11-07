use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::get_file_stem;
use crate::processing_objects::*;
use csv::StringRecord;
use crate::date_regex::DateRegex;
use regex::Regex;
use evtx::EvtxParser;


pub fn get_fake_timestamp_hit_for_evtx() -> Result<Option<IdentifiedTimeInformation>> {
    let regex = Regex::new(".*").ok().unwrap(); // Match anything. This won't actually get used

    let fake_evtx_regex_info = DateRegex {
        pretty_format: "Binary EVTX".to_string(),
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

pub fn stream_evtx_file(
    log_file: &LogFile,
    timestamp_hit: &Option<IdentifiedTimeInformation>,
    execution_settings: &ExecutionSettings,
) -> Result<LogRecordProcessor> {

    let evtx_parser = EvtxParser::from_path(&log_file.file_path);//Does this need to be mut?

    match evtx_parser {
        Ok(mut evtx_parser) => {
            let mut processing_object = LogRecordProcessor::new(
                timestamp_hit,
                execution_settings,
                get_file_stem(log_file)?,
                None,
                false,
            );
            for record in evtx_parser.records() {
                match record {
                    Ok(clean_record) => {
                        processing_object.process_record(LogFileRecord::new(
                            clean_record.event_record_id as usize,
                            Some(clean_record.timestamp.naive_utc()),
                            StringRecord::from(vec![clean_record.event_record_id.to_string(),clean_record.data]),
                        ))?;
                    }
                    Err(e) => {
                        processing_object.add_error(LavaError::new(format!("Error reading EVTX record because of {}", e), LavaErrorLevel::Medium));
                    }
                }
            }
            Ok(processing_object)
        },
        Err(e) => Err(LavaError::new(format!("Failed to open evtx file because {}", e), LavaErrorLevel::Critical))
    }

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
        for record in parser.records().take(10) {
            match record {
                Ok(r) => println!("Record {}\n{}",r.timestamp, r.event_record_id),
                Err(e) => eprintln!("{}", e),
            }
        }
    }
}
