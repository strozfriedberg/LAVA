use std::path::PathBuf;

use crate::basic_objects::*;
use crate::date_regex::DateRegex;
use crate::errors::*;
use crate::helpers::get_file_stem;
use crate::processing_objects::*;
use csv::StringRecord;
use evtx::{EvtxParser, ParserSettings, SerializedEvtxRecord};
use regex::Regex;

#[derive(Debug, Clone, Eq, PartialEq)]
struct Chunk {
    number: u64,
    starting_record_id: u64,
    ending_record_id: u64,
}

impl Chunk {
    pub fn new(number: u64, starting_record_id: u64, ending_record_id: u64) -> Self {
        Self {
            number,
            starting_record_id,
            ending_record_id,
        }
    }
}
impl Ord for Chunk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.starting_record_id.cmp(&other.starting_record_id)
    }
}

impl PartialOrd for Chunk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct OrderedEvtxParser {
    parser: EvtxParser<std::fs::File>,
    chunk_list: Vec<Chunk>,
}

impl OrderedEvtxParser {
    /// Creates a new EvtxFile wrapper from a given path.
    pub fn new(path: &PathBuf) -> Result<Self> {
        let mut parser = EvtxParser::from_path(&path).map_err(|e| {
            LavaError::new(
                format!("Issue creating a EvtxParser because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?;
        let mut starting_chunk_list: Vec<Chunk> = Vec::new();

        //Enumerate chunks and sort them based on record_id
        for (number, chunk) in parser.chunks().enumerate() {
            match chunk {
                Ok(r) => {
                    starting_chunk_list.push(Chunk::new(
                        number as u64,
                        r.header.first_event_record_id,
                        r.header.last_event_record_id,
                    ));
                    // println!("Chunk id {}, First Record ID in chunk {}, Last Record ID {}",number,  r.header.first_event_record_id, r.header.last_event_record_id),
                }
                Err(e) => eprintln!("{}", e),
            }
        }
        starting_chunk_list.sort();
        // println!("{:?}", starting_chunk_list);
        Ok(Self {
            parser: parser,
            chunk_list: starting_chunk_list,
        })
    }

    /// Processes all records in order, calling the provided closure for each record.
    /// This is more memory-efficient than collecting all records into a Vec.
    pub fn process_all_records<F>(&mut self, mut process_record: F) -> Result<()>
    where
        F: FnMut(SerializedEvtxRecord<String>) -> Result<()>,
    {
        // Iterate over chunks in order without cloning the list
        for chunk in &self.chunk_list {
            let find_chunk_result = self.parser.find_next_chunk(chunk.number);
            if let Some(chunk_result) = find_chunk_result {
                let (result, _number) = chunk_result;
                if let Ok(mut successful) = result {
                    // Create settings for each chunk to avoid move issues
                    let settings = ParserSettings::new();
                    let parsed_chunk_data = successful.parse(settings.into());
                    if let Ok(mut parsed) = parsed_chunk_data {
                        // Process records in this chunk one at a time
                        for event_result in parsed.iter() {
                            match event_result {
                                Ok(event) => match event.clone().into_xml() {
                                    Ok(record) => process_record(record)?,
                                    Err(e) => eprintln!("Error converting event to XML: {}", e),
                                },
                                Err(e) => eprintln!("Error parsing event: {}", e),
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn get_fake_timestamp_hit_for_evtx_file() -> Result<Option<IdentifiedTimeInformation>> {
    Ok(Some(build_fake_evtx_timestamp_hit_internal()))
}
pub fn build_fake_evtx_timestamp_hit_internal() -> IdentifiedTimeInformation {
    let regex = Regex::new(".*").ok().unwrap(); // Match anything. This won't actually get used

    let fake_evtx_regex_info = DateRegex {
        pretty_format: "Binary EVTX".to_string(),
        strftime_format: "FAKE_STRFTIME".to_string(),
        regex,
        function_to_call: None,
    };
    IdentifiedTimeInformation {
        column_name: None,
        column_index: None,
        regex_info: fake_evtx_regex_info,
        direction: Some(TimeDirection::Ascending),
    }
}

pub fn stream_evtx_file(
    log_file: &LogFile,
    timestamp_hit: &Option<IdentifiedTimeInformation>,
    execution_settings: &ExecutionSettings,
) -> Result<LogRecordProcessor> {
    let use_custom_parser = true;

    let mut processing_object = LogRecordProcessor::new(
        timestamp_hit,
        execution_settings,
        get_file_stem(log_file)?,
        None,
        false,
    );
    match use_custom_parser {
        true => {
            let evtx_parser = OrderedEvtxParser::new(&log_file.file_path); //Does this need to be mut?
            match evtx_parser {
                Ok(mut evtx_parser) => {
                    // println!("Before calling iterate over all records");
                    evtx_parser.process_all_records(|record| {
                        processing_object.process_record(LogFileRecord::new(
                            record.event_record_id as usize,
                            Some(record.timestamp.naive_utc()),
                            StringRecord::from(vec![record.data]),
                        ))?;
                        Ok(())
                    })?;
                    Ok(processing_object)
                }
                Err(e) => Err(LavaError::new(
                    format!("Failed to open evtx file because {}", e),
                    LavaErrorLevel::Critical,
                )),
            }
        }
        false => {
            let mut parser = EvtxParser::from_path(&log_file.file_path).unwrap();
            for record in parser.records() {
                match record {
                    Ok(r) => {
                        processing_object.process_record(LogFileRecord::new(
                            r.event_record_id as usize,
                            Some(r.timestamp.naive_utc()),
                            StringRecord::from(vec![r.data]),
                        ))?;
                    }
                    Err(e) => eprintln!("{}", e),
                }
            }
            Ok(processing_object)
        }
    }
}

#[cfg(test)]
mod evtx_handler_tests {

    use super::*;
    use evtx::EvtxParser;
    use evtx::ParserSettings;
    use std::path::PathBuf;
    use std::fs;
    use std::str::FromStr;

    #[test]
    fn test_out_of_order_evtx_normal_crate_1() {
        let file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("samples")
                        .join("evtx")
                        .join("out_of_order")
                        .join("2-vss_0-Microsoft-Windows-RemoteDesktopServices-RdpCoreTS%4Operational.evtx");
        assert!(does_normal_evtx_crate_parse_in_order(&file_path), "2-vss_0-Microsoft-Windows-RemoteDesktopServices-RdpCoreTS%4Operational.evtx was IN order when parsed by normal crate");
    }

    fn does_normal_evtx_crate_parse_in_order(file_path: &PathBuf) -> bool {
        let mut parser = EvtxParser::from_path(&file_path).unwrap();
        let mut previous_record_id: Option<u64> = None;
        let mut out_of_order = false;
        for record in parser.records() {
            match record {
                Ok(r) => {
                    match previous_record_id {
                        Some(prev_id) => {
                            if r.event_record_id < prev_id {
                                out_of_order = true;
                                println!("Out of order detected: current ID {} is less than previous ID {} in {}`", r.event_record_id, prev_id, file_path.display());
                                break;
                            }
                            previous_record_id = Some(r.event_record_id);
                        }
                        None => {previous_record_id = Some(r.event_record_id);}
                    }
                }
                Err(e) => eprintln!("{}", e),
            }
        }
        out_of_order
    }

    #[test]
    fn test_stream_evtx() {
        let test_args = ExecutionSettings {
            input: PathBuf::from("/dummy/input"),
            output_dir: PathBuf::from("/dummy/output"),
            regexes: vec![DateRegex {
                pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(),
                strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
                regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
                function_to_call: None,
            }],
            timestamp_field: None,
            quick_mode: false,
            multipart_mode: false,
            verbose_mode: true,
            actually_write_to_files: false,
        };
        let test_file = LogFile {
            log_type: LogType::Evtx,
            file_path: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("samples")
                .join("evtx")
                .join("security.evtx"),
        };
        let _ = stream_evtx_file(
            &test_file,
            &Some(build_fake_evtx_timestamp_hit_internal()),
            &test_args,
        );
    }
    #[test]
    fn test_evtx() {
        // Change this to a path of your .evtx sample.
        let fp = PathBuf::from("C:\\cases\\rust_testing\\Logs\\Logs\\Security.evtx");

        let mut parser = EvtxParser::from_path(fp).unwrap();
        let test = parser.find_next_chunk(0);
        if let Some(test) = test {
            let (result, number) = test;
            let settings = ParserSettings::new();
            println!("Chunk number {}", number);
            if let Ok(mut successful) = result {
                let test23 = successful.parse(settings.into());
                if let Ok(mut parsed) = test23 {
                    for event in parsed.iter().take(10) {
                        if let Ok(event) = event {
                            println!("ID: {}", event.event_record_id);
                            // let test = event.clone().into_xml().unwrap()
                        }
                    }
                }
            }
        }
        for (number, chunk) in parser.chunks().enumerate() {
            match chunk {
                Ok(r) => println!(
                    "Chunk id {}, First Record ID in chunk {}, Last Record ID {}",
                    number, r.header.first_event_record_id, r.header.last_event_record_id
                ),
                Err(e) => eprintln!("{}", e),
            }
        }
    }
}
