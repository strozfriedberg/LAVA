use std::path::PathBuf;

use crate::basic_objects::*;
use crate::date_regex::DateRegex;
use crate::errors::*;
use crate::helpers::get_file_stem;
use crate::processing_objects::*;
use csv::StringRecord;
use evtx::{EvtxParser,SerializedEvtxRecord,ParserSettings};
use regex::Regex;

#[derive(Debug, Clone, Eq, PartialEq)]
struct Chunk {
    number: u64,
    starting_record_id: u64,
    ending_record_id: u64,
}

impl Chunk {
    pub fn new(number:u64, starting_record_id:u64, ending_record_id:u64) -> Self{
        Self { number, starting_record_id, ending_record_id }
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
    file_path: PathBuf
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
        for (number ,chunk) in parser.chunks().enumerate() {
            match chunk {
                Ok(r) => {
                    starting_chunk_list.push(Chunk::new(number as u64, r.header.first_event_record_id, r.header.last_event_record_id));
                    // println!("Chunk id {}, First Record ID in chunk {}, Last Record ID {}",number,  r.header.first_event_record_id, r.header.last_event_record_id),
                },            
                Err(e) => eprintln!("{}", e),
            }
        }
        starting_chunk_list.sort();
        // println!("{:?}", starting_chunk_list);
        Ok(Self {
            parser: parser,
            chunk_list: starting_chunk_list,
            file_path:path.to_path_buf()
        })
    }

    pub fn iterate_over_chunk(&mut self, chunk_id: u64) -> impl Iterator<Item=SerializedEvtxRecord<String>>{

        let find_chunk_result =  self.parser.find_next_chunk(chunk_id);
        if let Some(chunk_result) = find_chunk_result {
            let(result, number) = chunk_result;
            let settings = ParserSettings::new(); // play with this later
            // println!("Chunk number {}", number);
            if let Ok(mut successful) = result{
                let parsed_chunk_data = successful.parse(settings.into());
                if let Ok(mut parsed) = parsed_chunk_data{
                    return parsed
                    .iter()
                    .map(|event| event.unwrap().clone().into_xml().unwrap())
                    .collect::<Vec<_>>() // collect into Vec to satisfy lifetime
                    .into_iter();
                }
            }
        }
        Vec::new().into_iter()
    }

    pub fn iterate_over_all_records(&mut self) -> Vec<SerializedEvtxRecord<String>> {
        self.chunk_list.clone()
            .iter()
            .flat_map(|chunk| self.iterate_over_chunk(chunk.number).collect::<Vec<_>>())
            .collect()
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
    let evtx_parser = OrderedEvtxParser::new(&log_file.file_path); //Does this need to be mut?
    
    match evtx_parser {
        Ok(mut evtx_parser) => {
            let mut processing_object = LogRecordProcessor::new(
                timestamp_hit,
                execution_settings,
                get_file_stem(log_file)?,
                None,
                false,
            );
            println!("Before calling iterate over all records");
            for record in evtx_parser.iterate_over_all_records() {
                println!("in loop");
                    processing_object.process_record(LogFileRecord::new(
                            record.event_record_id as usize,
                            Some(record.timestamp.naive_utc()),
                            StringRecord::from(vec![
                                record.data,
                            ]),
                        ))?;
                // match record {
                //     Ok(clean_record) => {
                //         processing_object.process_record(LogFileRecord::new(
                //             clean_record.event_record_id as usize,
                //             Some(clean_record.timestamp.naive_utc()),
                //             StringRecord::from(vec![
                //                 clean_record.event_record_id.to_string(),
                //                 clean_record.data,
                //             ]),
                //         ))?;
                //     }
                //     Err(e) => {
                //         processing_object.add_error(LavaError::new(
                //             format!("Error reading EVTX record because of {}", e),
                //             LavaErrorLevel::Medium,
                //         ));
                //     }
                // }
            }
            Ok(processing_object)
        }
        Err(e) => Err(LavaError::new(
            format!("Failed to open evtx file because {}", e),
            LavaErrorLevel::Critical,
        )),
    }
}

#[cfg(test)]
mod evtx_handler_tests {

    use super::*;
    use evtx::EvtxParser;
    use evtx::ParserSettings;
    use std::str::FromStr;
    use std::{num, path::PathBuf};

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
        let test_file = LogFile{
            log_type: LogType::Evtx,
            file_path: PathBuf::from("C:\\cases\\rust_testing\\Logs\\Logs\\Security.evtx")
        };
        stream_evtx_file(&test_file,&Some(build_fake_evtx_timestamp_hit_internal()), &test_args);
    }
    #[test]
    fn test_evtx() {
        // Change this to a path of your .evtx sample.
        let fp = PathBuf::from("C:\\cases\\rust_testing\\Logs\\Logs\\Security.evtx");

        let mut parser = EvtxParser::from_path(fp).unwrap();
        let test =  parser.find_next_chunk(0);
        if let Some(test) = test {
            let(result, number) = test;
            let settings = ParserSettings::new();
            println!("Chunk number {}", number);
            if let Ok(mut successful) = result{
                let test23 = successful.parse(settings.into());
                if let Ok(mut parsed) = test23{
                    for event in parsed.iter().take(10){
                        if let Ok(event) = event{
                            println!("ID: {}", event.event_record_id);
                            // let test = event.clone().into_xml().unwrap()
                        }

                    }
                }
            }
        }
        for (number ,chunk) in parser.chunks().enumerate() {
            match chunk {
                Ok(r) => println!("Chunk id {}, First Record ID in chunk {}, Last Record ID {}",number,  r.header.first_event_record_id, r.header.last_event_record_id),
                Err(e) => eprintln!("{}", e),
            }
        }
    }
}
