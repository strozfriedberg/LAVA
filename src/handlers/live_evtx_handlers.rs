use crate::basic_objects::*;
use crate::errors::*;
use crate::handlers::evtx_handlers::*;
use crate::port_stats_from_processing_object_into_processed_log_file;
use crate::processing_objects::*;
use chrono::NaiveDateTime;
use csv::StringRecord;
use quick_xml::Reader;
use quick_xml::events::Event as XmlEvent;
use std::{ffi::OsString, os::windows::ffi::OsStrExt};
use windows::{
    Win32::Foundation::ERROR_NO_MORE_ITEMS,
    Win32::Foundation::HANDLE,
    Win32::Security::{GetTokenInformation, TOKEN_ELEVATION, TOKEN_QUERY, TokenElevation},
    Win32::System::EventLog::{
        EVT_HANDLE, EVT_QUERY_FLAGS, EvtClose, EvtNext, EvtNextChannelPath, EvtOpenChannelEnum,
        EvtQuery, EvtQueryChannelPath, EvtRender, EvtRenderEventXml,
    },
    Win32::System::Threading::{GetCurrentProcess, OpenProcessToken},
    core::PCWSTR,
};

pub fn is_elevated() -> Result<bool> {
    unsafe {
        let mut token_handle = HANDLE(std::ptr::null_mut());
        let open_process_result =
            OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token_handle);
        if let Err(e) = open_process_result {
            return Err(LavaError::new(
                format!("Error calling OpenProcessToken because of {e}"),
                LavaErrorLevel::Critical,
            ));
        }

        let mut elevation = TOKEN_ELEVATION::default();
        let mut size = 0u32;

        let get_token_result = GetTokenInformation(
            token_handle,
            TokenElevation,
            Some(&mut elevation as *mut _ as *mut _),
            std::mem::size_of::<TOKEN_ELEVATION>() as u32,
            &mut size,
        );
        if let Err(e) = get_token_result {
            return Err(LavaError::new(
                format!("Error calling GetTokenInformation because of {e}"),
                LavaErrorLevel::Critical,
            ));
        }
        Ok(elevation.TokenIsElevated != 0)
    }
}

pub fn enumerate_event_logs() -> Result<Vec<String>> {
    unsafe {
        // Open channel enumeration
        let enum_handle = EvtOpenChannelEnum(None, 0);
        match enum_handle {
            Err(e) => {
                return Err(LavaError::new(
                    format!("Error opening EvtOpenChannelEnum because of {e}"),
                    LavaErrorLevel::Critical,
                ));
            }
            Ok(enum_handle) => {
                let mut buffer: Vec<u16> = vec![0u16; 256];
                let mut buffer_used = 0u32;
                let mut all_windows_event_logs: Vec<String> = Vec::new();
                loop {
                    let res =
                        EvtNextChannelPath(enum_handle, Some(&mut buffer[..]), &mut buffer_used);

                    if let Err(err) = res {
                        if err.code().0 as u32 == windows::Win32::Foundation::ERROR_NO_MORE_ITEMS.0
                        {
                            break;
                        } else {
                            eprintln!("Error enumerating channels: {:?}", err);
                            break;
                        }
                    }

                    // Convert UTF-16 buffer to Rust String
                    let channel_name =
                        String::from_utf16_lossy(&buffer[..(buffer_used as usize - 1)]);
                    all_windows_event_logs.push(channel_name);
                }

                let _ = EvtClose(enum_handle);
                Ok(all_windows_event_logs)
            }
        }
    }
}

pub fn process_live_evtx(
    event_log_name: &str,
    execution_settings: &ExecutionSettings,
) -> Result<ProcessedLogFile> {
    unsafe {
        // Convert channel name to wide string
        let channel_name: Vec<u16> = OsString::from(event_log_name)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // EVT_QUERY_REVERSE_DIRECTION = 0x200
        let flags = EVT_QUERY_FLAGS(0x200);

        // Open query on provided channel
        let query_handle = EvtQuery(None, PCWSTR(channel_name.as_ptr()), PCWSTR::null(), flags.0);
        match query_handle {
            Err(e) => {
                println!("ERROR on {} {}",event_log_name,LavaError::new(
                    format!("Error opening EvtQuery because of {e}"),
                    LavaErrorLevel::Critical,
                ));
                return  Ok(ProcessedLogFile::default());
            }
            Ok(query_handle) => {
                println!("Processing {}", event_log_name);
                let mut base_processed_file = ProcessedLogFile::default();
                base_processed_file.filename = Some(event_log_name.to_string());
                base_processed_file.file_path = Some(format!("C:\\Windows\\System32\\winevt\\Logs\\{}", event_log_name));
                let mut processing_object = LogRecordProcessor::new(
                    &Some(build_fake_evtx_timestamp_hit_internal()),
                    execution_settings,
                    event_log_name.to_string(),
                    None,
                    false,
                );
                // Buffer for events
                let mut events: [isize; 16] = [0; 16];

                loop {
                    let mut returned = 0u32;

                    let next_result = EvtNext(query_handle, &mut events[..], 0, 0, &mut returned);

                    if let Err(err) = next_result {
                        if err.code() == ERROR_NO_MORE_ITEMS.into() {
                            break;
                        } else {
                            eprintln!("EvtNext failed: {:?}", err);
                            break;
                        }
                    }

                    for evt in events.iter().take(returned as usize) {
                        if let Some(xml) = render_event_xml(EVT_HANDLE(*evt)) {
                            if let Some(ts) = extract_timestamp_from_xml(&xml) {
                                processing_object.process_record(LogFileRecord::new(
                                    extract_record_id_from_xml(&xml).unwrap() as usize,
                                    Some(ts),
                                    StringRecord::from(vec![xml]),
                                ))?;
                                // println!("{}", ts);
                                // println!("{}",xml)
                            }
                        }
                        let _ = EvtClose(EVT_HANDLE(*evt));
                    }
                }

                let _ = EvtClose(query_handle);
                port_stats_from_processing_object_into_processed_log_file(
                    &execution_settings,
                    &mut base_processed_file,
                    None,
                    processing_object,
                );
                return Ok(base_processed_file);
            }
        }
    }
}

/// Renders an event as XML string
unsafe fn render_event_xml(event: EVT_HANDLE) -> Option<String> {
    let mut buffer_used = 0u32;
    let mut property_count = 0u32;

    // First call to get buffer size
    let _ = EvtRender(
        None,
        event,
        EvtRenderEventXml.0,
        0,
        None,
        &mut buffer_used,
        &mut property_count,
    );

    let mut buffer = vec![0u16; (buffer_used / 2 + 1) as usize];

    if EvtRender(
        None,
        event,
        EvtRenderEventXml.0,
        buffer_used,
        Some(buffer.as_mut_ptr() as *mut _),
        &mut buffer_used,
        &mut property_count,
    )
    .is_ok()
    {
        Some(String::from_utf16_lossy(
            &buffer[..(buffer_used / 2) as usize],
        ))
    } else {
        None
    }
}

/// Extracts the timestamp from Event XML
// fn extract_timestamp_from_xml(xml: &str) -> Option<NaiveDateTime> {
//     let mut reader = Reader::from_str(xml);
//     reader.config_mut().trim_text(true);
//     let mut buf = Vec::new();
//     loop {
//         match reader.read_event_into(&mut buf) {
//             Ok(XmlEvent::Empty(e)) if e.name().as_ref() == b"TimeCreated" => {
//                 for a in e.attributes().flatten() {
//                     if a.key.as_ref() == b"SystemTime" {
//                         return Some(NaiveDateTime::parse_from_str(&String::from_utf8_lossy(&a.value).to_string(), "%Y-%m-%dT%H:%M:%S%.fZ").unwrap());
//                     }
//                 }
//             }
//             Ok(XmlEvent::Eof) => break,
//             Err(_) => break,
//             _ => {}
//         }
//         buf.clear();
//     }

//     None
// }

fn extract_timestamp_from_xml(xml: &str) -> Option<NaiveDateTime> {
    let system_time = extract_field_from_xml(xml, b"TimeCreated", Some(b"SystemTime"))?;
    NaiveDateTime::parse_from_str(&system_time, "%Y-%m-%dT%H:%M:%S%.fZ").ok()
}

fn extract_record_id_from_xml(xml: &str) -> Option<u64> {
    let record_id_str = extract_field_from_xml(xml, b"EventRecordID", None)?;
    record_id_str.parse::<u64>().ok()
}

fn extract_field_from_xml(
    xml: &str,
    element_name: &[u8],
    attribute_name: Option<&[u8]>,
) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Empty(e)) | Ok(XmlEvent::Start(e))
                if e.name().as_ref() == element_name =>
            {
                if let Some(attr_name) = attribute_name {
                    for a in e.attributes().flatten() {
                        if a.key.as_ref() == attr_name {
                            return Some(String::from_utf8_lossy(&a.value).to_string());
                        }
                    }
                } else {
                    if let Ok(text) = reader.read_text(e.name()) {
                        return Some(text.into_owned());
                    }
                }
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    None
}

#[cfg(test)]
mod evtx_handler_tests {

    use super::*;
    use std::path::PathBuf;
    use crate::date_regex::DateRegex;
    use regex::Regex;

    #[test]
    fn test_live_evtx() {
        // println!("{}", is_elevated().unwrap())
        // enumerate_event_logs();
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
        process_live_evtx("System", &test_args);
    }
}
