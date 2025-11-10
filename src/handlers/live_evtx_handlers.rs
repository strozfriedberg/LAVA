use quick_xml::Reader;
use quick_xml::events::Event as XmlEvent;
use crate::processing_objects::*;
use std::{ffi::OsString, os::windows::ffi::OsStrExt, ptr::null_mut};
use windows::{
    Win32::Foundation::ERROR_NO_MORE_ITEMS,
    Win32::System::EventLog::{
        EVT_HANDLE, EVT_QUERY_FLAGS, EvtClose, EvtNext, EvtQuery, EvtQueryChannelPath, EvtRender, EvtNextChannelPath, EvtOpenChannelEnum,
        EvtRenderEventXml,
    },
    core::PCWSTR,
};

fn enumerate_event_logs() -> windows::core::Result<Vec<String>> {
    unsafe {
        // Open channel enumeration
        let enum_handle: EVT_HANDLE = EvtOpenChannelEnum(None, 0)?;

        let mut buffer: Vec<u16> = vec![0u16; 256];
        let mut buffer_used = 0u32;
        let mut all_windows_event_logs: Vec<String> = Vec::new();
        loop {
            let res = EvtNextChannelPath(
                enum_handle,
                Some(&mut buffer[..]),
                &mut buffer_used,
            );

            if let Err(err) = res {
                if err.code().0 as u32 == windows::Win32::Foundation::ERROR_NO_MORE_ITEMS.0 {
                    break;
                } else {
                    eprintln!("Error enumerating channels: {:?}", err);
                    break;
                }
            }

            // Convert UTF-16 buffer to Rust String
            let channel_name = String::from_utf16_lossy(&buffer[..(buffer_used as usize - 1)]);
            all_windows_event_logs.push(channel_name);
        }

        let _ = EvtClose(enum_handle);
        Ok(all_windows_event_logs)
    }
    
}

fn test_read_live_evtx(event_log_name: &str) -> windows::core::Result<()> {
    unsafe {
        println!("BEGINNINNG");
        // Convert channel name to wide string
        let channel_name: Vec<u16> = OsString::from(event_log_name)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // EVT_QUERY_REVERSE_DIRECTION = 0x1
        let flags = EVT_QUERY_FLAGS(0x200);

        // Open query on Security channel
        let query_handle = EvtQuery(None, PCWSTR(channel_name.as_ptr()), PCWSTR::null(), flags.0);
        match query_handle {
            Err(e) => println!("{}", e),
            Ok(query_handle) => {
                println!("Reading events from newest → oldest...");
                // let mut processing_object = LogRecordProcessor::new(
                //     timestamp_hit,
                //     execution_settings,
                //     get_file_stem(log_file)?,
                //     None,
                //     false,
                // );
                // Buffer for events
                let mut events: [isize; 16] = [0; 16];

                loop {
                    let mut returned = 0u32;

                    let next_result = EvtNext(query_handle, &mut events[..], 0, 0, &mut returned);

                    if let Err(err) = next_result {
                        if err.code() == ERROR_NO_MORE_ITEMS.into() {
                            println!("Reached the oldest event.");
                            break;
                        } else {
                            eprintln!("EvtNext failed: {:?}", err);
                            break;
                        }
                    }

                    for evt in events.iter().take(returned as usize) {
                        if let Some(xml) = render_event_xml(EVT_HANDLE(*evt)) {
                            if let Some(ts) = extract_timestamp_from_xml(&xml) {
                                println!("{}", ts);
                                // println!("{}",xml)
                            }
                        }
                        let _ = EvtClose(EVT_HANDLE(*evt));
                    }
                }

                let _ = EvtClose(query_handle);
            }
        }
    }

    Ok(())
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
fn extract_timestamp_from_xml(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Empty(e)) if e.name().as_ref() == b"TimeCreated" => {
                for a in e.attributes().flatten() {
                    if a.key.as_ref() == b"SystemTime" {
                        return Some(String::from_utf8_lossy(&a.value).to_string());
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

    #[test]
    fn test_live_evtx() {
        enumerate_event_logs();

        // test_read_live_evtx("System");
    }
}
