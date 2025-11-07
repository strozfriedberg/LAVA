

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
