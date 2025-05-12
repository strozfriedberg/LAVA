use clap::{Command, arg};
use log_checker::process_all_files;
mod main_helpers;
use main_helpers::get_full_execution_settings;

fn main() {
    let matches = Command::new("LogCheck")
        .version("1.0")
        .about("Tool to check the validity and completeness of a given log set.")
        .arg(arg!(-i --input <VALUE> "Input directory of log files to process.").required(true))
        .arg(arg!(-o --output <VALUE> "Output directory.").default_value("LogCheck_Output"))
        .arg(arg!(-r --regexes <VALUE> "YML file with custom timestamp parsing to use. See Input_Regexes.yml for an example.").required(false))
        .arg(arg!(-t --tf <VALUE> "Timestamp field to use for time analysis. Supports -> for nested keys in JSONL.").required(false))
        .arg(arg!(-q --quick "Enable quick mode. Skips resource-intensive processing steps such as duplicate detection."))
        .get_matches();

    let execution_settings = get_full_execution_settings(&matches).unwrap(); // I think unwrap is fine here because I want to crash the program if I get an error here

    println!("Input directory: {:?}", execution_settings.input_dir);
    println!("Output directory: {:?}", execution_settings.output_dir);
    process_all_files(execution_settings)
}
