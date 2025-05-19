use clap::{Command, arg};
use lava::main_helpers::get_full_execution_settings;
use lava::process_all_files;
use std::time::Instant;

fn main() {
    let start = Instant::now();
    print_ascii_art();
    let matches = Command::new("LAVA")
        .version("1.0")
        .about("Tool to check the validity and completeness of a given log set.")
        .arg(arg!(-i --input <PATH> "Input directory of log files to process.").required(true))
        .arg(arg!(-o --output <PATH> "Output directory.").default_value("LAVA_Output"))
        .arg(arg!(-r --regexes <PATH> "YML file with custom timestamp parsing to use. See Input_Regexes.yml for an example.").required(false))
        .arg(arg!(-t --tf <PATH> "Timestamp field to use for time analysis. Supports -> for nested keys in JSONL.").required(false))
        .arg(arg!(-q --quick "Enable quick mode. Skips resource-intensive processing steps such as duplicate detection."))
        .get_matches();

    let execution_settings = get_full_execution_settings(&matches).unwrap(); // I think unwrap is fine here because I want to crash the program if I get an error here

    println!("Input directory: {:?}", execution_settings.input_dir);
    println!("Output directory: {:?}", execution_settings.output_dir);
    process_all_files(execution_settings);

    let duration = start.elapsed();
    let minutes = duration.as_secs_f64() / 60.0;

    println!("Finished in {:.2} minutes", minutes);
}

pub fn print_ascii_art() {
    let art = r#"
██╗      █████╗ ██╗   ██╗ █████╗ 
██║     ██╔══██╗██║   ██║██╔══██╗
██║     ███████║██║   ██║███████║
██║     ██╔══██║╚██╗ ██╔╝██╔══██║
███████╗██║  ██║ ╚████╔╝ ██║  ██║
╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚═╝  ╚═╝

Log Anomaly and Validity Analyzer
By: Colin Meek
"#;

    println!("{}", art);
}
