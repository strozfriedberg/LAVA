use clap::{Command, arg};
use log_checker::process_all_files;

fn main() {
    let matches = Command::new("LogCheck")
        .version("1.0")
        .about("Tool to check the validity and completeness of a given log set.")
        .arg(arg!(-i --input <VALUE> "Input directory of log files to process").required(true))
        .get_matches();

    let input_dir = format!(
        "{}/**/*",
        matches.get_one::<String>("input").expect("required")
    );

    process_all_files(input_dir)
}
