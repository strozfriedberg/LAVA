use clap::{arg, ArgMatches, Command};
use log_checker::process_all_files;
mod main_helpers;
use main_helpers::get_full_command_line_args;

fn main() {
    let matches = Command::new("LogCheck")
        .version("1.0")
        .about("Tool to check the validity and completeness of a given log set.")
        .arg(arg!(-i --input <VALUE> "Input directory of log files to process.").required(true))
        .arg(arg!(-o --output <VALUE> "Output directory. Default [LogCheck_Output].").default_value("LogCheck_Output"))
        .get_matches();

    let command_line_args = get_full_command_line_args(&matches).unwrap(); // I think unwrap is fine here because I want to crash the program if I get an error here
    
    println!("Input directory: {:?}", command_line_args.input_dir);
    println!("Output directory: {:?}", command_line_args.output_dir);
    process_all_files(command_line_args)
}


