use clap::{Arg, ArgGroup, Command, arg};
use lava::main_helpers::{get_full_execution_settings, print_compiled_regexes};
use lava::{process_all_files, process_live_windows_event_logs};

fn main() {
    let _ = enable_ansi_support::enable_ansi_support(); // don't care about the output of this because if it fails then oh well 
    print_ascii_art();
    let mut command = Command::new("LAVA")
        .version("1.0")
        .about("Tool to check the validity and completeness of a given log set.")
        .arg(arg!(-i --input <PATH> "Input log file or directory. If a directory is provided, all log files within will be recusively processed."))
        .arg(arg!(-o --output <PATH> "Output directory.").default_value("LAVA_Output"))
        .arg(arg!(-p --printregexes "Print the built in timestamp formats."))
        .arg(arg!(-r --regexes <PATH> "YML file with custom timestamp formats to use. For formatting example run --printregexes."))
        .arg(arg!(-t --tf <PATH> "Timestamp field to use for time analysis. Supports -> for nested keys in JSONL."))
        .arg(arg!(-q --quick "Quick mode. Skips resource-intensive processing steps such as file hashing and duplicate detection."))
        .arg(arg!(-m --multipart "Multipart mode. Treats all input files as chunks of the same log."));
    // .disable_version_flag(true)

    #[cfg(windows)]
    {
        command = command
        .arg(Arg::new("live-windows")
            .short('w')
            .long("live-windows")
            .help("Live Windows mode. Scans event logs on the live system using the Windows API.")
            .action(clap::ArgAction::SetTrue))
            .group(
                ArgGroup::new("required")
                    .args(&["input", "printregexes", "help", "live-windows"])
                    .required(true)
                    .multiple(false),
            );
    }
    #[cfg(not(windows))]
    {
        command = command.group(
            ArgGroup::new("required")
                .args(&["input", "printregexes", "help", "live-windows"])
                .required(true)
                .multiple(false),
        );
    }

    command = command.arg(arg!(-v --verbose "Verbose mode.")); // Not implemented yet

    let matches = command.get_matches();
    if matches.get_flag("printregexes") {
        print_compiled_regexes();
    } else {
        let execution_settings = get_full_execution_settings(&matches).unwrap(); // I think unwrap is fine here because I want to crash the program if I get an error here
        if matches.get_flag("live-windows") {
            process_live_windows_event_logs(execution_settings);
        } else {
            process_all_files(execution_settings);
        }
    }
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
