use clap::{arg, Command};


fn main() {

    let matches = Command::new("LogCheck")
        .version("1.0")
        .about("Tool to check the validity and completeness of a given log set.")
        .arg(arg!(-i --input <VALUE> "Input directory of log files to process").required(true))
        .get_matches();

    let input_dir = matches.get_one::<String>("input").expect("required");
    println!("two: {}", input_dir);
}