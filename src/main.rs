use clap::{arg, Command};


fn main() {

    let matches = Command::new("Log_checker")
        .version("1.0")
        .about("")
        .arg(arg!(-i --input <VALUE> "Input directory of log files to process").required(true))
        .get_matches();

    let input_dir = matches.get_one::<String>("input").expect("required");
    println!("two: {}", input_dir);
}