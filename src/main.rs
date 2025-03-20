use clap::{arg, Command};
use glob::glob;


fn main() {

    let matches = Command::new("LogCheck")
        .version("1.0")
        .about("Tool to check the validity and completeness of a given log set.")
        .arg(arg!(-i --input <VALUE> "Input directory of log files to process").required(true))
        .get_matches();

    let input_dir = format!("{}/**/*", matches.get_one::<String>("input").expect("required"));

    iterate_through_input_dir(input_dir)
}

fn iterate_through_input_dir(input_dir:String){
    for entry in glob(input_dir.as_str()).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => println!("{:?}", path.display()),
            Err(e) => println!("{:?}", e),
        }
    }
}
