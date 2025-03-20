use glob::glob;

pub struct Config {
    pub query: String,
    pub file_path: String,
}

pub fn iterate_through_input_dir(input_dir:String){
    for entry in glob(input_dir.as_str()).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => println!("{:?}", path.display()),
            Err(e) => println!("{:?}", e),
        }
    }
}