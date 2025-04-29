use serde::Deserialize;
use std::env;
use std::fmt;
use std::fs;
use std::path::Path;

#[derive(Deserialize)]
struct RawDateRegex {
    pretty_format: String,
    regex: String,
    strftime_format: String,
    test_input: String,
}

impl fmt::Display for RawDateRegex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.pretty_format, self.regex)
    }
}

fn main() {
    let yaml_path = Path::new("regexes.yml");
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generated_regexes.rs");
    let content = fs::read_to_string(yaml_path).expect("Failed to read YAML file");
    let parsed: Vec<RawDateRegex> = serde_yaml::from_str(&content).expect("Failed to parse YAML");
    let mut generated_code = String::new();
    generated_code.push_str("use once_cell::sync::Lazy;\n");
    generated_code.push_str("use regex::Regex;\n");
    generated_code.push_str("use crate::date_regex::DateRegex;\n\n");
    generated_code.push_str("pub static PREBUILT_DATE_REGEXES: Lazy<Vec<DateRegex>> = Lazy::new(|| {\n");
    generated_code.push_str("    vec![\n");

    for entry in parsed {
        // Write each item in the vec
        generated_code.push_str(&format!(
            "        DateRegex {{\n            pretty_format: \"{}\".to_string(),\n            strftime_format: \"{}\".to_string(),\n            regex: Regex::new(r\"({})\").unwrap(),\n        }},\n",
            entry.pretty_format,
            entry.strftime_format,
            entry.regex
        ));
    }

    generated_code.push_str("    ]\n});\n");

    fs::write(&dest_path, generated_code).expect("Failed to write generated_regexes.rs");

    println!("cargo:rerun-if-changed=regexes.yml");
    println!("cargo:rerun-if-changed=build.rs");
}
