use serde::Deserialize;
use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::path::Path;

#[derive(Deserialize)]
struct RawDateRegexWithTests {
    pretty_format: String,
    regex: String,
    strftime_format: String,
    test_input: String,
}

impl fmt::Display for RawDateRegexWithTests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.pretty_format, self.regex)
    }
}

#[derive(Deserialize)]
struct RawRedactionWithTests {
    name: String,
    pattern: String,
    should_match: String,
    should_not_match: String,
}

impl fmt::Display for RawRedactionWithTests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.name, self.pattern)
    }
}

fn main() {
    let regex_yaml_path = Path::new("regexes.yml");
    let content = fs::read_to_string(regex_yaml_path).expect("Failed to read YAML file");
    let parsed: Vec<RawDateRegexWithTests> =
        serde_yaml::from_str(&content).expect("Failed to parse YAML");

    let out_dir = env::var_os("OUT_DIR").unwrap();

    generate_date_regex_vector(&parsed, &out_dir);
    generate_date_regex_tests(&parsed, &out_dir);

    generate_

    println!("cargo:rerun-if-changed=redactions.yml");
    println!("cargo:rerun-if-changed=regexes.yml");
    println!("cargo:rerun-if-changed=build.rs");
}


fn generate_date_regex_vector(parsed: &Vec<RawDateRegexWithTests>, out_dir: &OsString) {

    let dest_path = Path::new(out_dir).join("generated_date_regexes.rs");

    let mut generated_code = String::new();
    generated_code.push_str("use once_cell::sync::Lazy;\n");
    generated_code.push_str("use regex::Regex;\n");
    generated_code.push_str("use crate::date_regex::DateRegex;\n\n");
    generated_code
        .push_str("pub static PREBUILT_DATE_REGEXES: Lazy<Vec<DateRegex>> = Lazy::new(|| {\n");
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

    fs::write(&dest_path, generated_code).expect("Failed to write generated_date_regexes.rs");
}

fn generate_date_regex_tests(parsed: &Vec<RawDateRegexWithTests>, out_dir: &OsString){
    let test_out_path = Path::new(&out_dir).join("generated_date_tests.rs");

    let mut test_code = String::new();
    test_code.push_str("#[cfg(test)]\n");
    test_code.push_str("mod generated_tests {\n");
    test_code.push_str("    use regex::Regex;\n");
    test_code.push_str("    use chrono::{NaiveDate, NaiveTime, NaiveDateTime};\n");
    test_code.push_str("    use crate::date_regex::DateRegex;\n\n");

    for (i, item) in parsed.iter().enumerate() {
        test_code.push_str("#[test]\n");
        test_code.push_str(&format!("fn test_regex_{}() {{\n", i,));
        test_code.push_str(&format!(
            "   let re = DateRegex {{\n            pretty_format: \"{}\".to_string(),\n            strftime_format: \"{}\".to_string(),\n            regex: Regex::new(r\"{}\").unwrap(),\n        }};\n",
            item.pretty_format,
            item.strftime_format,
            item.regex
        ));
        test_code.push_str("    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();\n");
        test_code.push_str("    let time = NaiveTime::from_hms_milli_opt(1, 0, 0, 0).unwrap();\n");
        test_code.push_str("    let expected_timestamp = NaiveDateTime::new(date, time);\n");
        test_code.push_str(&format!("    let actual_timestamp = re.get_timestamp_object_from_string_contianing_date(\"{}\".to_string()).unwrap().expect(\"Failed to get timestamp\");\n", item.test_input));
        test_code.push_str("    assert_eq!(expected_timestamp, actual_timestamp);\n");
        test_code.push_str("}\n");
    }

    test_code.push_str("}\n");

    fs::write(&test_out_path, test_code).expect("Failed to write generated_date_tests.rs");
}
