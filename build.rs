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
    should_match: Vec<String>,
    should_not_match: Vec<String>,
    function_to_call: Option<String>,
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
    should_match: Vec<String>,
    should_not_match: Vec<String>,
}

impl fmt::Display for RawRedactionWithTests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.name, self.pattern)
    }
}

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    let date_regex_yaml_path = Path::new("build_yml_files/dates.yml");
    let date_regex_content =
        fs::read_to_string(date_regex_yaml_path).expect("Failed to read YAML file");
    let date_regex_parsed: Vec<RawDateRegexWithTests> =
        serde_yaml::from_str(&date_regex_content).expect("Failed to parse YAML");

    generate_date_regex_vector(&date_regex_parsed, &out_dir);
    generate_date_regex_tests(&date_regex_parsed, &out_dir);

    let redactions_regex_yaml_path = Path::new("build_yml_files/redactions.yml");
    let redactions_regex_content =
        fs::read_to_string(redactions_regex_yaml_path).expect("Failed to read YAML file");
    let redactions_regex_parsed: Vec<RawRedactionWithTests> =
        serde_yaml::from_str(&redactions_regex_content).expect("Failed to parse YAML");

    generate_redactions_regex_vector(&redactions_regex_parsed, &out_dir);
    generate_redactions_regex_tests(&redactions_regex_parsed, &out_dir);

    println!("cargo:rerun-if-changed=redactions.yml");
    println!("cargo:rerun-if-changed=regexes.yml");
    println!("cargo:rerun-if-changed=build.rs");
}

fn generate_redactions_regex_vector(parsed: &Vec<RawRedactionWithTests>, out_dir: &OsString) {
    let dest_path = Path::new(out_dir).join("generated_redaction_regexes.rs");

    let mut generated_code = String::new();
    generated_code.push_str("use once_cell::sync::Lazy;\n");
    generated_code.push_str("use regex::Regex;\n");
    generated_code.push_str("use crate::redaction_regex::RedactionRegex;\n\n");
    generated_code.push_str(
        "pub static PREBUILT_REDACTION_REGEXES: Lazy<Vec<RedactionRegex>> = Lazy::new(|| {\n",
    );
    generated_code.push_str("    vec![\n");

    for entry in parsed {
        // Write each item in the vec
        generated_code.push_str(&format!(
            "        RedactionRegex {{\n            name: \"{}\".to_string(),\n            pattern: Regex::new(r\"{}\").unwrap(),\n        }},\n",
            entry.name,
            entry.pattern,
        ));
    }

    generated_code.push_str("    ]\n});\n");

    fs::write(&dest_path, generated_code).expect("Failed to write generated_redaction_regexes.rs");
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
            "        DateRegex {{\n            pretty_format: \"{}\".to_string(),\n            strftime_format: \"{}\".to_string(),\n            regex: Regex::new(r\"({})\").unwrap(),\n            function_to_call: {}\n         }},\n",
            entry.pretty_format,
            entry.strftime_format,
            entry.regex,
            match entry.function_to_call.clone() {
                Some(function)=> format!("Some(\"{}\".to_string())",function),
                None => "None".to_string()
                
            }
        ));
    }

    generated_code.push_str("    ]\n});\n");

    fs::write(&dest_path, generated_code).expect("Failed to write generated_date_regexes.rs");
}

fn generate_date_regex_tests(parsed: &Vec<RawDateRegexWithTests>, out_dir: &OsString) {
    let test_out_path = Path::new(&out_dir).join("generated_date_tests.rs");

    let mut test_code = String::new();
    test_code.push_str("#[cfg(test)]\n");
    test_code.push_str("mod generated_date_regex_tests {\n");
    test_code.push_str("    use regex::Regex;\n");
    test_code.push_str("    use chrono::{NaiveDate, NaiveTime, NaiveDateTime};\n");
    test_code.push_str("    use crate::date_regex::DateRegex;\n\n");

    for (i, item) in parsed.iter().enumerate() {
        for (should_match_index, should_match_value) in item.should_match.iter().enumerate() {
            test_code.push_str("#[test]\n");
            test_code.push_str(&format!(
                "fn generated_test_date_regex_{}_should_match_{}() {{\n",
                i, should_match_index
            ));
            test_code.push_str(&format!(
            "   let re = DateRegex {{\n            pretty_format: \"{}\".to_string(),\n            strftime_format: \"{}\".to_string(),\n            regex: Regex::new(r\"{}\").unwrap(),\n            function_to_call: {}\n         }};\n",
            item.pretty_format,
            item.strftime_format,
            item.regex,
            match item.function_to_call.clone() {
                Some(function)=> format!("Some(\"{}\".to_string())",function),
                None => "None".to_string()
                
            }
        ));
            test_code.push_str("    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();\n");
            test_code
                .push_str("    let time = NaiveTime::from_hms_milli_opt(1, 0, 0, 0).unwrap();\n");
            test_code.push_str("    let expected_timestamp = NaiveDateTime::new(date, time);\n");
            test_code.push_str(&format!("    let actual_timestamp = re.get_timestamp_object_from_string_contianing_date(\"{}\".to_string()).unwrap().expect(\"Failed to get timestamp\");\n", should_match_value));
            test_code.push_str("    assert_eq!(expected_timestamp, actual_timestamp);\n");
            test_code.push_str("}\n");
        }
        for (should_match_not_index, should_not_match_value) in
            item.should_not_match.iter().enumerate()
        {
            test_code.push_str("#[test]\n");
            test_code.push_str(&format!(
                "fn generated_test_date_regex_{}_should_not_match_{}() {{\n",
                i, should_match_not_index
            ));
            test_code.push_str(&format!(
            "   let re = DateRegex {{\n            pretty_format: \"{}\".to_string(),\n            strftime_format: \"{}\".to_string(),\n            regex: Regex::new(r\"{}\").unwrap(),\n            function_to_call: {}\n        }};\n",
            item.pretty_format,
            item.strftime_format,
            item,
            match item.function_to_call.clone() {
                Some(function)=> format!("Some(\"{}\".to_string())",function),
                None => "None".to_string()
                
            }
        ));
            test_code.push_str(&format!(
                r#"     match re.get_timestamp_object_from_string_contianing_date("{}".to_string()) {{
            Ok(maybe_date) => {{
                match maybe_date {{
                    Some(hit) => {{
                        let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
                        let time = NaiveTime::from_hms_milli_opt(1, 0, 0, 0).unwrap();
                        let expected_timestamp = NaiveDateTime::new(date, time);
                        assert_ne!(hit, expected_timestamp);
                    }},
                    None => assert!(true)
                }}
            }},
            Err(e) => panic!("{{}}",e)
        }}"#,
                should_not_match_value
            ));
            test_code.push_str("}\n");
        }
    }

    test_code.push_str("}\n");

    fs::write(&test_out_path, test_code).expect("Failed to write generated_date_tests.rs");
}

fn generate_redactions_regex_tests(parsed: &Vec<RawRedactionWithTests>, out_dir: &OsString) {
    let test_out_path = Path::new(&out_dir).join("generated_redactions_tests.rs");

    let mut test_code = String::new();
    test_code.push_str("#[cfg(test)]\n");
    test_code.push_str("mod generated_redactions_tests {\n");
    test_code.push_str("    use regex::Regex;\n");
    test_code.push_str("    use crate::redaction_regex::RedactionRegex;\n\n");

    for item in parsed.iter() {
        // Write affermative tests
        for (match_number, test_value) in item.should_match.iter().enumerate() {
            test_code.push_str("#[test]\n");
            test_code.push_str(&format!(
                "fn test_redaction_{}_should_match_{}() {{\n",
                item.name.trim().replace(' ', "_").to_lowercase(),
                match_number,
            ));
            test_code.push_str(&format!(
                "   let re = RedactionRegex {{\n            name: \"{}\".to_string(),\n            pattern: Regex::new(r\"{}\").unwrap(),\n        }};\n",
                item.name,
                item.pattern,
            ));
            test_code.push_str(&format!(
                "    assert!(re.string_contains_match(\"{}\"));\n",
                test_value
            ));
            test_code.push_str("}\n");
        }

        for (match_number, test_value) in item.should_not_match.iter().enumerate() {
            test_code.push_str("#[test]\n");
            test_code.push_str(&format!(
                "fn test_redaction_{}_should_not_match_{}() {{\n",
                item.name.trim().replace(' ', "_").to_lowercase(),
                match_number,
            ));
            test_code.push_str(&format!(
                "   let re = RedactionRegex {{\n            name: \"{}\".to_string(),\n            pattern: Regex::new(r\"{}\").unwrap(),\n        }};\n",
                item.name,
                item.pattern,
            ));
            test_code.push_str(&format!(
                "    assert!(!re.string_contains_match(\"{}\"));\n",
                test_value
            ));
            test_code.push_str("}\n");
        }
    }

    test_code.push_str("}\n");

    fs::write(&test_out_path, test_code).expect("Failed to write generated_date_tests.rs");
}
