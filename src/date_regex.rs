use crate::errors::*;
use chrono::NaiveDateTime;
use regex::Regex;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct RawDateRegex {
    pub pretty_format: String,
    pub regex: String,
    pub strftime_format: String,
}

#[derive(Debug, Clone)]
pub struct DateRegex {
    pub pretty_format: String,
    pub strftime_format: String,
    pub regex: Regex,
}

impl DateRegex {
    pub fn new_from_raw_date_regex(input: RawDateRegex) -> Self {
        DateRegex {
            pretty_format: input.pretty_format,
            strftime_format: input.strftime_format,
            regex: Regex::new(&format!(r"({})", input.regex)).unwrap(),
        }
    }
    pub fn get_timestamp_object_from_string_contianing_date(
        &self,
        string_to_extract_from: String,
    ) -> Result<Option<NaiveDateTime>> {
        if let Some(captures) = self.regex.captures(&string_to_extract_from) {
            // Get the matched string (the datetime)
            if let Some(datetime_str) = captures.get(0) {
                let datetime_str = datetime_str.as_str();
                // Now, parse the extracted datetime string into NaiveDateTime using the strftime_format
                let parsed_datetime =
                    NaiveDateTime::parse_from_str(datetime_str, &self.strftime_format).map_err(
                        |e| LogCheckError::new(format!("Unable to parse timestamp because {e}")),
                    )?;
                return Ok(Some(parsed_datetime));
            }
        }
        return Ok(None); // regex did not capture any portion of the string
    }
    pub fn string_contains_date(&self, string_to_verify: &str) -> bool {
        if self.regex.is_match(&string_to_verify) {
            return true;
        }
        false
    }
}
