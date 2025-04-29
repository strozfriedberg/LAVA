use crate::errors::*;
use chrono::NaiveDateTime;
use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Debug, Clone)]
pub struct DateRegex {
    pub pretty_format: String,
    pub strftime_format: String,
    pub regex: Regex,
}

impl DateRegex {
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
        return Ok(None) // regex did not capture any portion of the string
    }

    pub fn get_timestamp_object_from_string_that_is_exact_date(
        &self,
        string_that_is_date: String,
    ) -> Result<NaiveDateTime> {
        let parsed_datetime =
            NaiveDateTime::parse_from_str(&string_that_is_date, &self.strftime_format).map_err(
                |e| LogCheckError::new(format!("Issue parsing timestamp because of {e}")),
            )?;
        Ok(parsed_datetime)
    }
}

pub static DATE_REGEXES: Lazy<Vec<DateRegex>> = Lazy::new(|| {
    //Need to make sure to put the more specific ones at the beinning so they get hits first
    vec![
        DateRegex {
            pretty_format: "Mon D, YYYY h:MM:SS AM/PM".to_string(), // Custom human-readable format
            regex: Regex::new(r"([A-Za-z]{3} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2} [AP]M)").unwrap(),
            strftime_format: "%b %e, %Y %l:%M:%S %p".to_string(),
        },        
        DateRegex {
            pretty_format: "date= time=".to_string(),
            regex: Regex::new(r"(date=\d{4}-\d{2}-\d{2}\s+time=\d{2}:\d{2}:\d{2})").unwrap(),
            strftime_format: "date=%Y-%m-%d time=%H:%M:%S".to_string(),
        },
        DateRegex {
            pretty_format: "YYYY-MM-DDTHH:MM:SS.SSS".to_string(),
            regex: Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,3})").unwrap(),
            strftime_format: "%Y-%m-%dT%H:%M:%S%.3f".to_string(),
        },
        DateRegex {
            pretty_format: "YYYY-MM-DD HH:MM:SS".to_string(), // 24-hour datetime
            regex: Regex::new(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap(),
            strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
        },
        DateRegex {
            pretty_format: "YYYY-MM-DDTHH:MM:SSZ".to_string(), // ISO 8601
            regex: Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)").unwrap(),
            strftime_format: "%Y-%m-%dT%H:%M:%SZ".to_string(),
        },
    ]
});
