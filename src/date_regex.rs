use crate::errors::*;
use chrono::NaiveDateTime;
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
        return Ok(None); // regex did not capture any portion of the string
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
