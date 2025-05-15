use csv::StringRecord;
use regex::Regex;

#[derive(Debug)]
pub struct RedactionRegex {
    pub name: String,
    pub pattern: Regex,
}

impl RedactionRegex {
    #[cfg(test)]
    pub fn string_contains_match(&self, string_to_verify: &str) -> bool {
        if self.pattern.is_match(&string_to_verify) {
            return true;
        }
        false
    }

    pub fn string_record_contains_match(&self, record: &StringRecord) -> bool {
        for field in record.iter() {
            if self.pattern.is_match(field) {
                return true;
            }
        }
        false
    }
}
#[cfg(test)]
mod redaction_tests {
    use super::*;
    use regex::Regex;

    #[test]
    fn matches_simple_date() {
        let pattern = RedactionRegex {
            name: "Simple Date".to_string(),
            pattern: Regex::new(r"\d{4}-\d{2}-\d{2}").unwrap(), // e.g. "2023-05-14"
        };

        assert!(pattern.string_contains_match("The date is 2023-05-14."));
    }

    #[test]
    fn does_not_match_when_no_date() {
        let pattern = RedactionRegex {
            name: "Simple Date".to_string(),
            pattern: Regex::new(r"\d{4}-\d{2}-\d{2}").unwrap(),
        };

        assert!(!pattern.string_contains_match("There is no date here."));
    }

    #[test]
    fn matches_datetime_format() {
        let pattern = RedactionRegex {
            name: "DateTime Format".to_string(),
            pattern: Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap(),
        };

        assert!(pattern.string_contains_match("Timestamp: 2024-12-31 23:59:59"));
    }

    #[test]
    fn empty_string_does_not_match() {
        let pattern = RedactionRegex {
            name: "Empty String Check".to_string(),
            pattern: Regex::new(r"\d{4}-\d{2}-\d{2}").unwrap(),
        };

        assert!(!pattern.string_contains_match(""));
    }
    #[test]
    fn matches_date_in_record() {
        let pattern = RedactionRegex {
            name: "Date".to_string(),
            pattern: Regex::new(r"\d{4}-\d{2}-\d{2}").unwrap(), // e.g. "2023-05-14"
        };

        let record = StringRecord::from(vec!["hello", "2023-05-14", "world"]);
        assert!(pattern.string_record_contains_match(&record));
    }

    #[test]
    fn no_match_in_record() {
        let pattern = RedactionRegex {
            name: "Date".to_string(),
            pattern: Regex::new(r"\d{4}-\d{2}-\d{2}").unwrap(),
        };

        let record = StringRecord::from(vec!["foo", "bar", "baz"]);
        assert!(!pattern.string_record_contains_match(&record));
    }

    #[test]
    fn matches_in_first_field() {
        let pattern = RedactionRegex {
            name: "Number".to_string(),
            pattern: Regex::new(r"\d+").unwrap(),
        };

        let record = StringRecord::from(vec!["12345", "abc", "xyz"]);
        assert!(pattern.string_record_contains_match(&record));
    }

    #[test]
    fn empty_record_does_not_match() {
        let pattern = RedactionRegex {
            name: "Anything".to_string(),
            pattern: Regex::new(r".+").unwrap(), // matches any non-empty string
        };

        let record = StringRecord::new(); // empty record
        assert!(!pattern.string_record_contains_match(&record));
    }

    #[test]
    fn match_in_last_field() {
        let pattern = RedactionRegex {
            name: "Masked Email".to_string(),
            pattern: Regex::new(r"[a-zA-Z]{1,3}\*{2,}@").unwrap(),
        };

        let record = StringRecord::from(vec!["user", "info", "jo**@domain.com"]);
        assert!(pattern.string_record_contains_match(&record));
    }
}
