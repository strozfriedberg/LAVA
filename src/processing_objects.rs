use crate::basic_objects::*;
use crate::errors::*;
use chrono::NaiveDateTime;
use csv::StringRecord;
use csv::WriterBuilder;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::path::PathBuf;
use welford::Welford;
include!(concat!(env!("OUT_DIR"), "/generated_redaction_regexes.rs"));

#[cfg(test)]
mod tests {
    mod build_file_path_tests;
    mod direction_checker_tests;
    mod dupe_processing_tests;
    mod redaction_processing_tests;
    mod timestamp_processing_tests;
}

#[derive(PartialEq, Debug, Default)]
pub struct TimeDirectionChecker {
    pub previous: Option<NaiveDateTime>,
}

impl TimeDirectionChecker {
    pub fn process_timestamp(&mut self, current_datetime: NaiveDateTime) -> Option<TimeDirection> {
        if let Some(previous_datetime) = self.previous {
            if current_datetime > previous_datetime {
                // println!("Current datetime {} is after previous {}. Order is Ascending!", current_datetime.format("%Y-%m-%d %H:%M:%S").to_string(), previous_datetime.format("%Y-%m-%d %H:%M:%S").to_string());
                return Some(TimeDirection::Ascending);
            } else if current_datetime < previous_datetime {
                // println!("Current datetime {} is before previous {}. Order is Descending!", current_datetime.format("%Y-%m-%d %H:%M:%S").to_string(), previous_datetime.format("%Y-%m-%d %H:%M:%S").to_string());
                return Some(TimeDirection::Descending);
            }
        } else {
            self.previous = Some(current_datetime);
        }
        return None;
    }
}

#[derive(Default)]
pub struct LogRecordProcessor {
    pub order: Option<TimeDirection>,
    pub execution_settings: ExecutionSettings,
    pub file_name: String,
    pub data_field_headers: StringRecord,
    pub total_num_records: usize,
    pub timestamp_num_records: usize,
    pub min_timestamp: Option<NaiveDateTime>,
    pub max_timestamp: Option<NaiveDateTime>,
    pub previous_timestamp: Option<NaiveDateTime>,
    pub largest_time_gap: Option<TimeGap>,
    pub duplicate_checker_set: HashSet<u64>,
    pub num_dupes: usize,
    pub num_redactions: usize,
    pub errors: Vec<LavaError>,
    pub welford_calculator: Welford<i128>,
    process_timestamps: bool,
}

impl LogRecordProcessor {
    pub fn new(
        timestamp_hit: &Option<IdentifiedTimeInformation>,
        execution_settings: &ExecutionSettings,
        log_file_stem: String,
        headers: Option<StringRecord>,
    ) -> Self {
        let data_field_headers = match headers {
            Some(csv_headers) => csv_headers,
            None => StringRecord::from(vec!["Record"]),
        };
        let process_timestamps = match timestamp_hit {
            Some(hit) => match hit.direction {
                Some(_) => true,
                None => false,
            },
            None => false,
        };
        let order = match timestamp_hit {
            Some(hit) => hit.direction.clone(),
            None => None,
        };
        Self {
            order,
            execution_settings: execution_settings.clone(),
            file_name: log_file_stem,
            data_field_headers: data_field_headers,
            process_timestamps: process_timestamps,
            ..Default::default()
        }
    }
    pub fn process_record(&mut self, record: LogFileRecord) -> Result<()> {
        self.total_num_records += 1;

        if !self.execution_settings.quick_mode {
            self.process_record_for_dupes(&record)?;
            self.process_record_for_redactions(&record)?;
        }

        if self.process_timestamps {
            self.process_timestamp(&record)?;
        }

        Ok(())
    }

    pub fn process_record_for_dupes(&mut self, record: &LogFileRecord) -> Result<()> {
        let is_duplicate = !self
            .duplicate_checker_set
            .insert(record.hash_of_entire_record);
        if is_duplicate {
            // println!("Found duplicate record at index {}", record.index);
            self.num_dupes += 1;
            if self.execution_settings.actually_write_to_files {
                match self.write_hit_to_file(record, AlertOutputType::Duplicate, None) {
                    Ok(()) => (),
                    Err(e) => self.errors.push(e),
                }
            }
        }
        Ok(())
    }
    pub fn process_record_for_redactions(&mut self, record: &LogFileRecord) -> Result<()> {
        for redaction in PREBUILT_REDACTION_REGEXES.iter() {
            if redaction.string_record_contains_match(&record.raw_record) {
                self.num_redactions += 1;
                // println!("Found redaction in record {:?}", record.raw_record);
                if self.execution_settings.actually_write_to_files {
                    match self.write_hit_to_file(
                        record,
                        AlertOutputType::Redaction,
                        Some(redaction.name.clone()),
                    ) {
                        Ok(()) => (),
                        Err(e) => self.errors.push(e),
                    }
                }
            }
        }

        Ok(())
    }
    pub fn write_hit_to_file(
        &mut self,
        record: &LogFileRecord,
        alert_type: AlertOutputType,
        rule_name: Option<String>,
    ) -> Result<()> {
        let output_file = self.build_file_path(&alert_type)?;
        let file_existed_before = output_file.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_file)
            .map_err(|e| {
                LavaError::new(
                    format!("Unable to create output file because of {e}"),
                    LavaErrorLevel::Medium,
                )
            })?;

        let mut writer = WriterBuilder::new()
            .has_headers(false) // Disable automatic header writing
            .from_writer(file);

        if !file_existed_before {
            writer
                .write_record(&self.get_full_output_headers_based_on_alert_type(&alert_type))
                .map_err(|e| {
                    LavaError::new(
                        format!("Unable to write headers to file because of {e}"),
                        LavaErrorLevel::Medium,
                    )
                })?;
        }
        writer
            .write_record(&record.get_record_to_output(&alert_type, rule_name))
            .map_err(|e| {
                LavaError::new(
                    format!("Unable to write record because of {e}"),
                    LavaErrorLevel::Medium,
                )
            })?;
        Ok(())
    }

    fn get_full_output_headers_based_on_alert_type(
        &self,
        alert_type: &AlertOutputType,
    ) -> StringRecord {
        let mut full_output_headers = match alert_type {
            AlertOutputType::Duplicate => {
                StringRecord::from(vec!["Index of Hit", "Hash of Record"])
            }
            AlertOutputType::Redaction => {
                StringRecord::from(vec!["Index of Hit", "Rule Name"]) // Maybe in the future add the name of the rule that hit in the second column
            }
        };

        full_output_headers.extend(self.data_field_headers.iter());
        full_output_headers
    }

    pub fn build_file_path(&self, alert_type: &AlertOutputType) -> Result<PathBuf> {
        let execution_settings = self.execution_settings.clone();

        let output_subfolder_and_filename = match alert_type {
            AlertOutputType::Duplicate => format!("Duplicates/{}_DUPLICATES.csv", self.file_name),
            AlertOutputType::Redaction => {
                format!("Redactions/{}_POSSIBLE_REDACTIONS.csv", self.file_name)
            }
        };

        Ok(execution_settings
            .output_dir
            .join(output_subfolder_and_filename))
    }

    fn handle_first_out_of_order_timestamp(&mut self, record: &LogFileRecord) {
        self.process_timestamps = false;
        self.timestamp_num_records = 0;
        self.min_timestamp = None;
        self.max_timestamp = None;
        self.largest_time_gap = None;
        println!(
            "\x1b[31m{} was not sorted on the identified timestamp. Out of order record at index {}\x1b[0m",
            self.file_name, record.index
        );
        self.errors.push(LavaError::new(
            format!(
                "File was not sorted on the identified timestamp. Out of order record at index {}",
                record.index
            ),
            LavaErrorLevel::Medium,
        ));
    }

    pub fn process_timestamp(&mut self, record: &LogFileRecord) -> Result<()> {
        let current_timestamp = match record.timestamp {
            Some(timestamp) => timestamp,
            None => {
                self.errors.push(LavaError::new(
                    format!(
                        "The identified timestamp format could not be parsed from the timestamp field at index {}",
                        record.index,
                    ),
                    LavaErrorLevel::Medium,
                ));
                return Ok(());
            }
        };
        self.timestamp_num_records += 1;

        if let Some(previous_datetime) = self.previous_timestamp {
            // This is where all logic is done if it isn't the first record
            if self.order == Some(TimeDirection::Ascending) {
                if previous_datetime > current_timestamp {
                    self.handle_first_out_of_order_timestamp(record);
                    return Ok(());
                }
                self.max_timestamp = Some(current_timestamp)
            } else if self.order == Some(TimeDirection::Descending) {
                if previous_datetime < current_timestamp {
                    self.handle_first_out_of_order_timestamp(record);
                    return Ok(());
                }
                self.min_timestamp = Some(current_timestamp)
            }
            let current_time_gap = TimeGap::new(previous_datetime, current_timestamp);
            self.welford_calculator
                .push(current_time_gap.get_time_duration_number() as i128);
            if let Some(largest_time_gap) = self.largest_time_gap {
                if current_time_gap > largest_time_gap {
                    self.largest_time_gap =
                        Some(TimeGap::new(previous_datetime, current_timestamp));
                }
            } else {
                // This is the second row, intialize the time gap
                self.largest_time_gap = Some(TimeGap::new(previous_datetime, current_timestamp));
            }
        } else {
            // This is the first row, inialize both min and max to this value
            self.min_timestamp = Some(current_timestamp);
            self.max_timestamp = Some(current_timestamp);
        }
        self.previous_timestamp = Some(current_timestamp);
        Ok(())
    }

    pub fn get_mean_and_variance(&self) -> (f64, f64) {
        let mean = match self.welford_calculator.mean() {
            Some(real_mean) => real_mean as f64,
            None => 0.0,
        };
        let variance = match self.welford_calculator.var() {
            Some(variance) => variance as f64,
            None => 0.0,
        };
        (mean, variance)
    }
    pub fn get_possible_alert_values(&self) -> PossibleAlertValues {
        let (mean, variance) = self.get_mean_and_variance();
        // println!("mean: {:?}, standard deviation: {:?}", mean, standard_deviation);
        PossibleAlertValues {
            num_records: self.timestamp_num_records,
            num_dupes: self.num_dupes,
            num_redactions: self.num_redactions,
            largest_time_gap: self.largest_time_gap,
            errors: self.errors.clone(),
            mean: mean,
            std: variance.sqrt(),
        }
    }
    pub fn add_error(&mut self, error_to_add: LavaError) {
        self.errors.push(error_to_add);
    }
}

#[derive(Debug)]
pub struct PossibleAlertValues {
    pub num_records: usize,
    pub num_dupes: usize,
    pub num_redactions: usize,
    pub largest_time_gap: Option<TimeGap>,
    pub errors: Vec<LavaError>,
    pub mean: f64,
    pub std: f64,
}
