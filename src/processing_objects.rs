use crate::basic_objects::*;
use crate::errors::*;
use crate::helpers::*;
use chrono::NaiveDateTime;
use csv::StringRecord;
use csv::WriterBuilder;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::path::PathBuf;
include!(concat!(env!("OUT_DIR"), "/generated_redaction_regexes.rs"));

#[cfg(test)]
mod tests {
    mod build_file_path_tests;
    mod direction_checker_tests;
    mod dupe_processing_tests;
    mod timestamp_processing_tests;
    mod redaction_processing_tests;
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

#[derive(Debug, Default)]
pub struct LogRecordProcessor {
    pub order: Option<TimeDirection>,
    pub execution_settings: ExecutionSettings,
    pub file_name: String,
    pub data_field_headers: StringRecord,
    pub num_records: usize,
    pub min_timestamp: Option<NaiveDateTime>,
    pub max_timestamp: Option<NaiveDateTime>,
    pub previous_timestamp: Option<NaiveDateTime>,
    pub largest_time_gap: Option<TimeGap>,
    pub duplicate_checker_set: HashSet<u64>,
    pub num_dupes: usize,
    pub num_redactions: usize,
    pub errors: Vec<LavaError>,
    process_timestamps: bool,
}

impl LogRecordProcessor {
    pub fn new_with_order(
        order: Option<TimeDirection>,
        execution_settings: &ExecutionSettings,
        log_file_stem: String,
        headers: Option<StringRecord>,
    ) -> Self {
        let data_field_headers = match headers {
            Some(csv_headers) => csv_headers,
            None => StringRecord::from(vec!["Record"]),
        };
        Self {
            order,
            execution_settings: execution_settings.clone(),
            file_name: log_file_stem,
            data_field_headers: data_field_headers,
            process_timestamps: true,
            ..Default::default()
        }
    }
    pub fn process_record(&mut self, record: LogFileRecord) -> Result<()> {
        //Check for duplicates
        if !self.execution_settings.quick_mode {
            self.process_record_for_dupes(&record)?;
            self.process_record_for_redactions(&record)?;
        }
        //Update earliest and latest timestamp
        if self.process_timestamps {
            self.process_timestamp(&record)?;
        }

        Ok(())
    }

    pub fn process_record_for_dupes(
        &mut self,
        record: &LogFileRecord,
    ) -> Result<()> {
        let is_duplicate = !self
            .duplicate_checker_set
            .insert(record.hash_of_entire_record);
        if is_duplicate {
            println!("Found duplicate record at index {}", record.index);
            self.num_dupes += 1;
            if self.execution_settings.actually_write_to_files {
                let _ = self.write_hit_to_file(record, AlertOutputType::Duplicate)?;
            }
        }
        Ok(())
    }
    pub fn process_record_for_redactions(
        &mut self,
        record: &LogFileRecord,
    ) -> Result<()> {
        for redaction in PREBUILT_REDACTION_REGEXES.iter(){
            if redaction.string_record_contains_match(&record.raw_record) {
                // println!("Found duplicate record at index {}", record.index);
                self.num_redactions += 1;
                println!("Found redaction in record {:?}", record.raw_record);
                if self.execution_settings.actually_write_to_files {
                    let _ = self.write_hit_to_file(record, AlertOutputType::Redaction)?;
                }
            }
        }

        Ok(())
    }
    pub fn write_hit_to_file(&mut self, record: &LogFileRecord, alert_type: AlertOutputType) -> Result<()> {
        let output_file = self.build_file_path(&alert_type)?;
        let file_existed_before = output_file.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_file)
            .map_err(|e| {
                LavaError::new(format!("Unable to create output file because of {e}"), LavaErrorLevel::Critical)
            })?;

        let mut writer = WriterBuilder::new()
            .has_headers(false) // Disable automatic header writing
            .from_writer(file);

        if !file_existed_before {
            writer
                .write_record(
                    &self.get_full_output_headers_based_on_alert_type(&alert_type),
                )
                .map_err(|e| {
                    LavaError::new(format!("Unable to write headers to file because of {e}"), LavaErrorLevel::Critical)
                })?;
        }

        writer
            .write_record(&record.get_record_to_output(&alert_type))
            .map_err(|e| LavaError::new(format!("Unable to write record because of {e}"), LavaErrorLevel::Critical))?;
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
                StringRecord::from(vec!["Index of Hit"]) // Maybe in the future add the name of the rule that hit in the second column
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

    pub fn process_timestamp(&mut self, record: &LogFileRecord) -> Result<()> {
        if let Some(previous_datetime) = self.previous_timestamp {
            // This is where all logic is done if it isn't the first record
            if self.order == Some(TimeDirection::Ascending) {
                if previous_datetime > record.timestamp {
                    self.process_timestamps = false;
                    self.errors.push(LavaError::new(format!(
                        "File was not sorted on the identified timestamp. Out of order record at index {}",
                        record.index
                    ), LavaErrorLevel::Medium));
                }
                self.max_timestamp = Some(record.timestamp)
            } else if self.order == Some(TimeDirection::Descending) {
                if previous_datetime < record.timestamp {
                    self.process_timestamps = false;
                    self.errors.push(LavaError::new(format!(
                        "File was not sorted on the identified timestamp. Out of order record at index {}",
                        record.index
                    ), LavaErrorLevel::Medium));
                }
                self.min_timestamp = Some(record.timestamp)
            }
            let current_time_gap = TimeGap::new(previous_datetime, record.timestamp);
            if let Some(largest_time_gap) = self.largest_time_gap {
                if current_time_gap > largest_time_gap {
                    self.largest_time_gap = Some(TimeGap::new(previous_datetime, record.timestamp));
                }
            } else {
                // This is the second row, intialize the time gap
                self.largest_time_gap = Some(TimeGap::new(previous_datetime, record.timestamp));
            }
        } else {
            // This is the first row, inialize either the min or max timestamp
            if self.order == Some(TimeDirection::Ascending) {
                self.min_timestamp = Some(record.timestamp)
            } else if self.order == Some(TimeDirection::Descending) {
                self.max_timestamp = Some(record.timestamp)
            }
        }
        self.num_records = self.num_records + 1;
        self.previous_timestamp = Some(record.timestamp);
        Ok(())
    }

    pub fn get_statistics(&self) -> Result<TimeStatisticsFields> {
        let mut statistics_fields = TimeStatisticsFields::default();

        statistics_fields.num_records = Some(self.num_records.to_string());
        statistics_fields.min_timestamp = Some(
            self.min_timestamp
                .ok_or_else(|| LavaError::new("No min timestamp found", LavaErrorLevel::Critical))?
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
        );
        statistics_fields.max_timestamp = Some(
            self.max_timestamp
                .ok_or_else(|| LavaError::new("No max timestamp found", LavaErrorLevel::Critical))?
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
        );
        let min_max_gap = self
            .max_timestamp
            .ok_or_else(|| LavaError::new("No max timestamp found", LavaErrorLevel::Critical))?
            .signed_duration_since(
                self.min_timestamp
                    .ok_or_else(|| LavaError::new("No min timestamp found", LavaErrorLevel::Critical))?,
            );
        statistics_fields.min_max_duration = Some(format_timedelta(min_max_gap));

        let largest_time_gap = self
            .largest_time_gap
            .ok_or_else(|| LavaError::new("No largest time gap found", LavaErrorLevel::Critical))?;

        statistics_fields.largest_gap = Some(format!(
            "{} to {}",
            largest_time_gap.beginning_time.format("%Y-%m-%d %H:%M:%S"),
            largest_time_gap.end_time.format("%Y-%m-%d %H:%M:%S")
        ));
        statistics_fields.largest_gap_duration = Some(format_timedelta(largest_time_gap.gap));
        statistics_fields.num_dupes = Some(self.num_dupes.to_string());
        statistics_fields.num_redactions = Some(self.num_redactions.to_string());
        statistics_fields.errors = self.errors.clone();
        Ok(statistics_fields)
    }
}
