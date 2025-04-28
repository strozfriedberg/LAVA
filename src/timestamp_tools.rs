use crate::basic_objects::*;
use crate::errors::*;
use chrono::NaiveDateTime;
use std::collections::HashSet;

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

#[derive(PartialEq, Debug, Default)]
pub struct LogFileStatisticsAndAlerts {
    pub order: Option<TimeDirection>,
    pub min_timestamp: Option<NaiveDateTime>,
    pub max_timestamp: Option<NaiveDateTime>,
    pub previous_timestamp: Option<NaiveDateTime>,
    pub largest_time_gap: Option<TimeGap>, // Eventually maybe make this store the top few?
    pub duplicate_checker_set: HashSet<u64>,
}

impl LogFileStatisticsAndAlerts {
    pub fn new_with_order(order: Option<TimeDirection>) -> Self {
        Self {
            order,
            ..Default::default()
        }
    }
    pub fn process_record(&mut self, record: LogFileRecord) -> Result<()> {
        //Check for duplicates
        let is_duplicate = !self
            .duplicate_checker_set
            .insert(record.hash_of_entire_record);
        if is_duplicate {
            println!("Found duplicate record at index {}", record.index);
        }

        //Update earliest and latest timestamp
        if let Some(previous_datetime) = self.previous_timestamp {
            // This is where all logic is done if it isn't the first record
            if self.order == Some(TimeDirection::Ascending) {
                if previous_datetime > record.timestamp {
                    return Err(LogCheckError::new(format!(
                        "File was not sorted on the identified timestamp. Out of order record at index {}",
                        record.index
                    )));
                }
                self.max_timestamp = Some(record.timestamp)
            } else if self.order == Some(TimeDirection::Descending) {
                if previous_datetime < record.timestamp {
                    return Err(LogCheckError::new(format!(
                        "File was not sorted on the identified timestamp. Out of order record at index {}",
                        record.index
                    )));
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
        self.previous_timestamp = Some(record.timestamp);

        Ok(())
    }
}
