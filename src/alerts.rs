use crate::processing_objects::PossibleAlertValues;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum AlertLevel {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum AlertType {
    SusTimeGap,
    SusEventCount,
    DupeEvents,
    RedactionEvents,
    JsonError,
    MultipartOverlap,
}

fn get_alert_threshold_value(alert_level: AlertLevel, alert_type: AlertType) -> usize {
    match alert_type {
        AlertType::SusTimeGap => match alert_level {
            AlertLevel::High => 100,
            AlertLevel::Medium => 30,
            AlertLevel::Low => 10,
        },
        AlertType::SusEventCount => match alert_level {
            AlertLevel::High => 10000,
            AlertLevel::Medium => 1000,
            AlertLevel::Low => 100,
        },
        AlertType::DupeEvents => match alert_level {
            AlertLevel::High => 100,
            AlertLevel::Medium => 10,
            AlertLevel::Low => 0,
        },
        AlertType::RedactionEvents => match alert_level {
            AlertLevel::High => 100,
            AlertLevel::Medium => 10,
            AlertLevel::Low => 0,
        },
        AlertType::JsonError => match alert_level {
            AlertLevel::High => 0,
            AlertLevel::Medium => 0,
            AlertLevel::Low => 0,
        },
        AlertType::MultipartOverlap => match alert_level {
            AlertLevel::High => 0,
            AlertLevel::Medium => 0,
            AlertLevel::Low => 0,
        },
    }
}

pub fn get_message_for_alert_comfy_table(
    alert_level: AlertLevel,
    alert_type: AlertType,
    number_of_files: usize,
) -> String {
    match alert_type {
        AlertType::SusTimeGap => format!(
            "{} files had a largest time gap greater than {} standard deviations above the average time gap",
            number_of_files,
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::SusEventCount => format!(
            "{} files had an event count divisible by {}",
            number_of_files,
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::DupeEvents => format!(
            "{} files had greater than {} duplicate records",
            number_of_files,
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::RedactionEvents => format!(
            "{} files had greater than {} records with potential redactions",
            number_of_files,
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::JsonError => format!("{} files had JSON syntax errors", number_of_files),
        AlertType::MultipartOverlap => {
            format!("{} files contain overlapping time ranges", number_of_files)
        }
    }
}

pub fn get_message_for_alert_output_file(alert_level: AlertLevel, alert_type: AlertType) -> String {
    match alert_type {
        AlertType::SusTimeGap => format!(
            "Largest time gap greater than {} standard deviations above the average time gap",
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::SusEventCount => format!(
            "Event count was divisible by {}",
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::DupeEvents => format!(
            "Greater than {} duplicate records",
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::RedactionEvents => format!(
            "Greater than {} records with potential redactions",
            get_alert_threshold_value(alert_level, alert_type)
        ),
        AlertType::JsonError => {
            format!("File had json syntax errors that may interfere with parsing in other tools")
        }
        AlertType::MultipartOverlap => format!("Time range of this file overlaps with another"),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Alert {
    pub alert_level: AlertLevel,
    pub alert_type: AlertType,
}

impl Alert {
    pub fn new(alert_level: AlertLevel, alert_type: AlertType) -> Self {
        Self {
            alert_level,
            alert_type,
        }
    }
}

pub fn generate_alerts(things_to_alert_on: PossibleAlertValues) -> Vec<Alert> {
    let mut alerts: Vec<Alert> = Vec::new();

    //Num records alerts
    if things_to_alert_on.num_records > 0 {
        if let Some(level) =
            get_alert_level_remainder_zero(things_to_alert_on.num_records, AlertType::SusEventCount)
        {
            alerts.push(Alert::new(level, AlertType::SusEventCount));
        };
    };

    //Num dupes alerts
    if let Some(level) = get_alert_level_greater_than_threshold_values(
        things_to_alert_on.num_dupes,
        AlertType::DupeEvents,
    ) {
        alerts.push(Alert::new(level, AlertType::DupeEvents));
    };

    //Num redactions alerts
    if let Some(level) = get_alert_level_greater_than_threshold_values(
        things_to_alert_on.num_redactions,
        AlertType::RedactionEvents,
    ) {
        alerts.push(Alert::new(level, AlertType::RedactionEvents));
    };

    //Time gap alerts
    if let Some(time_gap) = things_to_alert_on.largest_time_gap {
        let standard_deviations_above_the_mean = ((time_gap.get_time_duration_number() as f64
            - things_to_alert_on.mean)
            / things_to_alert_on.std)
            .floor() as usize;
        if let Some(level) = get_alert_level_greater_than_threshold_values(
            standard_deviations_above_the_mean,
            AlertType::SusTimeGap,
        ) {
            alerts.push(Alert::new(level, AlertType::SusTimeGap));
        };
    };

    // JSON errors
    if things_to_alert_on
        .errors
        .iter()
        .any(|e| e.reason.contains("Unable to parse JSON"))
    {
        alerts.push(Alert::new(AlertLevel::High, AlertType::JsonError));
    }

    alerts
}

fn get_alert_level_greater_than_threshold_values(
    value: usize,
    alert_type: AlertType,
) -> Option<AlertLevel> {
    if value > get_alert_threshold_value(AlertLevel::High, alert_type) {
        Some(AlertLevel::High)
    } else if value > get_alert_threshold_value(AlertLevel::Medium, alert_type) {
        Some(AlertLevel::Medium)
    } else if value > get_alert_threshold_value(AlertLevel::Low, alert_type) {
        Some(AlertLevel::Low)
    } else {
        None
    }
}

fn get_alert_level_remainder_zero(n: usize, alert_type: AlertType) -> Option<AlertLevel> {
    if n % get_alert_threshold_value(AlertLevel::High, alert_type) == 0 {
        Some(AlertLevel::High)
    } else if n % get_alert_threshold_value(AlertLevel::Medium, alert_type) == 0 {
        Some(AlertLevel::Medium)
    } else if n % get_alert_threshold_value(AlertLevel::Low, alert_type) == 0 {
        Some(AlertLevel::Low)
    } else {
        None
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::basic_objects::TimeGap;
    use crate::processing_objects::PossibleAlertValues;
    use chrono::NaiveDate;
    use chrono::TimeDelta;

    pub fn dummy_timegap(gap_secs: i64) -> TimeGap {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = start + TimeDelta::seconds(gap_secs);
        TimeGap {
            gap: TimeDelta::seconds(gap_secs),
            beginning_time: start,
            end_time: end,
        }
    }
    #[test]
    fn test_get_alert_level_greater_than_threshold_values() {
        assert_eq!(
            get_alert_level_greater_than_threshold_values(200, AlertType::SusTimeGap),
            Some(AlertLevel::High)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(60, AlertType::SusTimeGap),
            Some(AlertLevel::Medium)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(15, AlertType::SusTimeGap),
            Some(AlertLevel::Low)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(2, AlertType::SusTimeGap),
            None
        );
    }

    #[test]
    fn test_get_alert_level_remainder_zero() {
        assert_eq!(
            get_alert_level_remainder_zero(20000, AlertType::SusEventCount),
            Some(AlertLevel::High)
        );
        assert_eq!(
            get_alert_level_remainder_zero(2000, AlertType::SusEventCount),
            Some(AlertLevel::Medium)
        );
        assert_eq!(
            get_alert_level_remainder_zero(300, AlertType::SusEventCount),
            Some(AlertLevel::Low)
        );
        assert_eq!(
            get_alert_level_remainder_zero(70, AlertType::SusEventCount),
            None
        );
    }

    #[test]
    fn test_get_alert_level_of_num_dupes() {
        assert_eq!(
            get_alert_level_greater_than_threshold_values(101, AlertType::DupeEvents),
            Some(AlertLevel::High)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(50, AlertType::DupeEvents),
            Some(AlertLevel::Medium)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(1, AlertType::DupeEvents),
            Some(AlertLevel::Low)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(0, AlertType::DupeEvents),
            None
        );
    }

    #[test]
    fn test_get_alert_level_of_num_redactions() {
        assert_eq!(
            get_alert_level_greater_than_threshold_values(150, AlertType::RedactionEvents),
            Some(AlertLevel::High)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(20, AlertType::RedactionEvents),
            Some(AlertLevel::Medium)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(1, AlertType::RedactionEvents),
            Some(AlertLevel::Low)
        );
        assert_eq!(
            get_alert_level_greater_than_threshold_values(0, AlertType::RedactionEvents),
            None
        );
    }

    #[test]
    fn test_generate_alerts_all_levels() {
        let input = PossibleAlertValues {
            num_records: 1000,
            num_dupes: 20,
            num_redactions: 2,
            largest_time_gap: Some(dummy_timegap(60)),
            errors: Vec::new(),
            mean: 10.0,
            std: 4.0,
        };

        let alerts = generate_alerts(input);

        assert_eq!(alerts.len(), 4);
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a.alert_level, AlertLevel::High))
        );
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a.alert_level, AlertLevel::Medium))
        );
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a.alert_level, AlertLevel::Low))
        );
    }

    #[test]
    fn test_generate_alerts_none_triggered() {
        let input = PossibleAlertValues {
            num_records: 0,
            num_dupes: 0,
            num_redactions: 0,
            largest_time_gap: Some(dummy_timegap(60)),
            errors: Vec::new(),
            mean: 50000.0,
            std: 10000.0,
        };

        let alerts = generate_alerts(input);

        assert_eq!(alerts.len(), 0);
    }
}
