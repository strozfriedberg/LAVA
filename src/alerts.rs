use chrono::TimeDelta;
use crate::processing_objects::PossibleAlertValues;

#[derive(Debug, PartialEq)]
pub enum AlertLevel {
    High,
    Medium,
    Low,
}

#[derive(Debug)]
pub enum AlertType {
    SusTimeGap(TimeDelta),
    SusEventCount(usize),
    DupeEvents(usize),
    RedactionEvents(usize),
    JsonError,
}


#[derive(Debug)]
pub struct Alert {
    alert_level: AlertLevel,
    alert_type: AlertType,
}

impl Alert {
    pub fn new(alert_level: AlertLevel, alert_type: AlertType) -> Self {
        Self {
            alert_level,
            alert_type,
        }
    }
}


pub fn generate_alerts(things_to_alert_on: PossibleAlertValues ) -> Vec<Alert> {
    let mut alerts: Vec<Alert> = Vec::new();

    //Num records alerts 
    if let Some(level) = get_alert_level_of_num_events(things_to_alert_on.num_records) {
        alerts.push(Alert::new(level, AlertType::SusEventCount(things_to_alert_on.num_records)));
    };

    //Num dupes alerts 
    if let Some(level) = get_alert_level_of_num_dupes(things_to_alert_on.num_dupes) {
        alerts.push(Alert::new(level, AlertType::DupeEvents(things_to_alert_on.num_dupes)));
    };

    //Num redactions alerts 
    if let Some(level) = get_alert_level_of_num_redactions(things_to_alert_on.num_redactions) {
        alerts.push(Alert::new(level, AlertType::RedactionEvents(things_to_alert_on.num_redactions)));
    };

    //Time gap alerts
    if let Some(time_gap) = things_to_alert_on.largest_time_gap {
        if let Some(level) = get_alert_level_of_time_gap(time_gap.gap) {
            alerts.push(Alert::new(level, AlertType::SusTimeGap(time_gap.gap)));
        };
    };
    
    alerts

}


fn get_alert_level_of_time_gap(time_gap: TimeDelta )-> Option<AlertLevel> {
    if time_gap >= TimeDelta::hours(24) {
        Some(AlertLevel::High)
    } else if time_gap >= TimeDelta::hours(1) {
        Some(AlertLevel::Medium)
    } else if time_gap >= TimeDelta::minutes(5) {
        Some(AlertLevel::Low)
    } else {
        None
    }
}


fn get_alert_level_of_num_redactions(num_redactions: usize)-> Option<AlertLevel> {
    if num_redactions > 100 {
        Some(AlertLevel::High)
    } else if num_redactions > 10  {
        Some(AlertLevel::Medium)
    } else if num_redactions > 0  {
        Some(AlertLevel::Low)
    } else {
        None
    }
}

fn get_alert_level_of_num_dupes(num_dupes: usize)-> Option<AlertLevel> {
    if num_dupes > 100 {
        Some(AlertLevel::High)
    } else if num_dupes > 10  {
        Some(AlertLevel::Medium)
    } else if num_dupes > 0  {
        Some(AlertLevel::Low)
    } else {
        None
    }
}

fn get_alert_level_of_num_events(n: usize) -> Option<AlertLevel> {
    if n % 1000 == 0 {
        Some(AlertLevel::High)
    } else if n % 100 == 0 {
        Some(AlertLevel::Medium)
    } else if n % 10 == 0 {
        Some(AlertLevel::Low)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic_objects::TimeGap;
    use crate::processing_objects::PossibleAlertValues;
    use chrono::NaiveDate;

    fn dummy_timegap(gap_secs: i64) -> TimeGap {
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
    fn test_get_alert_level_of_time_gap() {
        assert_eq!(get_alert_level_of_time_gap(TimeDelta::hours(25)), Some(AlertLevel::High));
        assert_eq!(get_alert_level_of_time_gap(TimeDelta::hours(2)), Some(AlertLevel::Medium));
        assert_eq!(get_alert_level_of_time_gap(TimeDelta::minutes(6)), Some(AlertLevel::Low));
        assert_eq!(get_alert_level_of_time_gap(TimeDelta::minutes(3)), None);
    }

    #[test]
    fn test_get_alert_level_of_num_events() {
        assert_eq!(get_alert_level_of_num_events(1000), Some(AlertLevel::High));
        assert_eq!(get_alert_level_of_num_events(200), Some(AlertLevel::Medium));
        assert_eq!(get_alert_level_of_num_events(30), Some(AlertLevel::Low));
        assert_eq!(get_alert_level_of_num_events(7), None);
    }

    #[test]
    fn test_get_alert_level_of_num_dupes() {
        assert_eq!(get_alert_level_of_num_dupes(101), Some(AlertLevel::High));
        assert_eq!(get_alert_level_of_num_dupes(50), Some(AlertLevel::Medium));
        assert_eq!(get_alert_level_of_num_dupes(1), Some(AlertLevel::Low));
        assert_eq!(get_alert_level_of_num_dupes(0), None);
    }

    #[test]
    fn test_get_alert_level_of_num_redactions() {
        assert_eq!(get_alert_level_of_num_redactions(150), Some(AlertLevel::High));
        assert_eq!(get_alert_level_of_num_redactions(20), Some(AlertLevel::Medium));
        assert_eq!(get_alert_level_of_num_redactions(1), Some(AlertLevel::Low));
        assert_eq!(get_alert_level_of_num_redactions(0), None);
    }

    #[test]
    fn test_generate_alerts_all_levels() {
        let input = PossibleAlertValues {
            num_records: 1000,
            num_dupes: 20,
            num_redactions: 2,
            largest_time_gap: Some(dummy_timegap(60)),
            errors: Vec::new(),
        };

        let alerts = generate_alerts(input);

        assert_eq!(alerts.len(), 3);
        assert!(alerts.iter().any(|a| matches!(a.alert_level, AlertLevel::High)));
        assert!(alerts.iter().any(|a| matches!(a.alert_level, AlertLevel::Medium)));
        assert!(alerts.iter().any(|a| matches!(a.alert_level, AlertLevel::Low)));
    }

    #[test]
    fn test_generate_alerts_none_triggered() {
        let input = PossibleAlertValues {
            num_records: 3,
            num_dupes: 0,
            num_redactions: 0,
            largest_time_gap: Some(dummy_timegap(60)),
            errors: Vec::new(),
        };

        let alerts = generate_alerts(input);

        assert_eq!(alerts.len(), 0);
    }
}
