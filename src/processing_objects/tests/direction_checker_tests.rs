use super::super::*;
use crate::test_helpers::*;
#[test]
fn returns_none_on_first_timestamp() {
    let mut checker = TimeDirectionChecker::default();
    let result = checker.process_timestamp(dt("2024-05-01 12:00:00"));
    assert_eq!(result, None);
    assert_eq!(checker.previous, Some(dt("2024-05-01 12:00:00")));
}

#[test]
fn detects_ascending_order() {
    let mut checker = TimeDirectionChecker {
        previous: Some(dt("2024-05-01 12:00:00")),
    };
    let result = checker.process_timestamp(dt("2024-05-01 13:00:00"));
    assert_eq!(result, Some(TimeDirection::Ascending));
}

#[test]
fn detects_descending_order() {
    let mut checker = TimeDirectionChecker {
        previous: Some(dt("2024-05-01 13:00:00")),
    };
    let result = checker.process_timestamp(dt("2024-05-01 12:00:00"));
    assert_eq!(result, Some(TimeDirection::Descending));
}

#[test]
fn returns_none_when_timestamps_are_equal() {
    let mut checker = TimeDirectionChecker {
        previous: Some(dt("2024-05-01 12:00:00")),
    };
    let result = checker.process_timestamp(dt("2024-05-01 12:00:00"));
    assert_eq!(result, None);
}