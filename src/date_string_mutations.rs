
pub fn strip_to_10_most_significant_digits(input_string: &str) -> &str {
    if input_string.len() > 10 {
        &input_string[..10]
    } else {
        input_string
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_more_than_10() {
        let input = "1234567890123";
        let output = strip_to_10_most_significant_digits(input);
        assert_eq!(output, "1234567890")
    }

    #[test]
    fn test_strip_exactly_10() {
        let input = "1234567890";
        let output = strip_to_10_most_significant_digits(input);
        assert_eq!(output, "1234567890")
    }

    #[test]
    fn test_strip_less_than_10() {
        let input = "12345";
        let output = strip_to_10_most_significant_digits(input);
        assert_eq!(output, "12345")
    }
}