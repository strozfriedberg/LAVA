pub fn strip_to_10_most_significant_digits(input_string: &str) -> String {
    if input_string.len() > 10 {
        input_string[..10].to_string()
    } else {
        input_string.to_string()
    }
}

pub fn append_1970_to_the_left(input: &str) -> String {
    format!("1970 {}", input)
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

    #[test]
    fn test_append_1970_to_the_left() {
        let input = "Jan 01 00:01:00";
        let output = append_1970_to_the_left(input);
        assert_eq!(output, "1970 Jan 01 00:01:00")
    }
}
