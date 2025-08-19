use crate::alerts::*;
use crate::basic_objects::*;
use crate::errors::*;
use chrono::NaiveDateTime;
use chrono::Utc;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::*;
use csv::StringRecord;
use csv::Writer;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub fn print_if_verbose_mode_on<T: Display>(thing_to_print: T) {
    if let Some(verbose_mode) = crate::VERBOSE.get() {
        if *verbose_mode {
            println!("{}", thing_to_print)
        }
    }
}

pub fn generate_log_filename() -> String {
    let now = Utc::now();
    let formatted = now.format("%Y-%m-%d_%H-%M-%S_LAVA_Output.csv");
    formatted.to_string()
}

pub fn hash_csv_record(record: &StringRecord) -> u64 {
    let mut hasher = DefaultHasher::new();
    record.iter().for_each(|field| field.hash(&mut hasher));
    hasher.finish()
}

pub fn get_file_stem(log_file: &LogFile) -> Result<String> {
    let file_name = &log_file
        .file_path
        .file_stem()
        .ok_or_else(|| LavaError::new("Could not get file stem.", LavaErrorLevel::Critical))?;
    Ok(file_name.to_string_lossy().to_string())
}

pub fn write_output_to_csv(
    processed_log_files: &Vec<ProcessedLogFile>,
    execution_settings: &ExecutionSettings,
) -> Result<()> {
    // in the final version, maybe have a full version that has tons of fields, and then a simplified version. Could have command line arg to trigger verbose one
    //Add something here to create the
    let output_filepath = execution_settings.output_dir.join(generate_log_filename());
    let mut wtr = Writer::from_path(&output_filepath).map_err(|e| {
        LavaError::new(
            format!("Unable to open ouptut file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    wtr.write_record(&[
        "Filename",
        "File Path",
        "SHA256 Hash",
        "Size",
        "First Data Row Used",
        "Header Used",
        "Timestamp Format",
        "Total Number of Records",
        "Number of Records Processed for Timestamp Analysis",
        "Min Timestamp",
        "Max Timestamp",
        "Duration of Entire Log File (Hours)",
        "Pretty Duration of Entire Log File",
        "Largest Time Gap (LTG)",
        "Duration of LTG (Hours)",
        "Pretty Duration of LTG",
        &format!("Mean {} of Time Gaps", WELFORD_TIME_SIGNIFIGANCE),
        &format!(
            "Standard Deviation of Time Gaps in {}",
            WELFORD_TIME_SIGNIFIGANCE
        ),
        "LTG Number of Standard Deviations Above the Mean",
        "Duplicate Record Count",
        "Possible Redactions Count",
        "Error",
    ])
    .map_err(|e| {
        LavaError::new(
            format!("Unable to write headers because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    for log_file in processed_log_files {
        wtr.serialize(log_file.get_strings_for_file_statistics_output_row())
            .map_err(|e| {
                LavaError::new(
                    format!("Issue writing lines of output file because of {e}"),
                    LavaErrorLevel::Critical,
                )
            })?;
    }
    wtr.flush().map_err(|e| {
        LavaError::new(
            format!("Issue flushing to the ouptut file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?; //Is this really needed?
    // println!("Data written to {}", output_filepath.to_string_lossy());
    Ok(())
}

pub fn write_errors_to_error_log(
    results: &Vec<ProcessedLogFile>,
    settings: &ExecutionSettings,
) -> Result<()> {
    let error_log_path = settings.output_dir.join("LAVA_Errors.log");

    // Open the file in append mode, create it if it doesn't exist
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&error_log_path)
        .map_err(|e| {
            LavaError::new(
                format!("Unable to open error log because of {}", e),
                LavaErrorLevel::Critical,
            )
        })?;

    let mut writer = BufWriter::new(file);

    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");

    for processed_file in results {
        if let Some(filename) = &processed_file.filename {
            for error in &processed_file.errors {
                writeln!(
                    writer,
                    "[{}] [{}] [{}]{}",
                    timestamp, error.level, filename, error.reason
                )
                .map_err(|e| {
                    LavaError::new(
                        format!("Unable to write to error log because of {}", e),
                        LavaErrorLevel::Critical,
                    )
                })?;
            }
        }
    }

    writer.flush().map_err(|e| {
        LavaError::new(
            format!("Unable to write to error log because of {}", e),
            LavaErrorLevel::Critical,
        )
    })?; // Ensure all writes are flushed

    Ok(())
}

pub fn print_pretty_alerts_and_write_to_alerts_output_file(
    results: &Vec<ProcessedLogFile>,
    execution_settings: &ExecutionSettings,
) -> Result<()> {
    let mut writer = match execution_settings.actually_write_to_files {
        false => None,
        true => {
            let output_file_path: PathBuf = execution_settings.output_dir.join("alerts_output.txt");
            let alert_output_file = OpenOptions::new()
                .create(true)
                .append(true)
                .write(true)
                .open(output_file_path)
                .expect("Failed to open alerts output file");
            Some(BufWriter::new(alert_output_file))
        }
    };

    let mut alert_table_structure: HashMap<AlertLevel, HashMap<AlertType, Vec<&String>>> =
        HashMap::new();
    for processed in results.iter() {
        for alert in processed.alerts.iter() {
            if let Some(writer) = writer.as_mut() {
                writeln!(
                    writer,
                    "File Path:{} | Level: {:?} | Type {:?} | Message: {}",
                    processed.file_path.as_ref().unwrap(),
                    alert.alert_level,
                    alert.alert_type,
                    get_message_for_alert_output_file(alert.alert_level, alert.alert_type)
                )
                .expect("Failed to write to alert output file");
            }
            alert_table_structure
                .entry(alert.alert_level)
                .or_insert_with(HashMap::new)
                .entry(alert.alert_type)
                .or_insert_with(Vec::new)
                .push(processed.file_path.as_ref().unwrap());
        }
    }
    let levels = [AlertLevel::High, AlertLevel::Medium, AlertLevel::Low];

    let mut output_table = Table::new();
    output_table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_style(TableComponent::VerticalLines, ' ');
    let hlch = output_table.style(TableComponent::HorizontalLines).unwrap();
    let tbch = output_table.style(TableComponent::TopBorder).unwrap();

    for level in levels.iter() {
        if let Some(alerts_of_this_level) = alert_table_structure.get(level) {
            if alerts_of_this_level.keys().len() > 0 {
                output_table
                    .add_row(vec![
                        Cell::new(alert_level_to_string(level)).fg(alert_level_color(level)),
                    ])
                    .set_style(TableComponent::MiddleIntersections, hlch)
                    .set_style(TableComponent::TopBorderIntersections, tbch)
                    .set_style(TableComponent::BottomBorderIntersections, hlch);
                let mut alerts_cell_string = String::new();
                for alert in alerts_of_this_level.keys() {
                    let num_files_in_this_category = alerts_of_this_level.get(alert).unwrap().len();
                    alerts_cell_string.push_str(&format!(
                        "{}\n",
                        get_message_for_alert_comfy_table(
                            level.clone(),
                            alert.clone(),
                            num_files_in_this_category
                        )
                    ));
                }
                output_table.add_row(vec![
                    Cell::new(alerts_cell_string.trim_end()).fg(alert_level_color(level)),
                ]);
            }
        }
    }
    if output_table.is_empty() {
        println!(
            "No alerts were generated when processing {} files",
            results.len()
        );
    } else {
        println!("{output_table}");
    }

    Ok(())
}

pub fn print_pretty_quick_stats(results: &Vec<ProcessedLogFile>) -> Result<()> {
    let mut successful_time_processed_data: Vec<QuickStats> = results
        .iter()
        .filter_map(|item| {
            // Only continue if *all* required fields are Some
            item.get_quick_stats()
        })
        .collect();

    successful_time_processed_data
        .sort_by(|a, b| b.largest_gap_duration.cmp(&a.largest_gap_duration));

    let first_five_slice =
        successful_time_processed_data[..successful_time_processed_data.len().min(5)].to_vec();

    if first_five_slice.len() > 0 {
        let mut output_table = Table::new();
        output_table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS);

        output_table.set_header(vec![
            Cell::new("Filename"),
            Cell::new("Min Timestamp"),
            Cell::new("Max Timestamp"),
            Cell::new("Record Count"),
            Cell::new("Largest Gap Duration"),
        ]);
        for result in first_five_slice.iter() {
            output_table.add_row(vec![
                Cell::new(&result.filename),
                Cell::new(&result.min_timestamp),
                Cell::new(&result.max_timestamp),
                Cell::new(&result.num_records),
                Cell::new(&result.largest_gap_duration_human),
            ]);
        }
        println!(
            "File(s) with the largest {} time gaps",
            first_five_slice.len()
        );
        println!("{output_table}");
    } else {
        println!("Time analysis failed to complete for all input files")
    }

    Ok(())
}

fn alert_level_to_string(alert_level: &AlertLevel) -> &str {
    match alert_level {
        AlertLevel::High => "HIGH ALERTS",
        AlertLevel::Medium => "MEDIUM ALERTS",
        AlertLevel::Low => "LOW ALERTS",
    }
}

fn alert_level_color(alert_level: &AlertLevel) -> comfy_table::Color {
    match alert_level {
        AlertLevel::High => comfy_table::Color::Red,
        AlertLevel::Medium => comfy_table::Color::Yellow,
        AlertLevel::Low => comfy_table::Color::Green,
    }
}

pub fn convert_vector_of_processed_log_files_into_one_for_multipart(
    all_processed_logs: &Vec<ProcessedLogFile>,
) -> ProcessedLogFile {
    let mut combined_processed_log_file = ProcessedLogFile::default();
    let mut list_of_clean_data_for_individual_processed_log_files: Vec<
        ProcessedLogFileComboEssentials,
    > = vec![];
    for processed_log_file in all_processed_logs {
        // Right now the errors and alerts themselves don't have the filename associated with it, so will have to change that maybe?
        //here tag them with their original filepath
        combined_processed_log_file
            .alerts
            .extend(processed_log_file.alerts.clone());
        combined_processed_log_file
            .errors
            .extend(processed_log_file.errors.clone());
        //append number of records
        combined_processed_log_file.total_num_records += processed_log_file.total_num_records;
        combined_processed_log_file.timestamp_num_records += processed_log_file.timestamp_num_records;

        //update dupes
        if let Some(current_num_dupes) = processed_log_file.num_dupes {
            *combined_processed_log_file.num_dupes.get_or_insert(0) += current_num_dupes;
        }
        if let Some(current_num_redactions) = processed_log_file.num_redactions {
            *combined_processed_log_file.num_redactions.get_or_insert(0) += current_num_redactions;
        }
        
        if let Some(log_combo_essentials) =
            processed_log_file.get_processed_log_file_combination_essentials()
        {
            list_of_clean_data_for_individual_processed_log_files.push(log_combo_essentials);
        }
    }
    list_of_clean_data_for_individual_processed_log_files
        .sort_by(|a, b| a.min_timestamp.cmp(&b.min_timestamp));
    let combined_name = format!(
        "{}_SUCCESSFUL_INPUT_FILES_COMBINED",
        list_of_clean_data_for_individual_processed_log_files.len()
    );

    combined_processed_log_file.filename = Some(combined_name.clone());
    combined_processed_log_file.file_path = Some(combined_name);

    //Combine stats and alert if overlapped
    let mut combined_processed_files_essentials: Option<ProcessedLogFileComboEssentials> = None;

    for clean_processed_log_file in list_of_clean_data_for_individual_processed_log_files {
        if let Some(previous_stats_essentials) = combined_processed_files_essentials.as_mut() {
            //combine the mean count and var
            println!("Next one: {:?}", clean_processed_log_file);
            if let Some((count, mean, var)) = get_combined_count_mean_and_var_of_two_sets(
                previous_stats_essentials.num_time_gaps,
                previous_stats_essentials.time_gap_mean,
                previous_stats_essentials.time_gap_var,
                clean_processed_log_file.num_time_gaps,
                clean_processed_log_file.time_gap_mean,
                clean_processed_log_file.time_gap_var,
            ) {
                previous_stats_essentials.num_time_gaps = count;
                previous_stats_essentials.time_gap_mean = mean;
                previous_stats_essentials.time_gap_var = var;
            }

            // if the largest gap of the next one is larger than update it
            if let Some(current_time_gap) = clean_processed_log_file.largest_gap {
                if let Some(prev_time_gap) = previous_stats_essentials.largest_gap {
                    // both time gaps
                    if current_time_gap > prev_time_gap {
                        previous_stats_essentials.largest_gap = Some(current_time_gap)
                    }
                } else {
                    // there is a current time gap but no previous
                    previous_stats_essentials.largest_gap = Some(current_time_gap);
                }
            }

            //if the two file overlap then add an alert and DON"T add in the time gap
            if &previous_stats_essentials.max_timestamp > &clean_processed_log_file.min_timestamp {
                combined_processed_log_file
                    .alerts
                    .push(Alert::new(AlertLevel::High, AlertType::MultipartOverlap))
            } else {
                //If the two files do not overlap, then update the count mean var with the gap between files. AND if this gap is larger than the current one, update it
                let gap_between_files = TimeGap::new(
                    previous_stats_essentials.max_timestamp,
                    clean_processed_log_file.min_timestamp,
                );
                let (count, mean, var) = get_updated_count_mean_var_when_add_value_to_set(
                    previous_stats_essentials.num_time_gaps,
                    previous_stats_essentials.time_gap_mean,
                    previous_stats_essentials.time_gap_var,
                    gap_between_files.get_time_duration_number() as f64,
                );
                previous_stats_essentials.num_time_gaps = count;
                previous_stats_essentials.time_gap_mean = mean;
                previous_stats_essentials.time_gap_var = var;

                match previous_stats_essentials.largest_gap {
                    Some(prev_largest_gap) => {
                        if gap_between_files > prev_largest_gap {
                            previous_stats_essentials.largest_gap = Some(gap_between_files);
                        }
                    },
                    None => {
                        previous_stats_essentials.largest_gap = Some(gap_between_files);}
                }

            }

            // Update min an max timestmap
            if clean_processed_log_file.min_timestamp < previous_stats_essentials.min_timestamp {
                previous_stats_essentials.min_timestamp = clean_processed_log_file.min_timestamp
            }
            if clean_processed_log_file.max_timestamp > previous_stats_essentials.max_timestamp {
                previous_stats_essentials.max_timestamp = clean_processed_log_file.max_timestamp
            }

        } else {
            // This is the first one
            combined_processed_files_essentials = Some(clean_processed_log_file)
        }
    }
    if let Some(final_combined_essentials) = combined_processed_files_essentials{
        combined_processed_log_file.min_timestamp = Some(final_combined_essentials.min_timestamp);
        combined_processed_log_file.max_timestamp = Some(final_combined_essentials.max_timestamp);
        combined_processed_log_file.largest_gap = final_combined_essentials.largest_gap;
        combined_processed_log_file.mean_time_gap = Some(final_combined_essentials.time_gap_mean);
        combined_processed_log_file.variance_time_gap = Some(final_combined_essentials.time_gap_var);
    }
    
    combined_processed_log_file
}

fn combine_mean_values(count1: usize, mean1: f64, count2: usize, mean2: f64) -> Option<f64> {
    let total_count = count1 + count2;
    if total_count == 0 {
        return None; // avoid division by zero
    }
    let combined_mean = ((count1 as f64 * mean1) + (count2 as f64 * mean2)) / total_count as f64;
    Some(combined_mean)
}

fn get_combined_count_mean_and_var_of_two_sets(
    count1: usize,
    mean1: f64,
    var1: f64,
    count2: usize,
    mean2: f64,
    var2: f64,
) -> Option<(usize, f64, f64)> {
    let total_count = count1 + count2;
    if total_count == 0 {
        return None;
    }

    // get combined mean
    let combined_mean = combine_mean_values(count1, mean1, count2, mean2)?;

    // sum of squares for each sample: n * (var + mean^2)
    let ss1 = count1 as f64 * (var1 + mean1.powi(2));
    let ss2 = count2 as f64 * (var2 + mean2.powi(2));

    // total variance = ( (ss1 + ss2) / total_count ) - (combined_mean)^2
    let combined_var = ((ss1 + ss2) / total_count as f64) - combined_mean.powi(2);

    Some((total_count, combined_mean, combined_var))
}

fn get_updated_count_mean_var_when_add_value_to_set(
    initial_count: usize,
    initial_mean: f64,
    initial_var: f64,
    value_to_add: f64,
) -> (usize, f64, f64) {
    if initial_count == 0 {
        // base case: variance is 0 when only one sample
        return (1 as usize, value_to_add, 0.0 as f64);
    }

    let new_count = initial_count + 1;
    let delta = value_to_add - initial_mean;
    let new_mean = initial_mean + delta / new_count as f64;

    // variance update (sample variance definition)
    let new_var = ((initial_count as f64) * initial_var + delta * (value_to_add - new_mean))
        / (new_count as f64);

    (new_count, new_mean, new_var)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::tests::dummy_timegap;
    use chrono::NaiveDateTime;

    #[test]
    fn test_combine_mean_basic_combination() {
        let mean = combine_mean_values(2, 10.0, 2, 20.0);
        assert_eq!(mean, Some(15.0));
    }

    #[test]
    fn test_combine_mean_different_counts() {
        let mean = combine_mean_values(5, 4.6, 4, 6.5);
        let mean = mean.expect("mean was None");
        assert_eq!(mean, 5.444444444444445);
    }

    #[test]
    fn test_combine_var_count_mean_different_counts() {
        let (count, mean, var) =
            get_combined_count_mean_and_var_of_two_sets(5, 4.6, 4.64, 4, 6.5, 12.25).unwrap();
        assert_eq!(var, 8.913580246913579);
        assert_eq!(count, 9);
        assert_eq!(mean, 5.444444444444445);
    }

    #[test]
    fn test_add_value_to_set_and_get_updated_count_mean_var() {
        let (count, mean, var) = get_updated_count_mean_var_when_add_value_to_set(
            9,
            5.444444444444445,
            8.913580246913579,
            15.0,
        );
        assert_eq!(var, 16.24);
        assert_eq!(count, 10);
        assert_eq!(mean, 6.4);
    }

    fn sample_processed_log_file(
        name: &str,
        start_time: Option<&str>,
        end_time: Option<&str>,
        largest_gap: Option<i64>,
        mean_time_gap: Option<f64>,
        variance: Option<f64>,
        count: usize,
        errors: Vec<LavaError>,
        alerts: Vec<Alert>
    ) -> ProcessedLogFile {
        ProcessedLogFile {
            sha256hash: Some(
                "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2".to_string(),
            ),
            filename: Some(name.to_string()),
            file_path: Some(format!("C:/logs/{}", name)),
            size: Some("1.2 MB".to_string()),
            first_data_row_used: Some("2".to_string()),
            time_header: Some("timestamp".to_string()),
            time_format: Some("%Y-%m-%d %H:%M:%S".to_string()),
            min_timestamp: start_time
                .map(|et| NaiveDateTime::parse_from_str(&et, "%Y-%m-%d %H:%M:%S").unwrap()),
            max_timestamp: end_time
                .map(|et| NaiveDateTime::parse_from_str(&et, "%Y-%m-%d %H:%M:%S").unwrap()),
            largest_gap: largest_gap.map(|et| dummy_timegap(et)), // Example: 1 hour gap
            mean_time_gap: mean_time_gap,
            variance_time_gap: variance,
            total_num_records: count,
            timestamp_num_records: count,
            num_dupes: Some(2),
            num_redactions: Some(1),
            errors: errors,
            alerts: alerts,
        }
    }
    #[test]
    fn test_combine_processed_log_files_basic() {
        let log_files: Vec<ProcessedLogFile> = vec![
            sample_processed_log_file(
                "test1",
                Some("2025-08-13 05:00:00"),
                Some("2025-08-13 05:10:00"),
                Some(12000),
                Some(53037.0),
                Some(153231.8047),
                12,
                vec![LavaError::new("Some error", LavaErrorLevel::Critical)],
                vec![]
            ),
            sample_processed_log_file(
                "test2",
                Some("2025-08-13 05:11:00"),
                Some("2025-08-13 05:15:00"),
                Some(12001),
                Some(53039.0),
                Some(153231.8047),
                45,
                vec![],
                vec![Alert::new(AlertLevel::High, AlertType::DupeEvents)]
            ),
        ];
        let result = convert_vector_of_processed_log_files_into_one_for_multipart(&log_files);
        assert_eq!(result.min_timestamp, Some(NaiveDateTime::parse_from_str("2025-08-13 05:00:00", "%Y-%m-%d %H:%M:%S").unwrap()));
        assert_eq!(result.max_timestamp, Some(NaiveDateTime::parse_from_str("2025-08-13 05:15:00", "%Y-%m-%d %H:%M:%S").unwrap()));
        assert_eq!(result.largest_gap.unwrap().get_time_duration_number(), 12001000);
        assert_eq!(result.variance_time_gap, Some(1000419.603786759));
        assert_eq!(result.mean_time_gap, Some(53162.91071428571));
        assert_eq!(result.timestamp_num_records, 57);
        assert_eq!(result.alerts.len(), 1);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.num_redactions, Some(2));
        assert_eq!(result.num_dupes, Some(4));
        println!("{:?}",result.alerts);
    }
}
