use crate::alerts::*;
use crate::basic_objects::*;
use crate::errors::*;
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
        if let Some(alerts) = &processed.alerts {
            for alert in alerts.iter() {
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
