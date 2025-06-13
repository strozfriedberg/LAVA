use crate::alerts::*;
use crate::basic_objects::*;
use crate::errors::*;
use chrono::{TimeDelta, Utc};
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::*;
use csv::StringRecord;
use csv::Writer;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub fn generate_log_filename() -> String {
    let now = Utc::now();
    let formatted = now.format("%Y-%m-%d_%H-%M-%S_LAVA_Output.csv");
    formatted.to_string()
}

pub fn format_timedelta(tdelta: TimeDelta) -> String {
    let total_seconds = tdelta.num_seconds().abs(); // make it positive for display

    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
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
        "Number of Records",
        "Earliest Timestamp",
        "Latest Timestamp",
        "Duration of Entire Log File",
        "Largest Time Gap",
        "Duration of Largest Time Gap",
        &format!("Mean {} of Time Gaps", WELFORD_TIME_SIGNIFIGANCE),
        &format!("Standard Deviation in {}", WELFORD_TIME_SIGNIFIGANCE),
        "Largest Time Gap Num Standard Deviations Above",
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
        let error_message = if log_file.errors.is_empty() {
            String::new()
        } else {
            if log_file.errors.len() > 1 {
                format!(
                    "There were {} errors during processing. Check errors.csv for detailed errors.",
                    log_file.errors.len()
                )
            } else {
                log_file.errors[0].reason.clone()
            }
        };
        wtr.serialize(vec![
            log_file.filename.as_deref().unwrap_or(""),
            log_file.file_path.as_deref().unwrap_or(""),
            log_file.sha256hash.as_deref().unwrap_or(""),
            log_file.size.as_deref().unwrap_or(""),
            log_file.first_data_row_used.as_deref().unwrap_or(""),
            log_file.time_header.as_deref().unwrap_or(""),
            log_file.time_format.as_deref().unwrap_or(""),
            log_file.num_records.as_deref().unwrap_or(""),
            log_file.min_timestamp.as_deref().unwrap_or(""),
            log_file.max_timestamp.as_deref().unwrap_or(""),
            log_file.min_max_duration.as_deref().unwrap_or(""),
            log_file.largest_gap.as_deref().unwrap_or(""),
            log_file.largest_gap_duration.as_deref().unwrap_or(""),
            log_file.mean_time_gap.as_deref().unwrap_or(""),
            log_file.std_dev_time_gap.as_deref().unwrap_or(""),
            log_file.number_of_std_devs_above.as_deref().unwrap_or(""),
            log_file.num_dupes.as_deref().unwrap_or(""),
            log_file.num_redactions.as_deref().unwrap_or(""),
            &error_message,
        ])
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
    println!("Data written to {}", output_filepath.to_string_lossy());
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

pub fn print_pretty_alerts_and_write_to_output_file(
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
