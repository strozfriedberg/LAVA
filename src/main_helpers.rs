use crate::PREBUILT_DATE_REGEXES;
use crate::basic_objects::*;
use crate::date_regex::DateRegex;
use crate::date_regex::RawDateRegex;
use crate::errors::*;
use clap::ArgMatches;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

pub fn print_compiled_regexes() -> () {
    println!(
        "Built in regex / strftime pairs for timestamp analysis. To provide your own, use --regexes, making sure to escape backslashes in yml file like \\\\\n"
    );
    for date_format in PREBUILT_DATE_REGEXES.iter() {
        println!("{}\n", date_format);
    }
}

pub fn get_full_execution_settings(matches: &ArgMatches) -> Result<ExecutionSettings> {
    // might want to perfrom lots of sanitation here
    let input_dir = PathBuf::from(
        matches
            .get_one::<String>("input")
            .ok_or_else(|| LavaError::new("No input parameter found.", LavaErrorLevel::Critical))?
            .clone(),
    );
    let output_dir = PathBuf::from(
        matches
            .get_one::<String>("output")
            .ok_or_else(|| LavaError::new("No output parameter found.", LavaErrorLevel::Critical))?
            .clone(),
    );

    setup_output_dir(&output_dir)?;

    let regexes: Vec<DateRegex> = if let Some(regex_yml_path) = matches.get_one::<String>("regexes")
    {
        get_user_supplied_regexes_from_command_line(Path::new(regex_yml_path)).map_err(|e| {
            LavaError::new(
                format!("Unable to open output file because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?
    } else {
        PREBUILT_DATE_REGEXES.clone()
    };

    let timestamp_field = matches.get_one::<String>("tf").cloned();

    Ok(ExecutionSettings {
        input: input_dir,
        output_dir: output_dir,
        regexes: regexes,
        timestamp_field: timestamp_field,
        quick_mode: matches.get_flag("quick"),
        multipart_mode: matches.get_flag("multipart"),
        verbose_mode: matches.get_flag("verbose"),
        actually_write_to_files: true,
    })
}

fn setup_output_dir(output_dir: &Path) -> Result<()> {
    if !output_dir.exists() {
        fs::create_dir_all(output_dir).map_err(|e| {
            LavaError::new(
                format!("Unable to create output directory because of {e}"),
                LavaErrorLevel::Critical,
            )
        })?;
    }

    // Create "Duplicates" and "Redactions" subdirectories
    let duplicates_dir = output_dir.join("Duplicates");
    let redactions_dir = output_dir.join("Redactions");

    fs::create_dir_all(&duplicates_dir).map_err(|e| {
        LavaError::new(
            format!("Unable to create output directory because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    fs::create_dir_all(&redactions_dir).map_err(|e| {
        LavaError::new(
            format!("Unable to create output directory because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;

    Ok(())
}

fn get_user_supplied_regexes_from_command_line(regex_file_path: &Path) -> Result<Vec<DateRegex>> {
    let content = fs::read_to_string(regex_file_path).map_err(|e| {
        LavaError::new(
            format!("Failed to read YAML file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let parsed: Vec<RawDateRegex> = serde_yaml::from_str(&content).map_err(|e| {
        LavaError::new(
            format!("Failed to parse YAML file because of {e}"),
            LavaErrorLevel::Critical,
        )
    })?;
    let converted: Vec<DateRegex> = parsed
        .into_iter()
        .map(DateRegex::new_from_raw_date_regex)
        .collect();
    Ok(converted)
}
