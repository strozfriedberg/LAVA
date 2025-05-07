use clap::ArgMatches;
use log_checker::basic_objects::*;
use log_checker::errors::*;
use std::path::PathBuf;
use std::path::Path;
use std::fs;
use log_checker::date_regex::DateRegex;
use log_checker::date_regex::RawDateRegex;

pub fn get_full_command_line_args(matches: &ArgMatches) -> Result<CommandLineArgs> { // might want to perfrom lots of sanitation here
    let input_dir = PathBuf::from(matches.get_one::<String>("input").ok_or_else(|| LogCheckError::new("No input parameter found."))?.clone());
    let output_dir = PathBuf::from(matches.get_one::<String>("output").ok_or_else(|| LogCheckError::new("No output parameter found."))?.clone());

    setup_output_dir(&output_dir)?;

    let regexes = matches
    .get_one::<String>("regexes")
    .map(|regex_yml_path| {
        get_user_supplied_regexes_from_command_line(Path::new(regex_yml_path))
            .map_err(|e| LogCheckError::new(format!("Unable to open output file because of {e}")))
    })
    .transpose()?;

    Ok(CommandLineArgs {
        input_dir: input_dir,
        output_dir: output_dir,
        provided_regexes: regexes,

    })
}

pub fn setup_output_dir(output_dir: &Path) -> Result<()>{
    if !output_dir.exists() {
        fs::create_dir_all(output_dir).map_err(|e| LogCheckError::new(format!("Unable to create output directory because of {e}")))?;
    }

    // Create "Duplicates" and "Redactions" subdirectories
    let duplicates_dir = output_dir.join("Duplicates");
    let redactions_dir = output_dir.join("Redactions");

    fs::create_dir_all(&duplicates_dir).map_err(|e| LogCheckError::new(format!("Unable to create output directory because of {e}")))?;
    fs::create_dir_all(&redactions_dir).map_err(|e| LogCheckError::new(format!("Unable to create output directory because of {e}")))?;

    Ok(())
}

pub fn get_user_supplied_regexes_from_command_line(regex_file_path: &Path) -> Result<Vec<DateRegex>>{
    let content = fs::read_to_string(regex_file_path).map_err(|e| LogCheckError::new(format!("Failed to read YAML file because of {e}")))?;
    let parsed: Vec<RawDateRegex> = serde_yaml::from_str(&content).map_err(|e| LogCheckError::new(format!("Failed to parse YAML file because of {e}")))?;
    let converted: Vec<DateRegex> = parsed.into_iter()
                                    .map(DateRegex::new_from_raw_date_regex)
                                    .collect();
    Ok(converted)
}