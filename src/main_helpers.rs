use std::path::PathBuf;
use clap::ArgMatches;
use log_checker::basic_objects::*;
use log_checker::errors::*;
use std::fs;


pub fn get_full_command_line_args(matches: &ArgMatches) -> Result<CommandLineArgs> { // might want to perfrom lots of sanitation here
    let input_dir = PathBuf::from(matches.get_one::<String>("input").ok_or_else(|| LogCheckError::new("No input parameter found."))?.clone());
    let output_dir = PathBuf::from(matches.get_one::<String>("output").ok_or_else(|| LogCheckError::new("No output parameter found."))?.clone());

    setup_output_dir(&output_dir)?;

    Ok(CommandLineArgs {
        input_dir: input_dir,
        output_dir: output_dir,
    })
}

pub fn setup_output_dir(output_dir: &PathBuf) -> Result<()>{
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