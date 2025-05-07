use std::path::PathBuf;
use clap::ArgMatches;
use log_checker::basic_objects::*;
use log_checker::errors::*;


pub fn get_full_command_line_args(matches: &ArgMatches) -> Result<CommandLineArgs> {
    let input_dir = matches.get_one::<PathBuf>("input").ok_or_else(|| LogCheckError::new("No input parameter found."))?;
    let output_dir = matches.get_one::<PathBuf>("output").ok_or_else(|| LogCheckError::new("No output parameter found."))?;

    Ok(CommandLineArgs {
        input_dir: input_dir.clone(),
        output_dir: output_dir.clone(),
    })
}

pub fn setup_output_dir(output_dir: &PathBuf) -> Result<()>{
    Ok(())
}