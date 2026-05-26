use crate::commands::open_repository;
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::entries::Entry;
use std::path::Path;

pub fn cat(name: &str, matches: &ArgMatches) -> std::io::Result<i32> {
    let repository = open_repository(false);
    let path = matches.get_one::<String>("path").expect("required");

    if !repository
        .list_archives()?
        .into_iter()
        .any(|name| name == *name)
    {
        println!(
            "{} {} {}",
            "backup".red(),
            name.cyan(),
            "does not exist!".red()
        );

        return Ok(1);
    }

    let archive = repository.get_archive(name)?;

    if let Some(entry) = archive.find_archive_entry(Path::new(path)) {
        match entry {
            Entry::File(file) => {
                repository
                    .read_entry_content(Entry::File(file.clone()), &mut std::io::stdout().lock())?;
            }
            _ => {
                println!("{} {}", path.cyan(), "is not a file!".red());

                return Ok(1);
            }
        }
    } else {
        println!("{} {}", path.cyan(), "does not exist!".red());

        return Ok(1);
    }

    Ok(0)
}
