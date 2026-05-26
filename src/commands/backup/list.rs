use crate::commands::open_repository;
use clap::ArgMatches;
use colored::Colorize;

pub fn list(_matches: &ArgMatches) -> std::io::Result<i32> {
    let repository = open_repository(false);

    println!("{}", "listing backups...".bright_black());

    let list = repository.list_archives()?;

    println!(
        "{} {}",
        "listing backups...".bright_black(),
        "DONE".green().bold()
    );

    if list.is_empty() {
        println!();
        println!("{}", "no backups found".red());
        return Ok(1);
    }

    println!();

    for backup in list {
        println!("{}", backup.cyan().bold().underline());
    }

    Ok(0)
}
