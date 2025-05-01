use crate::commands::open_repository;
use clap::ArgMatches;
use colored::Colorize;

pub fn list(_matches: &ArgMatches) -> i32 {
    let repository = open_repository(false);

    println!("{}", "listing backups...".bright_black());

    let list = repository.list_archives().unwrap();

    println!(
        "{} {}",
        "listing backups...".bright_black(),
        "DONE".green().bold()
    );

    if list.is_empty() {
        println!();
        println!("{}", "no backups found".red());
        return 1;
    }

    for backup in list {
        println!();
        println!("{}", backup.cyan().bold().underline());
    }

    0
}
