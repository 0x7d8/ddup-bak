use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::repository::Repository;
use std::path::Path;

pub fn init(matches: &ArgMatches) -> i32 {
    let directory = matches.get_one::<String>("directory").expect("required");
    let chunk_size = *matches.get_one::<usize>("chunk_size").expect("required");
    let max_chunk_count = *matches
        .get_one::<usize>("max_chunk_count")
        .expect("required");

    if std::path::Path::new(directory).join(".ddup-bak").exists() {
        println!("{} {}", ".ddup-bak".cyan(), "already exists!".red());

        return 1;
    }

    println!(
        "{} {} {}",
        "initializing".bright_black(),
        ".ddup-bak".cyan(),
        "...".bright_black()
    );

    Repository::new(
        Path::new(directory),
        chunk_size,
        max_chunk_count,
        Vec::new(),
    );

    println!(
        "{} {} {} {}",
        "initializing".bright_black(),
        ".ddup-bak".cyan(),
        "...".bright_black(),
        "DONE".green().bold()
    );

    0
}
