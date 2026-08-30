use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::repository::Repository;
use std::path::Path;

pub fn init(matches: &ArgMatches) -> std::io::Result<i32> {
    let directory = matches.get_one::<String>("directory").expect("required");
    let chunk_size = *matches.get_one::<usize>("chunk_size").expect("required");
    let max_chunk_count = *matches
        .get_one::<usize>("max_chunk_count")
        .expect("required");
    let hash_algorithm = matches
        .get_one::<String>("hash")
        .expect("required")
        .parse::<ddup_bak::chunks::HashAlgorithm>()?;

    if std::path::Path::new(directory).join(".ddup-bak").exists() {
        println!("{} {}", ".ddup-bak".cyan(), "already exists!".red());

        return Ok(1);
    }

    println!(
        "{} {} {}",
        "initializing".bright_black(),
        ".ddup-bak".cyan(),
        "...".bright_black()
    );

    Repository::new_with_hash(
        Path::new(directory),
        chunk_size,
        max_chunk_count,
        hash_algorithm,
        None,
    )?;

    println!(
        "{} {} {} {}",
        "initializing".bright_black(),
        ".ddup-bak".cyan(),
        "...".bright_black(),
        "DONE".green().bold()
    );

    Ok(0)
}
