use crate::commands::Progress;
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::repository::Repository;
use std::{path::Path, sync::Arc};

pub fn rebuild(matches: &ArgMatches) -> std::io::Result<i32> {
    let directory = matches.get_one::<String>("directory").expect("required");
    let chunk_size = *matches.get_one::<usize>("chunk_size").expect("required");
    let max_chunk_count = *matches
        .get_one::<usize>("max_chunk_count")
        .expect("required");

    if !Path::new(directory).join(".ddup-bak").exists() {
        println!("{} {}", ".ddup-bak".cyan(), "does not exist!".red());
        return Ok(1);
    }

    println!(
        "{} {} {}",
        "rebuilding".bright_black(),
        ".ddup-bak".cyan(),
        "...".bright_black()
    );

    let mut progress = Progress::new(usize::MAX);
    progress.spinner(|progress, spinner| {
        format!(
            "\r\x1B[K {} {} {}",
            "rebuilding repository...".bright_black().italic(),
            spinner.cyan(),
            progress.text.read().cyan()
        )
    });

    let repository = Repository::rebuild(
        Path::new(directory),
        chunk_size,
        max_chunk_count,
        None,
        None,
        Some({
            let progress = progress.clone();
            Arc::new(move |hash, references| {
                progress.set_text(format!(
                    "{} ({references} references)",
                    ddup_bak::chunks::hex(hash)[..12].cyan()
                ));
            })
        }),
    )?;

    progress.finish();
    println!(
        "{} {} {} {}",
        "rebuilding".bright_black(),
        ".ddup-bak".cyan(),
        "...".bright_black(),
        "DONE".green().bold()
    );

    // Anything left unreadable is what the rebuild could not account for, and it is why
    // cleaning is now refused, so say so rather than leaving it to be discovered.
    let unreadable = repository.unreadable_archives()?;
    if !unreadable.is_empty() {
        println!();
        println!(
            "{} {}",
            "could not read:".red(),
            unreadable.join(", ").cyan()
        );
        if let Err(err) = repository.get_archive(&unreadable[0]) {
            println!("{} {}", unreadable[0].cyan(), err);
        }
        println!(
            "{}",
            "their chunks are kept and no chunk can be deleted until they are gone".bright_black()
        );
    }

    Ok(0)
}
