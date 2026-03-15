use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::repository::Repository;
use std::{path::Path, sync::Arc};

use crate::commands::Progress;

pub fn rebuild(matches: &ArgMatches) -> i32 {
    let directory = matches.get_one::<String>("directory").expect("required");
    let chunk_size = *matches.get_one::<usize>("chunk_size").expect("required");
    let max_chunk_count = *matches
        .get_one::<usize>("max_chunk_count")
        .expect("required");

    if !std::path::Path::new(directory).join(".ddup-bak").exists() {
        println!("{} {}", ".ddup-bak".cyan(), "does not exist!".red());

        return 1;
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
            progress.text.read().unwrap().cyan()
        )
    });

    progress.finish();

    Repository::open_or_rebuild(
        Path::new(directory),
        chunk_size,
        max_chunk_count,
        None,
        None,
        Some({
            let progress = progress.clone();

            Arc::new(move |chunk, _chunk_hash, references| {
                progress.set_text(format!(
                    "{} ({} references)",
                    format!("chunk #{chunk}").cyan(),
                    references
                ));
            })
        }),
    )
    .unwrap();

    println!(
        "{} {} {} {}",
        "rebuilding".bright_black(),
        ".ddup-bak".cyan(),
        "...".bright_black(),
        "DONE".green().bold()
    );

    0
}
