use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use std::{path::Path, sync::Arc};

pub fn create(matches: &ArgMatches) -> i32 {
    let mut repository = open_repository(true);
    let name = matches.get_one::<String>("name").expect("required");
    let directory = matches.get_one::<String>("directory");
    let threads = matches.get_one::<usize>("threads").expect("required");
    let compression = matches.get_one::<String>("compression").expect("required");
    let compression = match compression.as_str() {
        "none" => ddup_bak::archive::CompressionFormat::None,
        "gzip" => ddup_bak::archive::CompressionFormat::Gzip,
        "deflate" => ddup_bak::archive::CompressionFormat::Deflate,
        "brotli" => ddup_bak::archive::CompressionFormat::Brotli,
        _ => panic!("invalid compression format"),
    };

    if repository
        .list_archives()
        .unwrap()
        .into_iter()
        .any(|backup| backup == *name)
    {
        println!(
            "{} {} {}",
            "backup".red(),
            name.cyan(),
            "already exists!".red()
        );

        return 1;
    }

    println!("{}", "creating backup...".bright_black());

    let mut progress = Progress::new(usize::MAX);
    progress.spinner(|progress, spinner| {
        format!(
            "\r\x1B[K {} {} {}",
            "chunking...".bright_black().italic(),
            spinner.cyan(),
            progress.text.read().unwrap().cyan()
        )
    });

    repository
        .create_archive(
            name,
            directory.map(Path::new),
            Some({
                let progress = progress.clone();

                Arc::new(move |file| {
                    progress.set_text(file.to_string_lossy());
                })
            }),
            None,
            Some(Arc::new(move |_, _| compression)),
            *threads,
        )
        .unwrap();

    progress.finish();

    println!(
        "{} {}",
        "creating backup...".bright_black(),
        "DONE".green().bold()
    );

    0
}
