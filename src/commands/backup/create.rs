use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::CompressionFormat;
use std::{path::Path, sync::Arc};

pub fn create(matches: &ArgMatches) -> std::io::Result<i32> {
    let repository = open_repository();
    let name = matches.get_one::<String>("name").expect("required");
    let directory = matches.get_one::<String>("directory").map(Path::new);
    let threads = *matches.get_one::<usize>("threads").expect("required");
    let compression = match matches
        .get_one::<String>("compression")
        .expect("required")
        .as_str()
    {
        "none" => CompressionFormat::None,
        "gzip" => CompressionFormat::Gzip,
        "deflate" => CompressionFormat::Deflate,
        "brotli" => CompressionFormat::Brotli,
        "zstd" => CompressionFormat::Zstd,
        _ => panic!("invalid compression format"),
    };
    if !compression.is_supported() {
        println!("{}", compression.unsupported_message().red());
        return Ok(1);
    }

    if repository
        .list_archives()?
        .iter()
        .any(|backup| backup == name)
    {
        println!(
            "{} {} {}",
            "backup".red(),
            name.cyan(),
            "already exists!".red()
        );
        return Ok(1);
    }

    println!("{}", "creating backup...".bright_black());

    let mut progress = Progress::new(usize::MAX);
    progress.spinner(|progress, spinner| {
        format!(
            "\r\x1B[K {} {} {}",
            "chunking...".bright_black().italic(),
            spinner.cyan(),
            progress.text.read().cyan()
        )
    });

    let walker = directory.map(|directory| {
        ignore::WalkBuilder::new(directory)
            .follow_links(false)
            .git_global(false)
            .build()
    });

    repository.create_archive(
        name,
        walker,
        directory,
        Some({
            let progress = progress.clone();
            Arc::new(move |file| progress.set_text(file.to_string_lossy()))
        }),
        Some(Arc::new(move |_, _| compression)),
        threads,
    )?;

    progress.finish();
    println!(
        "{} {}",
        "creating backup...".bright_black(),
        "DONE".green().bold()
    );

    Ok(0)
}
