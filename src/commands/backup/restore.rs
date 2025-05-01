use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::entries::Entry;
use std::sync::Arc;

pub fn restore(matches: &ArgMatches) -> i32 {
    let repository = open_repository(false);

    let name = matches.get_one::<String>("name").expect("required");
    let destination = matches.get_one::<String>("destination");
    let threads = matches.get_one::<usize>("threads").expect("required");

    if !repository
        .list_archives()
        .unwrap()
        .into_iter()
        .any(|name| name == *name)
    {
        println!(
            "{} {} {}",
            "backup".red(),
            name.cyan(),
            "does not exist!".red()
        );

        return 1;
    }

    println!("{}", "restoring backup...".bright_black());

    let archive = repository.get_archive(name).unwrap();

    fn recursive_count_entries(entry: &Entry) -> usize {
        match entry {
            Entry::Directory(entries) => {
                let mut count = 1;

                for entry in entries.entries.iter() {
                    count += recursive_count_entries(entry);
                }

                count
            }
            _ => 1,
        }
    }

    let mut total = 0;
    for entry in archive.entries().iter() {
        total += recursive_count_entries(entry);
    }

    let mut progress = Progress::new(total);
    progress.spinner(|progress, spinner| {
        format!(
            "\r\x1B[K {} {} {}/{} ({}%)",
            "restoring chunks...".bright_black().italic(),
            spinner.cyan(),
            progress.progress().to_string().cyan(),
            progress.total.to_string().cyan(),
            progress.percent().round().to_string().cyan()
        )
    });

    repository
        .restore_entries(
            name,
            archive.into_entries(),
            Some({
                let progress = progress.clone();

                Arc::new(move |_| {
                    progress.incr(1usize);
                })
            }),
            *threads,
        )
        .unwrap();

    progress.finish();

    println!(
        "{} {}",
        "restoring backup...".bright_black(),
        "DONE".green().bold()
    );

    if let Some(destination) = destination {
        println!(
            "{} {}{}",
            "restoring to".bright_black(),
            destination.cyan(),
            "...".bright_black()
        );

        if std::path::Path::new(destination).exists() {
            for entry in std::fs::read_dir(destination).unwrap().flatten() {
                let path = entry.path();

                if path.file_name().unwrap() == ".ddup-bak" {
                    continue;
                }

                if path.is_file() {
                    std::fs::remove_file(path).unwrap();
                } else if path.is_dir() {
                    std::fs::remove_dir_all(path).unwrap();
                }
            }
        }

        let source = std::path::Path::new(".ddup-bak/archives-restored/").join(name);
        let destination = std::path::Path::new(destination);

        std::fs::create_dir_all(destination).unwrap();

        for entry in std::fs::read_dir(source).unwrap().flatten() {
            let path = entry.path();
            let destination_path = destination.join(path.file_name().unwrap());

            std::fs::rename(path, destination_path).unwrap();
        }

        println!(
            "{} {} {} {}",
            "restoring to".bright_black(),
            destination.to_string_lossy().cyan(),
            "...".bright_black(),
            "DONE".green().bold()
        );
    }

    0
}
