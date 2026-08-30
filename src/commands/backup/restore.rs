use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::entries::Entry;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

const STAGING_DIR: &str = ".ddup-bak-restore";

pub fn restore(matches: &ArgMatches) -> std::io::Result<i32> {
    let repository = open_repository();
    let name = matches.get_one::<String>("name").expect("required");
    let destination = matches.get_one::<String>("destination").map(PathBuf::from);
    let threads = *matches.get_one::<usize>("threads").expect("required");

    if !repository
        .list_archives()?
        .iter()
        .any(|backup| backup == name)
    {
        println!(
            "{} {} {}",
            "backup".red(),
            name.cyan(),
            "does not exist!".red()
        );
        return Ok(1);
    }

    println!("{}", "restoring backup...".bright_black());

    let archive = repository.get_archive(name)?;
    let total = archive.entries().iter().map(count_entries).sum();

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

    // Restore into a fresh directory on the destination's filesystem, then swap it in, so a
    // failed restore never touches the existing destination contents.
    let staging = match &destination {
        Some(destination) => destination.join(STAGING_DIR),
        None => Path::new(".ddup-bak/archives-restored").join(name),
    };
    remove_if_exists(&staging)?;

    repository.restore_entries_to(
        archive.into_entries(),
        &staging,
        Some({
            let progress = progress.clone();
            Arc::new(move |_| progress.incr(1usize))
        }),
        threads,
    )?;

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
            destination.display().to_string().cyan(),
            "...".bright_black()
        );

        for entry in std::fs::read_dir(&destination)? {
            let entry = entry?;
            if entry.file_name() == ".ddup-bak" || entry.file_name() == STAGING_DIR {
                continue;
            }
            remove_if_exists(&entry.path())?;
        }

        for entry in std::fs::read_dir(&staging)? {
            let entry = entry?;
            std::fs::rename(entry.path(), destination.join(entry.file_name()))?;
        }
        std::fs::remove_dir(&staging)?;

        println!(
            "{} {} {} {}",
            "restoring to".bright_black(),
            destination.display().to_string().cyan(),
            "...".bright_black(),
            "DONE".green().bold()
        );
    }

    Ok(0)
}

fn count_entries(entry: &Entry) -> usize {
    match entry {
        Entry::Directory(dir) => 1 + dir.entries.iter().map(count_entries).sum::<usize>(),
        _ => 1,
    }
}

fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    let Ok(metadata) = path.symlink_metadata() else {
        return Ok(());
    };

    if metadata.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}
