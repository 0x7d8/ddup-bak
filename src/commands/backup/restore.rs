use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::entries::Entry;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

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

    // Held through the restore so no delete removes the chunks.
    let _lock = repository.shared_lock()?;
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

    let progress_callback = Some({
        let progress = progress.clone();

        Arc::new(move |_: &Path| progress.incr(1usize)) as Arc<_>
    });
    match &destination {
        Some(destination) => {
            repository.restore_entries_replacing(
                archive.into_entries(),
                destination,
                progress_callback,
                threads,
            )?;
        }
        None => {
            repository.restore_entries(name, archive.into_entries(), progress_callback, threads)?;
        }
    }

    progress.finish();

    println!(
        "{} {}",
        "restoring backup...".bright_black(),
        "DONE".green().bold()
    );

    Ok(0)
}

fn count_entries(entry: &Entry) -> usize {
    match entry {
        Entry::Directory(dir) => 1 + dir.entries.iter().map(count_entries).sum::<usize>(),
        _ => 1,
    }
}
