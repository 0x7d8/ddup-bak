use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::{archive::entries::Entry, lock::Lock};
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

    // Held until the restore is done, so no delete takes the chunks between here and there.
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
    // Restore into a fresh directory on the destination's filesystem, then swap it in, so a
    // failed restore never touches the existing destination contents. Without a destination
    // the repository restores into its own place, where restores of one archive take turns.
    // Restores into one destination take turns, from staging through the swap below.
    let mut turn = None;
    let staging = match &destination {
        Some(destination) => {
            std::fs::create_dir_all(destination)?;
            let locks = repository.directory.join(".ddup-bak/restore-locks");
            std::fs::create_dir_all(&locks)?;
            let key = ddup_bak::chunks::hex(
                blake3::hash(destination.canonicalize()?.as_os_str().as_encoded_bytes()).as_bytes(),
            );
            turn = Some(Lock::exclusive(
                &locks.join(format!("destination-{}", &key[..16])),
            )?);
            let staging = destination.join(STAGING_DIR);
            remove_if_exists(&staging)?;
            repository.restore_entries_to(
                archive.into_entries(),
                &staging,
                progress_callback,
                threads,
            )?;
            staging
        }
        None => {
            repository.restore_entries(name, archive.into_entries(), progress_callback, threads)?
        }
    };

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
        drop(turn);

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
    ddup_bak::repository::remove_restored(path)
}
