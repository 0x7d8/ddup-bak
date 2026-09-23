use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use std::sync::Arc;

pub fn delete(matches: &ArgMatches) -> std::io::Result<i32> {
    let repository = open_repository();
    let name = matches.get_one::<String>("name").expect("required");

    let exists = |names: Vec<String>| names.iter().any(|backup| backup == name);
    if !exists(repository.list_archives()?) && !exists(repository.pending_deletions()?) {
        println!(
            "{} {} {}",
            "backup".red(),
            name.cyan(),
            "does not exist!".red()
        );

        return Ok(1);
    }

    println!("{}", "deleting backup...".bright_black());

    let mut progress = Progress::new(usize::MAX);
    progress.spinner(|progress, spinner| {
        format!(
            "\r\x1B[K {} {} {}",
            "dereferencing chunks...".bright_black().italic(),
            spinner.cyan(),
            progress.text.read().cyan()
        )
    });

    repository.delete_archive(
        name,
        Some({
            let progress = progress.clone();

            Arc::new(move |hash, deleted| {
                progress.set_text(format!(
                    "{} {}",
                    ddup_bak::chunks::hex(hash)[..12].cyan(),
                    if deleted {
                        "(deleted)".green()
                    } else {
                        "(kept)".bright_black()
                    }
                ));
            })
        }),
    )?;

    progress.finish();

    println!(
        "{} {}",
        "deleting backup...".bright_black(),
        "DONE".green().bold()
    );

    Ok(0)
}
