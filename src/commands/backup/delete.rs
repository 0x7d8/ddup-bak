use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use std::sync::Arc;

pub fn delete(matches: &ArgMatches) -> std::io::Result<i32> {
    let repository = open_repository(true);
    let name = matches.get_one::<String>("name").expect("required");

    if !repository
        .list_archives()?
        .into_iter()
        .any(|name| name == *name)
    {
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

            Arc::new(move |chunk, deleted| {
                progress.set_text(format!(
                    "{} {}",
                    format!("chunk #{chunk}").cyan(),
                    if deleted {
                        "(deleted)".green()
                    } else {
                        "(not deleted)".red()
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
