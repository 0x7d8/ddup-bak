use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use std::sync::Arc;

pub fn clean(_matches: &ArgMatches) -> std::io::Result<i32> {
    let repository = open_repository(true);

    println!("{}", "cleaning repository...".bright_black());

    let mut progress = Progress::new(usize::MAX);
    progress.spinner(|progress, spinner| {
        format!(
            "\r\x1B[K {} {} {}",
            "cleaning repository...".bright_black().italic(),
            spinner.cyan(),
            progress.text.read().cyan()
        )
    });

    repository.clean(Some({
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
    }))?;

    progress.finish();

    println!(
        "{} {}",
        "cleaning repository...".bright_black(),
        "DONE".green().bold()
    );

    Ok(0)
}
