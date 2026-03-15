use std::sync::Arc;

use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;

pub fn clean(_matches: &ArgMatches) -> i32 {
    let repository = open_repository(true);

    println!("{}", "cleaning repository...".bright_black());

    let mut progress = Progress::new(usize::MAX);
    progress.spinner(|progress, spinner| {
        format!(
            "\r\x1B[K {} {} {}",
            "cleaning repository...".bright_black().italic(),
            spinner.cyan(),
            progress.text.read().unwrap().cyan()
        )
    });

    repository
        .clean(Some({
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
        }))
        .unwrap();

    progress.finish();

    println!(
        "{} {}",
        "cleaning repository...".bright_black(),
        "DONE".green().bold()
    );

    0
}
