use std::sync::Arc;

use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;

pub fn create(matches: &ArgMatches) -> i32 {
    let mut repository = open_repository();
    let name = matches.get_one::<String>("name").expect("required");

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
            None,
            Some({
                let progress = progress.clone();

                Arc::new(move |file| {
                    progress.set_text(file.to_string_lossy());
                })
            }),
            None,
            16,
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
