use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::entries::Entry;
use std::io::Write;

enum Format {
    Tar,
    TarGz,
}

pub fn convert(matches: &ArgMatches) -> i32 {
    let mut repository = open_repository(false);

    let name = matches.get_one::<String>("name").expect("required");
    let output = matches.get_one::<String>("output");
    let format = matches.get_one::<String>("format").expect("required");
    let format = match format.as_str() {
        "tar" => Format::Tar,
        "tar.gz" => Format::TarGz,
        _ => panic!("invalid format"),
    };

    if !repository
        .list_archives()
        .unwrap()
        .into_iter()
        .any(|archive_name| archive_name == *name)
    {
        println!(
            "{} {} {}",
            "backup".red(),
            name.cyan(),
            "does not exist!".red()
        );

        return 1;
    }

    let archive = repository.get_archive(name).unwrap();

    if let Some(output) = output {
        println!("{}", "converting backup...".bright_black());

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
                "resolving chunks...".bright_black().italic(),
                spinner.cyan(),
                progress.progress().to_string().cyan(),
                progress.total.to_string().cyan(),
                progress.percent().round().to_string().cyan()
            )
        });

        let file = std::fs::File::create(output).unwrap();

        convert_entries(
            &mut repository,
            archive.into_entries(),
            file,
            Some(&progress),
            format,
        );

        progress.finish();

        println!(
            "{} {}",
            "converting backup...".bright_black(),
            "DONE".green().bold()
        );
    } else {
        let output = std::io::stdout().lock();

        convert_entries(
            &mut repository,
            archive.into_entries(),
            output,
            None,
            format,
        );
    }

    0
}

fn convert_entries<S: Write + 'static>(
    repository: &mut ddup_bak::repository::Repository,
    entries: Vec<Entry>,
    output: S,
    progress: Option<&Progress>,
    format: Format,
) {
    match format {
        Format::Tar | Format::TarGz => {
            let output: Box<dyn Write + 'static> = match format {
                Format::Tar => Box::new(output),
                Format::TarGz => Box::new(flate2::write::GzEncoder::new(
                    output,
                    flate2::Compression::default(),
                )),
            };

            let mut tar = tar::Builder::new(output);
            tar.mode(tar::HeaderMode::Complete);

            for entry in entries {
                tar_recursive_convert_entries(entry, repository, &mut tar, progress, "");
            }

            tar.finish().unwrap();
        }
    }
}

fn tar_recursive_convert_entries(
    entry: Entry,
    repository: &mut ddup_bak::repository::Repository,
    archive: &mut tar::Builder<Box<dyn Write>>,
    progress: Option<&Progress>,
    parent_path: &str,
) {
    match entry {
        Entry::Directory(entries) => {
            let path = if parent_path.is_empty() {
                entries.name.clone()
            } else {
                format!("{}/{}", parent_path, entries.name)
            };

            let mut entry_header = tar::Header::new_gnu();
            entry_header.set_uid(entries.owner.0 as u64);
            entry_header.set_gid(entries.owner.1 as u64);
            entry_header.set_mode(entries.mode.bits());

            entry_header.set_mtime(
                entries
                    .mtime
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            entry_header.set_entry_type(tar::EntryType::Directory);

            let dir_path = if path.ends_with('/') {
                path.clone()
            } else {
                format!("{}/", path)
            };

            archive
                .append_data(&mut entry_header, &dir_path, std::io::empty())
                .unwrap();

            if let Some(progress) = progress {
                progress.incr(1usize);
            }

            for entry in entries.entries {
                tar_recursive_convert_entries(entry, repository, archive, progress, &path);
            }
        }
        Entry::File(file) => {
            let path = if parent_path.is_empty() {
                file.name.clone()
            } else {
                format!("{}/{}", parent_path, file.name)
            };

            let mut entry_header = tar::Header::new_gnu();
            entry_header.set_uid(file.owner.0 as u64);
            entry_header.set_gid(file.owner.1 as u64);
            entry_header.set_mode(file.mode.bits());

            entry_header.set_mtime(
                file.mtime
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            entry_header.set_entry_type(tar::EntryType::Regular);
            entry_header.set_size(file.size_real);

            let reader = repository.entry_reader(Entry::File(file.clone())).unwrap();

            archive
                .append_data(&mut entry_header, &path, reader)
                .unwrap();

            if let Some(progress) = progress {
                progress.incr(1usize);
            }
        }
        Entry::Symlink(link) => {
            let path = if parent_path.is_empty() {
                link.name.clone()
            } else {
                format!("{}/{}", parent_path, link.name)
            };

            let mut entry_header = tar::Header::new_gnu();
            entry_header.set_uid(link.owner.0 as u64);
            entry_header.set_gid(link.owner.1 as u64);
            entry_header.set_mode(link.mode.bits());

            entry_header.set_mtime(
                link.mtime
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            entry_header.set_entry_type(tar::EntryType::Symlink);

            archive
                .append_link(&mut entry_header, &path, &link.target)
                .unwrap();

            if let Some(progress) = progress {
                progress.incr(1usize);
            }
        }
    }
}
