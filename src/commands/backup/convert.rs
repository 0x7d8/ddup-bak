use crate::commands::{Progress, open_repository};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::entries::Entry;
use std::{fs::File, io::Write};

enum Format {
    Tar,
    TarGz,
    Ddup,
}

pub fn convert(matches: &ArgMatches) -> i32 {
    let mut repository = open_repository(false);

    let name = matches.get_one::<String>("name").expect("required");
    let output = matches.get_one::<String>("output");
    let format = matches.get_one::<String>("format").expect("required");
    let format = match format.as_str() {
        "tar" => Format::Tar,
        "tar.gz" => Format::TarGz,
        "ddup" => Format::Ddup,
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

        let file = File::create(output).unwrap();

        convert_entries_file(
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
                _ => unreachable!(),
            };

            let mut tar = tar::Builder::new(output);
            tar.mode(tar::HeaderMode::Complete);

            for entry in entries {
                tar_recursive_convert_entries(entry, repository, &mut tar, progress, "");
            }

            tar.finish().unwrap();
        }
        _ => unimplemented!(),
    }
}

fn convert_entries_file(
    repository: &mut ddup_bak::repository::Repository,
    entries: Vec<Entry>,
    output: File,
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
                _ => unreachable!(),
            };

            let mut tar = tar::Builder::new(output);
            tar.mode(tar::HeaderMode::Complete);

            for entry in entries {
                tar_recursive_convert_entries(entry, repository, &mut tar, progress, "");
            }

            tar.finish().unwrap();
        }
        Format::Ddup => {
            let mut archive = ddup_bak::archive::Archive::new(output);

            for entry in entries {
                ddup_recursive_convert_entries(entry, repository, &mut archive, progress, None);
            }

            archive.write_end_header().unwrap();
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

fn ddup_recursive_convert_entries(
    entry: Entry,
    repository: &mut ddup_bak::repository::Repository,
    archive: &mut ddup_bak::archive::Archive,
    progress: Option<&Progress>,
    parent_entry: Option<&mut ddup_bak::archive::entries::DirectoryEntry>,
) {
    match entry {
        Entry::Directory(directory) => {
            let mut dir_entry = ddup_bak::archive::entries::DirectoryEntry {
                name: directory.name,
                owner: directory.owner,
                mode: directory.mode,
                mtime: directory.mtime,
                entries: Vec::new(),
            };

            if let Some(progress) = progress {
                progress.incr(1usize);
            }

            for entry in directory.entries {
                ddup_recursive_convert_entries(
                    entry,
                    repository,
                    archive,
                    progress,
                    Some(&mut dir_entry),
                );
            }

            if let Some(parent) = parent_entry {
                parent.entries.push(Entry::Directory(Box::new(dir_entry)));
            } else {
                archive.entries.push(Entry::Directory(Box::new(dir_entry)));
            }
        }
        Entry::File(file) => {
            let file_entry = archive
                .write_file_entry(
                    repository.entry_reader(Entry::File(file.clone())).unwrap(),
                    None,
                    file.name,
                    file.mode,
                    file.mtime,
                    file.owner,
                    ddup_bak::archive::CompressionFormat::Deflate,
                )
                .unwrap();

            if let Some(parent) = parent_entry {
                parent.entries.push(Entry::File(file_entry));
            } else {
                archive.entries.push(Entry::File(file_entry));
            }

            if let Some(progress) = progress {
                progress.incr(1usize);
            }
        }
        Entry::Symlink(link) => {
            if let Some(parent) = parent_entry {
                parent.entries.push(Entry::Symlink(link));
            } else {
                archive.entries.push(Entry::Symlink(link));
            }

            if let Some(progress) = progress {
                progress.incr(1usize);
            }
        }
    }
}
