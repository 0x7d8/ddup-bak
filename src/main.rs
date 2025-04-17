use std::{
    io::{Read, Write},
    path::Path,
};

fn restore_entry(entry: ddup_bak::Entry, directory: &str) {
    println!(
        "Restoring entry: {}",
        Path::new(directory).join(entry.name()).display()
    );

    match entry {
        ddup_bak::Entry::File(mut file_entry) => {
            let file_path = std::path::Path::new(directory).join(&file_entry.name);
            let mut file = std::fs::File::create(&file_path).unwrap();

            let mut buffer = vec![0; 1024 * 1024];
            loop {
                let bytes_read = file_entry.read(&mut buffer).unwrap();
                if bytes_read == 0 {
                    break;
                }

                file.write_all(&buffer[..bytes_read]).unwrap();
            }
        }
        ddup_bak::Entry::Directory(dir_entry) => {
            let dir_path = std::path::Path::new(directory).join(dir_entry.name);
            std::fs::create_dir_all(&dir_path).unwrap();

            for sub_entry in dir_entry.entries {
                restore_entry(sub_entry, dir_path.to_str().unwrap());
            }
        }
        #[cfg(unix)]
        ddup_bak::Entry::Symlink(link_entry) => {
            let link_path = std::path::Path::new(directory).join(&link_entry.name);
            std::os::unix::fs::symlink(link_entry.target, &link_path).unwrap();
        }
        #[cfg(windows)]
        ddup_bak::Entry::Symlink(link_entry) => {
            let link_path = std::path::Path::new(directory).join(&link_entry.name);
            let target_path = std::path::Path::new(directory).join(&link_entry.target);

            if link_entry.target_dir {
                std::os::windows::fs::symlink_dir(target_path, &link_path).unwrap();
            } else {
                std::os::windows::fs::symlink_file(target_path, &link_path).unwrap();
            }
        }
    }
}

fn main() {
    let mode = std::env::args().nth(1).expect("No mode given");

    match mode.as_str() {
        "encode" => {
            let directory = std::env::args().nth(2).expect("No directory given");
            let output_file = std::env::args().nth(3).expect("No output file given");
            let output = std::fs::File::create(output_file).expect("Failed to create output file");
            let mut output = ddup_bak::Archive::new(output);

            output
                .add_directory(
                    &directory,
                    Some(|file| {
                        println!("Adding file: {}", file.display());
                    }),
                )
                .unwrap();
        }
        "decode" => {
            let input_file = std::env::args().nth(2).expect("No file given");
            let input = ddup_bak::Archive::open(&input_file).expect("Failed to open input file");
            let directory = std::env::args().nth(3);

            println!("{:#?}", input);

            if let Some(dir) = &directory {
                std::fs::remove_dir_all(dir).unwrap_or_default();
                std::fs::create_dir_all(dir).unwrap();
            }

            println!("File count: {}", input.entries().len());
            if let Some(dir) = &directory {
                for entry in input.into_entries() {
                    restore_entry(entry, dir);
                }
            }
        }
        _ => {
            println!("Invalid mode. Use 'encode' or 'decode'.");
        }
    }
}
