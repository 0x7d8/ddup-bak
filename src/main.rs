use std::path::Path;

fn main() {
    let mode = std::env::args().nth(1).expect("No mode given");

    let mut repository = if std::path::Path::new(".ddup-bak").is_dir() {
        ddup_bak::repository::Repository::open(Path::new(".")).unwrap()
    } else {
        ddup_bak::repository::Repository::new(Path::new("."), 1024 * 1024, vec![])
    };

    match mode.as_str() {
        "encode" => {
            let archive = std::env::args().nth(2).expect("No archive given");

            repository
                .create_archive(
                    &archive,
                    Some(|file| {
                        println!("Chunked file: {}", file.display());
                    }),
                    Some(|file| {
                        println!("Archived file: {}", file.display());
                    }),
                    8,
                )
                .unwrap();
        }
        "decode" => {
            let archive = std::env::args().nth(2).expect("No archive given");
            let output_dir = std::env::args().nth(3).expect("No output directory given");

            let restored_output_dir = repository
                .restore_archive(
                    &archive,
                    Some(|file| {
                        println!("Restored file: {}", file.display());
                    }),
                    8,
                )
                .unwrap();

            for entry in std::fs::read_dir(&output_dir).unwrap().flatten() {
                let path = entry.path();
                if path.file_name().unwrap() == ".ddup-bak" {
                    continue;
                }

                if path.is_dir() {
                    std::fs::remove_dir_all(&path).unwrap();
                } else {
                    std::fs::remove_file(&path).unwrap();
                }
            }

            for entry in std::fs::read_dir(restored_output_dir).unwrap().flatten() {
                let path = entry.path();

                let new_path = Path::new(&output_dir).join(path.file_name().unwrap());
                std::fs::rename(path, new_path).unwrap();
            }
        }
        "nuke" => {
            let archive = std::env::args().nth(2).expect("No archive given");

            repository
                .delete_archive(
                    &archive,
                    Some(|chunk_id, deleted| {
                        if deleted {
                            println!("Deleted chunk: {}", chunk_id);
                        } else {
                            println!("Dereferenced chunk: {}", chunk_id);
                        }
                    }),
                )
                .unwrap();
        }
        _ => {
            println!("Invalid mode. Use 'encode' or 'decode' or 'nuke'.");
        }
    }
}
