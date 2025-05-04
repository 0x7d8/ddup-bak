use clap::{Arg, Command};

mod commands;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn cli() -> Command {
    Command::new("ddup-bak")
        .about("A description")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .version(VERSION)
        .subcommand(
            Command::new("init")
                .about("Initializes a new ddup-bak repository")
                .arg(
                    Arg::new("directory")
                        .help("The directory to initialize the repository in")
                        .num_args(1)
                        .default_value(".")
                        .required(false),
                )
                .arg(
                    Arg::new("chunk_size")
                        .help("The chunk size to use for the repository (bytes)")
                        .short('c')
                        .long("chunk-size")
                        .num_args(1)
                        .default_value("1048576")
                        .value_parser(clap::value_parser!(usize))
                        .required(false),
                )
                .arg(
                    Arg::new("max_chunk_count")
                        .help("The max chunk count to allow for individual files, if exceeded, chunk size will be halfed until count is below this value, 0 means no limit")
                        .short('m')
                        .long("max-chunk-count")
                        .num_args(1)
                        .default_value("0")
                        .value_parser(clap::value_parser!(usize))
                        .required(false),
                )
                .arg_required_else_help(false),
        )
        .subcommand(
            Command::new("backup")
                .about("Manages backups")
                .subcommand(
                    Command::new("create")
                        .about("Creates a new backup")
                        .arg(
                            Arg::new("name")
                                .help("The name of the backup to create")
                                .num_args(1)
                                .required(true),
                        )
                        .arg(
                            Arg::new("directory")
                                .help("The directory to backup")
                                .num_args(1)
                                .required(false),
                        )
                        .arg(
                            Arg::new("threads")
                                .help("The number of threads to use for the backup")
                                .short('t')
                                .long("threads")
                                .num_args(1)
                                .default_value("16")
                                .value_parser(clap::value_parser!(usize))
                                .required(false),
                        )
                        .arg(
                            Arg::new("compression")
                                .help("The compression format to use")
                                .short('c')
                                .long("compression")
                                .num_args(1)
                                .default_value("deflate")
                                .required(false),
                        )
                        .arg_required_else_help(true),
                )
                .subcommand(
                    Command::new("delete")
                        .about("Deletes a backup")
                        .arg(
                            Arg::new("name")
                                .help("The name of the backup to delete")
                                .num_args(1)
                                .required(true),
                        )
                        .arg_required_else_help(false),
                )
                .subcommand(
                    Command::new("restore")
                        .about("Restores a backup")
                        .arg(
                            Arg::new("name")
                                .help("The name of the backup to restore")
                                .num_args(1)
                                .required(true),
                        )
                        .arg(
                            Arg::new("destination")
                                .help("The destination to restore the backup to")
                                .num_args(1)
                                .required(false),
                        )
                        .arg(
                            Arg::new("threads")
                                .help("The number of threads to use for the restore")
                                .short('t')
                                .long("threads")
                                .num_args(1)
                                .default_value("16")
                                .value_parser(clap::value_parser!(usize))
                                .required(false),
                        )
                        .arg_required_else_help(false),
                )
                .subcommand(
                    Command::new("convert")
                        .about("Converts a backup")
                        .arg(
                            Arg::new("name")
                                .help("The name of the backup to convert")
                                .num_args(1)
                                .required(true),
                        )
                        .arg(
                            Arg::new("output")
                                .help("The output file to convert to")
                                .num_args(1)
                                .required(false),
                        )
                        .arg(
                            Arg::new("format")
                                .help("The format to convert to")
                                .short('f')
                                .long("format")
                                .num_args(1)
                                .required(true)
                                .value_parser(["tar", "tar.gz", "zip"])
                                .default_value("tar")
                                .required(false),
                        )
                        .arg_required_else_help(false),
                )
                .subcommand(
                    Command::new("list")
                        .about("Lists all backups")
                        .arg_required_else_help(false),
                )
                .subcommand(
                    Command::new("fs")
                        .about("Manages the backup file system")
                        .arg(
                            Arg::new("name")
                                .help("The name of the backup to list files for")
                                .num_args(1)
                                .required(true),
                        )
                        .subcommand(
                            Command::new("ls")
                                .about("Lists files in the backup file system")
                                .arg(
                                    Arg::new("path")
                                        .help("The path to list files for")
                                        .num_args(1)
                                        .required(false),
                                )
                                .arg_required_else_help(false),
                        )
                        .subcommand(
                            Command::new("cat")
                                .about("Displays the content of a file in the backup file system")
                                .arg(
                                    Arg::new("path")
                                        .help("The path to the file to display")
                                        .num_args(1)
                                        .required(true),
                                )
                                .arg_required_else_help(false),
                        ),
                )
                .arg_required_else_help(true)
                .subcommand_required(true),
        )
}

fn main() {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("init", sub_matches)) => std::process::exit(commands::init::init(sub_matches)),
        Some(("backup", sub_matches)) => match sub_matches.subcommand() {
            Some(("create", sub_matches)) => {
                std::process::exit(commands::backup::create::create(sub_matches))
            }
            Some(("delete", sub_matches)) => {
                std::process::exit(commands::backup::delete::delete(sub_matches))
            }
            Some(("restore", sub_matches)) => {
                std::process::exit(commands::backup::restore::restore(sub_matches))
            }
            Some(("convert", sub_matches)) => {
                std::process::exit(commands::backup::convert::convert(sub_matches))
            }
            Some(("list", sub_matches)) => {
                std::process::exit(commands::backup::list::list(sub_matches))
            }
            Some(("fs", sub_matches)) => match sub_matches.subcommand() {
                Some(("ls", sub_sub_matches)) => std::process::exit(commands::backup::fs::ls::ls(
                    sub_matches.get_one::<String>("name").unwrap(),
                    sub_sub_matches,
                )),
                Some(("cat", sub_sub_matches)) => {
                    std::process::exit(commands::backup::fs::cat::cat(
                        sub_matches.get_one::<String>("name").unwrap(),
                        sub_sub_matches,
                    ))
                }
                _ => cli().print_help().unwrap(),
            },
            _ => unreachable!(),
        },
        _ => cli().print_help().unwrap(),
    }
}
