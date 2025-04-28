use crate::commands::open_repository;
use chrono::{DateTime, Local};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::entries::Entry;
use std::{collections::HashMap, fs::Permissions, io::Write, path::Path, time::SystemTime};

#[inline]
fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{}", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes < 1024 * 1024 * 1024 * 1024 {
        format!("{:.1}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else {
        format!("{:.1}T", bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 1024.0))
    }
}

fn render_unix_permissions(mode: &Permissions) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode_bits = mode.mode();
        let mut result = String::with_capacity(10);

        result.push(if mode_bits & 0o400 != 0 { 'r' } else { '-' });
        result.push(if mode_bits & 0o200 != 0 { 'w' } else { '-' });
        result.push(if mode_bits & 0o100 != 0 { 'x' } else { '-' });

        result.push(if mode_bits & 0o040 != 0 { 'r' } else { '-' });
        result.push(if mode_bits & 0o020 != 0 { 'w' } else { '-' });
        result.push(if mode_bits & 0o010 != 0 { 'x' } else { '-' });

        result.push(if mode_bits & 0o004 != 0 { 'r' } else { '-' });
        result.push(if mode_bits & 0o002 != 0 { 'w' } else { '-' });
        result.push(if mode_bits & 0o001 != 0 { 'x' } else { '-' });

        result
    }
    #[cfg(not(unix))]
    {
        if mode.readonly() {
            "r--".to_string()
        } else {
            "rw-".to_string()
        }
    }
}

fn format_time(time: SystemTime) -> String {
    let datetime: DateTime<Local> = time.into();

    datetime.format("%b %e %H:%M").to_string()
}

#[cfg(unix)]
fn get_username(uid: u32) -> String {
    use libc::{getpwuid, getpwuid_r, passwd, uid_t};
    use std::{ffi::CStr, mem::MaybeUninit};

    let mut buf = [0; 2048];
    let mut result = MaybeUninit::<passwd>::uninit();
    let mut passwd_ptr = std::ptr::null_mut();

    unsafe {
        let ret = getpwuid_r(
            uid as uid_t,
            result.as_mut_ptr(),
            buf.as_mut_ptr(),
            buf.len(),
            &mut passwd_ptr,
        );

        if ret == 0 && !passwd_ptr.is_null() {
            let passwd = result.assume_init();
            let username = CStr::from_ptr(passwd.pw_name)
                .to_string_lossy()
                .into_owned();

            return username;
        }
    }

    unsafe {
        let passwd = getpwuid(uid as uid_t);
        if !passwd.is_null() {
            let username = CStr::from_ptr((*passwd).pw_name)
                .to_string_lossy()
                .into_owned();

            return username;
        }
    }

    uid.to_string()
}

#[cfg(unix)]
fn get_groupname(gid: u32) -> String {
    use libc::{getgrgid, getgrgid_r, gid_t, group};
    use std::{ffi::CStr, mem::MaybeUninit};

    let mut buf = [0; 2048];
    let mut result = MaybeUninit::<group>::uninit();
    let mut group_ptr = std::ptr::null_mut();

    unsafe {
        let ret = getgrgid_r(
            gid as gid_t,
            result.as_mut_ptr(),
            buf.as_mut_ptr(),
            buf.len(),
            &mut group_ptr,
        );

        if ret == 0 && !group_ptr.is_null() {
            let group = result.assume_init();
            let groupname = CStr::from_ptr(group.gr_name).to_string_lossy().into_owned();

            return groupname;
        }
    }

    unsafe {
        let group = getgrgid(gid as gid_t);
        if !group.is_null() {
            let groupname = CStr::from_ptr((*group).gr_name)
                .to_string_lossy()
                .into_owned();

            return groupname;
        }
    }

    gid.to_string()
}

#[cfg(not(unix))]
fn get_username(uid: u32) -> String {
    uid.to_string()
}

#[cfg(not(unix))]
fn get_groupname(gid: u32) -> String {
    gid.to_string()
}

#[cfg(unix)]
fn is_executable(mode: &Permissions) -> bool {
    use std::os::unix::fs::PermissionsExt;

    mode.mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_mode: &Permissions) -> bool {
    false
}

fn calculate_column_widths(
    entries: &[&Entry],
    users: &mut HashMap<u32, String>,
    groups: &mut HashMap<u32, String>,
) -> (usize, usize, usize, usize) {
    let mut max_link_count_len = 0;
    let mut max_user_len = 0;
    let mut max_group_len = 0;
    let mut max_size_len = 0;

    for entry in entries {
        let link_count = match entry {
            Entry::Directory(dir) => dir.entries.len(),
            _ => 1,
        };

        let size = match entry {
            Entry::File(file) => format_bytes(file.size_real).len(),
            Entry::Symlink(link) => format_bytes(link.target.len() as u64).len(),
            _ => 1,
        };

        let (uid, gid) = entry.owner();

        let username = users.entry(uid).or_insert_with(|| get_username(uid));
        let groupname = groups.entry(gid).or_insert_with(|| get_groupname(gid));

        max_link_count_len = max_link_count_len.max(link_count.to_string().len());
        max_user_len = max_user_len.max(username.len());
        max_group_len = max_group_len.max(groupname.len());
        max_size_len = max_size_len.max(size);
    }

    (
        max_link_count_len,
        max_user_len,
        max_group_len,
        max_size_len,
    )
}

fn render_entry(
    entry: &Entry,
    link_count_width: usize,
    user_width: usize,
    group_width: usize,
    size_width: usize,
    users: &HashMap<u32, String>,
    groups: &HashMap<u32, String>,
) -> String {
    let file_type = match entry {
        Entry::File(_) => '-',
        Entry::Directory(_) => 'd',
        Entry::Symlink(_) => 'l',
    };

    let (uid, gid) = entry.owner();
    let username = users.get(&uid).unwrap();
    let groupname = groups.get(&gid).unwrap();

    let perms = render_unix_permissions(entry.mode());
    let time_str = format_time(entry.mtime());

    match entry {
        Entry::File(file) => {
            let name = if is_executable(&file.mode) {
                file.name.green().bold()
            } else {
                file.name.normal()
            };

            format!(
                "{}{} {:>width_link_count$} {:<width_user$} {:<width_group$} {:>width_size$} {} {}",
                file_type,
                perms,
                1,
                username,
                groupname,
                format_bytes(file.size_real),
                time_str,
                name,
                width_link_count = link_count_width,
                width_user = user_width,
                width_group = group_width,
                width_size = size_width
            )
        }
        Entry::Directory(dir) => {
            let name = dir.name.blue().bold();
            let link_count = dir.entries.len();

            format!(
                "{}{} {:>width_link_count$} {:<width_user$} {:<width_group$} {:>width_size$} {} {}",
                file_type,
                perms,
                link_count,
                username,
                groupname,
                0,
                time_str,
                name,
                width_link_count = link_count_width,
                width_user = user_width,
                width_group = group_width,
                width_size = size_width
            )
        }
        Entry::Symlink(link) => {
            let name = link.name.bright_cyan().bold();
            let target = format!(
                "-> {}",
                if is_executable(&link.mode) {
                    link.target.blue().on_green()
                } else {
                    link.target.blue()
                }
            );

            format!(
                "{}{} {:>width_link_count$} {:<width_user$} {:<width_group$} {:>width_size$} {} {} {}",
                file_type,
                perms,
                1,
                username,
                groupname,
                format_bytes(link.target.len() as u64),
                time_str,
                name,
                target,
                width_link_count = link_count_width,
                width_user = user_width,
                width_group = group_width,
                width_size = size_width
            )
        }
    }
}

fn render_entries(mut entries: Vec<&Entry>) {
    let mut users: HashMap<u32, String> = HashMap::new();
    let mut groups: HashMap<u32, String> = HashMap::new();

    let (link_count_width, user_width, group_width, size_width) =
        calculate_column_widths(&entries, &mut users, &mut groups);

    entries.sort_by(|a, b| {
        let a_name = a.name();
        let b_name = b.name();

        if a_name == b_name {
            return a.mtime().cmp(&b.mtime());
        }

        a_name.cmp(b_name)
    });

    let mut lock = std::io::stdout().lock();

    for entry in entries {
        let rendered_entry = render_entry(
            entry,
            link_count_width,
            user_width,
            group_width,
            size_width,
            &users,
            &groups,
        );

        writeln!(lock, "{}", rendered_entry).unwrap();
    }
}

pub fn ls(name: &str, matches: &ArgMatches) -> i32 {
    let repository = open_repository();
    let path = matches.get_one::<String>("path");

    if !repository
        .list_archives()
        .unwrap()
        .into_iter()
        .any(|name| name == *name)
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

    let path = Path::new(path.map_or(".", |s| s.as_str()));
    if let Some(entry) = archive.find_archive_entry(path).unwrap() {
        let mut entries = Vec::new();

        match entry {
            Entry::Directory(dir) => {
                for entry in dir.entries.iter() {
                    entries.push(entry);
                }
            }
            _ => {
                entries.push(entry);
            }
        }

        println!(
            "total {} entries, {}",
            entries.len(),
            format_bytes(
                entries
                    .iter()
                    .map(|e| match e {
                        Entry::File(f) => f.size_real,
                        Entry::Symlink(s) => s.target.len() as u64,
                        _ => 0,
                    })
                    .sum()
            )
        );

        render_entries(entries);
    } else if path.components().all(|c| c.as_os_str() == ".") {
        println!(
            "total {} entries, {}",
            archive.entries().len(),
            format_bytes(
                archive
                    .entries()
                    .iter()
                    .map(|e| match e {
                        Entry::File(f) => f.size_real,
                        Entry::Symlink(s) => s.target.len() as u64,
                        _ => 0,
                    })
                    .sum()
            )
        );

        render_entries(archive.entries().iter().collect::<Vec<_>>());
    } else {
        println!(
            "{} {}",
            path.display().to_string().cyan(),
            "does not exist!".red()
        );

        return 1;
    }

    0
}
