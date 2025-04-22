use crate::commands::open_repository;
use chrono::{DateTime, Local};
use clap::ArgMatches;
use colored::Colorize;
use ddup_bak::archive::Entry;
use std::{fs::Permissions, path::Path, time::SystemTime};

fn render_unix_permissions(mode: &Permissions) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode_bits = mode.mode();
        let mut result = String::new();

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
            "r-x".to_string()
        }
    }
}

fn format_time(time: SystemTime) -> String {
    let datetime: DateTime<Local> = time.into();
    datetime.format("%b %e %H:%M").to_string()
}

fn get_file_type_char(entry: &Entry) -> &'static str {
    match entry {
        Entry::File(_) => "-",
        Entry::Directory(_) => "d",
        Entry::Symlink(_) => "l",
    }
}

#[cfg(unix)]
fn get_username(uid: u32) -> String {
    use libc::{getpwuid, getpwuid_r, passwd, uid_t};
    use std::ffi::CStr;
    use std::mem::MaybeUninit;
    use std::ptr;

    let mut buf = [0; 2048]; // Buffer for passwd struct
    let mut result = MaybeUninit::<passwd>::uninit();
    let mut passwd_ptr = ptr::null_mut();

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

    format!("{}", uid)
}

#[cfg(unix)]
fn get_groupname(gid: u32) -> String {
    use libc::{getgrgid, getgrgid_r, gid_t, group};
    use std::ffi::CStr;
    use std::mem::MaybeUninit;
    use std::ptr;

    let mut buf = [0; 2048];
    let mut result = MaybeUninit::<group>::uninit();
    let mut group_ptr = ptr::null_mut();

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

    format!("{}", gid)
}

#[cfg(not(unix))]
fn get_username(uid: u32) -> String {
    format!("{}", uid)
}

#[cfg(not(unix))]
fn get_groupname(gid: u32) -> String {
    format!("{}", gid)
}

#[cfg(unix)]
fn is_executable(mode: &Permissions) -> bool {
    use std::os::unix::fs::PermissionsExt;
    mode.mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_mode: &Permissions) -> bool {
    !_mode.readonly()
}

fn calculate_column_widths(entries: &[Entry]) -> (usize, usize) {
    let mut max_user_len = 0;
    let mut max_group_len = 0;

    for entry in entries {
        let (uid, gid) = match entry {
            Entry::File(file) => (file.owner.0, file.owner.1),
            Entry::Directory(dir) => (dir.owner.0, dir.owner.1),
            Entry::Symlink(link) => (link.owner.0, link.owner.1),
        };

        let username = get_username(uid);
        let groupname = get_groupname(gid);

        max_user_len = max_user_len.max(username.len());
        max_group_len = max_group_len.max(groupname.len());
    }

    (max_user_len, max_group_len)
}

fn render_entry(entry: &Entry, user_width: usize, group_width: usize) -> String {
    let file_type = get_file_type_char(entry);

    match entry {
        Entry::File(file) => {
            let perms = render_unix_permissions(&file.mode);
            let username = get_username(file.owner.0);
            let groupname = get_groupname(file.owner.1);
            let time_str = format_time(file.mtime);
            let name = if is_executable(&file.mode) {
                file.name.green().bold()
            } else {
                file.name.normal()
            };

            format!(
                "{}{} {:>4} {:<width_user$} {:<width_group$}     {} {}",
                file_type,
                perms,
                1,
                username,
                groupname,
                time_str,
                name,
                width_user = user_width,
                width_group = group_width
            )
        }
        Entry::Directory(dir) => {
            let perms = render_unix_permissions(&dir.mode);
            let username = get_username(dir.owner.0);
            let groupname = get_groupname(dir.owner.1);
            let time_str = format_time(dir.mtime);
            let name = dir.name.blue().bold();
            let link_count = (dir.entries.len() + 2).to_string();

            format!(
                "{}{} {:>4} {:<width_user$} {:<width_group$}     {} {}",
                file_type,
                perms,
                link_count,
                username,
                groupname,
                time_str,
                name,
                width_user = user_width,
                width_group = group_width
            )
        }
        Entry::Symlink(link) => {
            let perms = render_unix_permissions(&link.mode);
            let username = get_username(link.owner.0);
            let groupname = get_groupname(link.owner.1);
            let time_str = format_time(link.mtime);
            let name = link.name.cyan();
            let target = format!("-> {}", link.target).cyan().bold();

            format!(
                "{}{} {:>4} {:<width_user$} {:<width_group$}     {} {} {}",
                file_type,
                perms,
                1,
                username,
                groupname,
                time_str,
                name,
                target,
                width_user = user_width,
                width_group = group_width
            )
        }
    }
}

fn render_entries(entries: &[Entry]) -> String {
    let (user_width, group_width) = calculate_column_widths(entries);

    entries
        .iter()
        .map(|entry| render_entry(entry, user_width, group_width))
        .collect::<Vec<_>>()
        .join("\n")
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
            Entry::File(file) => {
                entries.push(Entry::File(file.clone()));
            }
            Entry::Directory(dir) => {
                for entry in dir.entries.iter() {
                    entries.push(entry.clone());
                }
            }
            Entry::Symlink(link) => {
                entries.push(Entry::Symlink(link.clone()));
            }
        }

        let rendered_entries = render_entries(&entries);
        println!("{}", rendered_entries);
    } else if path.components().all(|c| c.as_os_str() == ".") {
        let rendered_entries = render_entries(archive.entries());
        println!("{}", rendered_entries);
    } else {
        println!(
            "{} {} {}",
            "path".red(),
            path.display().to_string().cyan(),
            "does not exist!".red()
        );

        return 1;
    }

    0
}
