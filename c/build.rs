use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let config = cbindgen::Config::from_file("cbindgen.toml").unwrap_or_default();

    let mut header_path = PathBuf::from(&crate_dir);
    header_path.push("include");
    std::fs::create_dir_all(&header_path).unwrap();
    header_path.push("libddupbak.h");

    let bindings = cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .with_parse_deps(false)
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings");

    bindings.write_to_file(header_path);
}
