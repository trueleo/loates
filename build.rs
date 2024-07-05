use std::{error::Error, io};

fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "web")]
    compile_web_ui().unwrap();
    Ok(())
}

pub fn compile_web_ui() -> io::Result<()> {
    use std::env;
    use std::fs::create_dir_all;
    use std::path::PathBuf;

    use cargo_toml::Manifest;
    use static_files::resource_dir;

    const CARGO_MANIFEST_DIR: &str = "CARGO_MANIFEST_DIR";
    const OUT_DIR: &str = "OUT_DIR";

    let cargo_manifest_dir = PathBuf::from(env::var(CARGO_MANIFEST_DIR).unwrap());
    let cargo_toml = cargo_manifest_dir.join("Cargo.toml");
    let out_dir = PathBuf::from(env::var(OUT_DIR).unwrap());
    let ui_path = out_dir.join("ui");

    let manifest = Manifest::from_path(cargo_toml).unwrap();

    let manifest = manifest
        .package
        .expect("package not specified in Cargo.toml")
        .metadata
        .expect("no metadata specified in Cargo.toml");

    let metadata = manifest
        .get("ui")
        .expect("UI Metadata not defined correctly");

    let url = metadata["assets-url"].as_str().unwrap();

    let build_zip = ureq::get(url)
        .call()
        .map(|data| {
            let mut buf: Vec<u8> = Vec::new();
            data.into_reader().read_to_end(&mut buf).unwrap();
            buf
        })
        .expect("Failed to get resource from {url}");

    create_dir_all(&ui_path)?;
    let mut zip = zip::read::ZipArchive::new(io::Cursor::new(&build_zip))?;
    zip.extract(&ui_path)?;
    resource_dir(ui_path.join("dist")).build()?;

    Ok(())
}
