// main.rs
use crate::config::Config;
mod config;
mod process_files;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg: Config = config::load_config()?;

    println!("Reading data from S3...");
    process_files::process_files(&cfg).await?;


    Ok(())
}
