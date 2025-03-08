// config.rs
use dotenv::dotenv;
use std::env;

pub struct Config {
    pub input_prefix: String,
    pub output_prefix: String,
    pub bucket_name: String,
}

pub fn load_config() -> anyhow::Result<Config> {
    dotenv().ok();
    Ok(Config {
        input_prefix: env::var("INPUT_PREFIX")?,
        output_prefix: env::var("OUTPUT_PREFIX")?,
        bucket_name: env::var("BUCKET_NAME")?,
    })
}
