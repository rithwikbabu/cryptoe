use polars::prelude::*;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ClientOptions, ObjectStore, path::Path};
use futures::TryStreamExt;
use std::io::{BufReader, Cursor, Read};
use crate::config::Config;
use std::collections::HashSet;
use std::sync::Arc;
use chrono::NaiveDate;

pub async fn process_files(cfg: &Config) -> anyhow::Result<()> {
    // 1. set up the s3 client for reading input files
    let client = ClientOptions::default().with_timeout_disabled();

    let s3 = AmazonS3Builder::from_env()
        .with_client_options(client)
        .with_url("s3://flatfiles/")
        .with_virtual_hosted_style_request(false)
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;

    // 2. set up the gcp client for listing the output files (to know which dates are processed)
    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(&cfg.bucket_name)
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;

    // 3. List existing output files in GCS (using your output_prefix)
    //    and extract the dates that have already been processed.
    let output_prefix = Path::from(cfg.output_prefix.clone());
    let mut existing_dates = HashSet::new();
    let mut gcs_output_stream = gcs.list(Some(&output_prefix));
    while let Some(obj_meta) = gcs_output_stream.try_next().await? {
        let location = obj_meta.location.as_ref();
        // Expecting output files to be named like "YYYY-MM-DD.parquset"
        if let Some(file_name) = location.rsplit('/').next() {
            if let Some(date_str) = file_name.strip_suffix(".parquet") {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    existing_dates.insert(date);
                }
            }
        }
    }

    // 4. list files from s3 using the provided input prefix
    let prefix = Path::from(cfg.input_prefix.clone());
    let mut stream = s3.list(Some(&prefix));

    // 5. Define the local output directory for saving parquet files.
    //    You can adjust this path or expose it via your Config struct.
    let local_output_dir = "local_parquet_files";
    tokio::fs::create_dir_all(local_output_dir).await?;

    while let Some(obj_meta) = stream.try_next().await? {
        let location = obj_meta.location.as_ref();

        // Extract the date from the file name (expecting "YYYY-MM-DD.csv.gz")
        let file_name = location.rsplit('/').next().unwrap_or("");
        let mut skip_file = false;
        if let Some(date_str) = file_name.strip_suffix(".csv.gz") {
            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                if existing_dates.contains(&date) {
                    println!("Skipping file {} as date {} is already processed", location, date);
                    skip_file = true;
                }
            }
        }
        if skip_file {
            continue;
        }

        println!("Processing file: {}", location);

        // 6. download the object from s3 into memory
        let get_result = s3.get(&obj_meta.location).await?;
        let bytes = get_result.bytes().await?;

        // 7. decompress the csv gzip file
        let gz_decoder = flate2::read::GzDecoder::new(&bytes[..]);
        let mut buf_reader = BufReader::new(gz_decoder);

        // 8. read the entire decompressed csv data into a vec<u8>
        let mut csv_data = Vec::new();
        buf_reader.read_to_end(&mut csv_data)?;

        // 9. create a cursor over the csv data
        let cursor = Cursor::new(csv_data);

        let schema = Schema::from_iter([
            Field::new("ticker".into(), DataType::String),
            Field::new("conditions".into(), DataType::Int32),
            Field::new("exchange".into(), DataType::Int32),
            Field::new("id".into(), DataType::Int64),
            Field::new("participant_timestamp".into(), DataType::Int64),
            Field::new("price".into(), DataType::Float64),
            Field::new("size".into(), DataType::Float64),
        ]);
        let schema_arc = Some(Arc::new(schema));

        // 10. read csv into a polars dataframe using the cursor
        let mut df = CsvReadOptions::default()
            .with_has_header(true)
            .with_schema(schema_arc)
            .into_reader_with_file_handle(cursor)
            .finish()
            .unwrap();

        // 11. convert the dataframe to zipped parquet format into an in-memory buffer
        let mut parquet_buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut parquet_buffer)
            .with_compression(ParquetCompression::Zstd(Some(
                ZstdLevel::try_new(3).unwrap()
            )))
            .finish(&mut df)?;

        // 12. define the output file name (convert ".csv.gz" to ".parquet")
        let file_name = location.rsplit('/').next().unwrap_or("output");
        let output_file_name = file_name.replace(".csv.gz", ".parquet");
        let local_output_path = format!("{}/{}", local_output_dir, output_file_name);

        // 13. save the parquet file locally
        tokio::fs::write(&local_output_path, parquet_buffer).await?;
        println!("Saved parquet file locally at: {}", local_output_path);
    }

    Ok(())
}
