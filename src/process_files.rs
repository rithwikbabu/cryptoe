use polars::prelude::*;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path};
use futures::TryStreamExt;
use flate2::read::GzDecoder;
use std::io::{BufReader, Cursor, Read};
use crate::config::Config;


pub async fn process_files(cfg: &Config) -> anyhow::Result<()> {
    // 1. set up the s3 client for reading input files
    let s3 = AmazonS3Builder::from_env()
        .with_url("s3://flatfiles/")
        .with_virtual_hosted_style_request(false)
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;

    // 2. set up the gcp client for writing the output files
    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(&cfg.bucket_name)
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;

    // 3. list files from s3 using the provided input prefix
    let prefix = Path::from(cfg.input_prefix.clone());
    let mut stream = s3.list(Some(&prefix));

    while let Some(obj_meta) = stream.try_next().await? {
        println!("Processing file: {}", obj_meta.location);

        // 4. download the object from s3 into memory
        let get_result = s3.get(&obj_meta.location).await?;
        let bytes = get_result.bytes().await?;

        // 5. decompress the csv gzip file
        let gz_decoder = GzDecoder::new(&bytes[..]);
        let mut buf_reader = BufReader::new(gz_decoder);

        // 6 read the entire decompressed csv data into a vec<u8>
        let mut csv_data = Vec::new();
        buf_reader.read_to_end(&mut csv_data)?;

        // 7. create a cursor over the csv data
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

        // 8. read csv into a polars dataframe using the cursor
        let mut df = CsvReadOptions::default()
            .with_has_header(true)
            .with_schema(schema_arc)
            .into_reader_with_file_handle(cursor)
            .finish()
            .unwrap();


        // 9. convert the dataframe to zipped parquet format into an in-memory buffer
        let mut parquet_buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut parquet_buffer)
            .with_compression(ParquetCompression::Zstd(
                Some(ZstdLevel::try_new(3).unwrap())
            ))
            .finish(&mut df)?;

        // 10. define the output path on gcp
        let file_name = obj_meta.location
            .as_ref()
            .rsplit('/')
            .next()
            .unwrap_or("output");
        let output_file_name = file_name.replace(".csv.gz", ".parquet");
        let output_path = Path::from(format!("{}/{}", cfg.output_prefix, output_file_name));

        // 10. upload the parquet file to your gcp bucket
        gcs.put(&output_path, parquet_buffer.into()).await?;

        println!("Uploaded parquet file to: {}", output_path);
    }

    Ok(())
}
