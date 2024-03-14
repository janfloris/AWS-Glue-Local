import pyarrow.parquet as pq
import s3fs

# Create a filesystem handle for S3
s3 = s3fs.S3FileSystem()

# Path to the Parquet file in S3
parquet_file_path = 's3://media-data-processing-dev/google/cron_name/run-1702924011312-part-block-0-r-00000-snappy.parquet'

# Open Parquet file
parquet_file = pq.ParquetDataset(parquet_file_path, filesystem=s3)

# Read Parquet file
table = parquet_file.read().to_pandas()
print(table.head())  # Display the first few rows of the table
