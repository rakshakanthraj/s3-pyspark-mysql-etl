import os
from resources.dev import config
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import decrypt

# Keys inside config.py are ALREADY encrypted, so decrypt them directly
s3_client_provider = S3ClientProvider(
    decrypt(config.aws_access_key), 
    decrypt(config.aws_secret_key)
)
s3_client = s3_client_provider.get_client()

local_file_path = "/home/raksh/spark_data"

def upload_to_s3(s3_directory, s3_bucket, local_path):
    s3_prefix = f"{s3_directory.rstrip('/')}/"
    try:
        for root, dirs, files in os.walk(local_path):
            for file in files:
                if file.endswith(".csv"):
                    full_file_path = os.path.join(root, file)
                    s3_key = f"{s3_prefix}{file}"

                    print(f"Uploading {file} to s3://{s3_bucket}/{s3_key}")
                    s3_client.upload_file(full_file_path, s3_bucket, s3_key)
                    print(f"Successfully uploaded {file}")
    except Exception as e:
        print(f"Error during S3 upload: {e}")
        raise e

if __name__ == "__main__":
    s3_directory = config.s3_source_directory  # "sales_data/"
    s3_bucket = config.bucket_name              # "raksha-de-project-2026"
    
    upload_to_s3(s3_directory, s3_bucket, local_file_path)

