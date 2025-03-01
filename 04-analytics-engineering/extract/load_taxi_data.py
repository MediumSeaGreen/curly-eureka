import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time
from glob import glob
from constants import (
    BUCKET_NAME,
    CREDENTIALS_FILE,
    BASE_URL_YELLOW,
    BASE_URL_GREEN,
    BASE_URL_FHV,
    MONTHS,
    CHUNK_SIZE,
    DOWNLOAD_DIR,
    FHV_CASTING_DICT,
    YELLOW_CASTING_DICT,
    GREEN_CASTING_DICT,
)
from fix_parquet import fix_files


def download_file(year, month, url):
    url = f"{url}_{year}-{month}.parquet"
    file_path = os.path.join(
        DOWNLOAD_DIR, f"{url.split('/')[-1].rsplit('_', 1)[0]}_{year}-{month}.parquet"
    )

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name, bucket, client):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, bucket, client, max_retries=3):
    blob_name = (
        f"{os.path.basename(file_path).split('_')[0]}/{os.path.basename(file_path)}"
    )
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name, bucket, client):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


def get_data(url, year, casting_dict):
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    bucket = client.bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(
            executor.map(
                download_file, [year] * len(MONTHS), MONTHS, [url] * len(MONTHS)
            )
        )

    fix_files(casting_dict)

    directory = os.getcwd()
    file_paths = glob(os.path.join(directory, "*.parquet"))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(
            upload_to_gcs,
            filter(None, file_paths),
            [bucket] * len(file_paths),
            [client] * len(file_paths),
        )  # Remove None values

    """ for file_path in glob(os.path.join(DOWNLOAD_DIR, "*.parquet")):
        os.remove(file_path) """

    print("All files processed and verified.")


if __name__ == "__main__":
    # get_data(BASE_URL_YELLOW, 2019, YELLOW_CASTING_DICT)
    # get_data(BASE_URL_YELLOW, 2020, YELLOW_CASTING_DICT)
    get_data(BASE_URL_GREEN, 2019, GREEN_CASTING_DICT)
    get_data(BASE_URL_GREEN, 2020, GREEN_CASTING_DICT)
    # get_data(BASE_URL_FHV, 2019, FHV_CASTING_DICT)
