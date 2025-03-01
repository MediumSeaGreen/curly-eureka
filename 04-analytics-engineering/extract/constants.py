import os

BUCKET_NAME = os.getenv("BUCKET_NAME")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS")
BASE_URL_YELLOW = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"
BASE_URL_GREEN = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata"
BASE_URL_FHV = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
CHUNK_SIZE = 8 * 1024 * 1024
DOWNLOAD_DIR = "."
FHV_CASTING_DICT = {
    "dispatching_base_num": "string",
    "pickup_datetime": "datetime64[s]",
    "dropOff_datetime": "datetime64[s]",
    "PUlocationID": "Int64",
    "DOlocationID": "Int64",
    "SR_Flag": "Int64",
    "Affiliated_base_number": "string",
}
YELLOW_CASTING_DICT = {
    "VendorID": "Int64",
    "tpep_pickup_datetime": "datetime64[s]",
    "tpep_dropoff_datetime": "datetime64[s]",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "improvement_surcharge": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "airport_fee": "float64",
}
GREEN_CASTING_DICT = {
    "VendorID": "Int64",
    "lpep_pickup_datetime": "datetime64[ms]",
    "lpep_dropoff_datetime": "datetime64[ms]",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "improvement_surcharge": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "total_amount": "float64",
    "trip_type": "Int64",
    "ehail_fee": "float64",
    "congestion_surcharge": "float64",
}
