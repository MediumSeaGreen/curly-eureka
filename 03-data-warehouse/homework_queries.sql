-- Question 1
SELECT count(*) FROM `demo_dataset.yellow_tripdata_regular`;

-- Question 2
SELECT DISTINCT PULocationID FROM `demo_dataset.yellow_tripdata_regular`;
SELECT DISTINCT PULocationID FROM `demo_dataset.yellow_tripdata_external`;

-- Question 3
SELECT PULocationID FROM `demo_dataset.yellow_tripdata_regular`;
SELECT PULocationID, DOLocationID FROM `demo_dataset.yellow_tripdata_regular`;

-- Question 4
SELECT count(*) FROM `demo_dataset.yellow_tripdata_regular` where fare_amount = 0;

-- Question 5
CREATE OR REPLACE TABLE `demo_dataset.yellow_tripdata_partitoned`
PARTITION BY
DATE (tpep_dropoff_datetime) 
CLUSTER BY VendorID AS
SELECT * FROM `demo_dataset.yellow_tripdata_regular`;

-- Question 6
SELECT DISTINCT VendorID FROM `demo_dataset.yellow_tripdata_regular` WHERE tpep_dropoff_datetime >= '2024-03-01 00:00:00'
AND tpep_dropoff_datetime < '2024-03-16 00:00:00';

SELECT DISTINCT VendorID FROM `demo_dataset.yellow_tripdata_partitoned` WHERE tpep_dropoff_datetime >= '2024-03-01 00:00:00'
AND tpep_dropoff_datetime < '2024-03-16 00:00:00';