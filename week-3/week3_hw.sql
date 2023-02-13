CREATE OR REPLACE EXTERNAL TABLE `root-welder-375217.trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_root-welder-375217/data/fhv/fhv_tripdata_2019-*.csv.gz']
);
CREATE OR REPLACE TABLE `trips_data_all.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `trips_data_all.fhv_tripdata`;

Question 1
--43244696
SELECT count(*)
FROM trips_data_all.fhv_tripdata

Question 2
-- 0 billed 2.52gb
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `trips_data_all.fhv_tripdata`;
-- 336.71
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `trips_data_all.fhv_nonpartitioned_tripdata`;

Question 3
-- 717748
SELECT COUNT(*)
FROM `trips_data_all.fhv_nonpartitioned_tripdata`
WHERE PUlocationID is null
AND DOlocationID is null

Question 4
Partition by pickup_datetime Cluster on affiliated_base_number

Question 5
CREATE OR REPLACE TABLE `trips_data_all.fhv_partitioned_clustered_tripdata`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number
AS (SELECT * FROM `trips_data_all.fhv_tripdata`);

. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31
-- Est 23mb
SELECT DISTINCT(affiliated_base_number)
FROM `trips_data_all.fhv_partitioned_clustered_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
-- 647.87
SELECT DISTINCT(affiliated_base_number)
FROM `trips_data_all.fhv_nonpartitioned_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

Question 6
GCP Bucket


Question 7
True