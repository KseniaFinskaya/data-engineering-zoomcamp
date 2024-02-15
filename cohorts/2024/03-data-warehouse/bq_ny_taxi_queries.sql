CREATE OR REPLACE EXTERNAL TABLE greentaxi-414403.ny_taxi.external_green_taxi_2022
OPTIONS (
format='PARQUET',
uris=['gs://ksenia-finskaya-green-taxi/ny_green_taxi_2022_parquet/green_tripdata_2022-*.parquet']
);

select count(1) from greentaxi-414403.ny_taxi.external_green_taxi_2022;
--84402

create or replace table greentaxi-414403.ny_taxi.green_taxi_2022
as
select * from greentaxi-414403.ny_taxi.external_green_taxi_2022;

create or replace table greentaxi-414403.ny_taxi.green_taxi_2022_partitioned
PARTITION BY
DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
as
select * from greentaxi-414403.ny_taxi.external_green_taxi_2022;

select count(1) from greentaxi-414403.ny_taxi.external_green_taxi_2022
where fare_amount=0;
-- distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022
select distinct PULocationID from greentaxi-414403.ny_taxi.green_taxi_2022
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
--12.82 MB
select distinct PULocationID from greentaxi-414403.ny_taxi.green_taxi_2022_partitioned
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
-- 1.12 MB

select distinct PULocationID from greentaxi-414403.ny_taxi.green_taxi_2022;
-- 6.41 MB
