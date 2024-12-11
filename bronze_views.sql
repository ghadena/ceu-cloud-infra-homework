CREATE DATABASE ghadena_homework;

DROP TABLE IF EXISTS ghadena_homework.bronze_views;

CREATE EXTERNAL TABLE
    bronze_views (
    article STRING,
    views INT, 
    rank INT,
    date DATE,
    retrieved_at timestamp) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://gh25-wikidata/datalake/views/';

select * from ghadena_homework.bronze_views;
 
