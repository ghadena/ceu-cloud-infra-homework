DROP TABLE IF EXISTS ghadena_homework.silver_views;

CREATE TABLE ghadena_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://gh25-wikidata/datalake/views_silver/'
    ) AS
    SELECT 
        article,
        views, 
        rank,
        date
    FROM ghadena_homework.bronze_views;

select * from silver_views;
