DROP TABLE IF EXISTS ghadena_homework.gold_allviews;

CREATE TABLE ghadena_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://gh25-wikidata/datalake/gold_allviews/'
    ) AS
    SELECT 
        article,
        SUM(views) AS total_top_view,
        MIN(rank) AS top_rank,
        COUNT(DISTINCT date) AS ranked_days
    FROM ghadena_homework.silver_views
    GROUP BY article;
    
select * from gold_allviews;
