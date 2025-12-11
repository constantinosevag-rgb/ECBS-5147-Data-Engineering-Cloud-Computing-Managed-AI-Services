CREATE EXTERNAL TABLE IF NOT EXISTS 
konstantinos.raw_views (
    title STRING,
    views INT,
    rank INT,
    date STRING,
    retrieved_at STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://konstantinos-wikidata/raw-views/';