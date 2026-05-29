DROP TABLE IF EXISTS morph_storage_engine;
SELECT name FROM system.table_engines WHERE name = 'Morph';
CREATE TABLE morph_storage_engine
(
    x UInt64
)
ENGINE = Morph('bucket-id', 'token', 'Parquet');

DROP TABLE morph_storage_engine;

-- Empty bucket must fail fast, before any HTTP call is attempted.
SELECT * FROM morph('', 'token', 'Parquet', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
