-- Schema definition for dim_trade_calendar_full
CREATE OR REPLACE MACRO dim._schema_dim_trade_calendar_full() AS
MAP {
    0: {name: 'trade_date', type: 'DATE', default_value: NULL},
    1: {name: 'trade_year', type: 'INTEGER', default_value: NULL},
    2: {name: 'trade_month', type: 'INTEGER', default_value: NULL},
    3: {name: 'trade_quarter', type: 'INTEGER', default_value: NULL},
    4: {name: 'trade_day_of_week', type: 'INTEGER', default_value: NULL},
    5: {name: 'trade_day_of_month', type: 'INTEGER', default_value: NULL},
    6: {name: 'trade_day_of_year', type: 'INTEGER', default_value: NULL},
    7: {name: 'trade_week_of_year', type: 'INTEGER', default_value: NULL},
    8: {name: 'is_weekday', type: 'BOOLEAN', default_value: NULL},
    9: {name: 'is_month_end', type: 'BOOLEAN', default_value: NULL},
    10: {name: 'is_quarter_end', type: 'BOOLEAN', default_value: NULL},
    11: {name: 'is_year_end', type: 'BOOLEAN', default_value: NULL}
};

-- Seed parquet for schema initialization
COPY (
    SELECT
        CAST(NULL AS DATE) AS trade_date,
        CAST(NULL AS INTEGER) AS trade_year,
        CAST(NULL AS INTEGER) AS trade_month,
        CAST(NULL AS INTEGER) AS trade_quarter,
        CAST(NULL AS INTEGER) AS trade_day_of_week,
        CAST(NULL AS INTEGER) AS trade_day_of_month,
        CAST(NULL AS INTEGER) AS trade_day_of_year,
        CAST(NULL AS INTEGER) AS trade_week_of_year,
        CAST(NULL AS BOOLEAN) AS is_weekday,
        CAST(NULL AS BOOLEAN) AS is_month_end,
        CAST(NULL AS BOOLEAN) AS is_quarter_end,
        CAST(NULL AS BOOLEAN) AS is_year_end
    WHERE 1=0
) TO 's3://stock-data/lake/dim/dim_trade_calendar_full/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- DIM View definition
CREATE OR REPLACE VIEW dim.dim_trade_calendar_full AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/dim/dim_trade_calendar_full/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

-- Date partition macro for querying specific partitions
CREATE OR REPLACE MACRO dim.dim_trade_calendar_full_dt(p_date) AS TABLE
SELECT * FROM dim.dim_trade_calendar_full
WHERE dt = p_date;
