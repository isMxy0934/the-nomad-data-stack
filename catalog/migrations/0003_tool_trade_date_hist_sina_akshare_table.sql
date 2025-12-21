-- ODS Tool Trade Date Hist from Sina AkShare
CREATE OR REPLACE MACRO ods._schema_tool_trade_date_hist_sina_akshare() AS
MAP {
    0: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_tool_trade_date_hist_sina_akshare/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW ods.ods_tool_trade_date_hist_sina_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_tool_trade_date_hist_sina_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO ods.ods_tool_trade_date_hist_sina_akshare_dt(p_date) AS TABLE 
SELECT * FROM ods.ods_tool_trade_date_hist_sina_akshare
WHERE dt = p_date;