SELECT
  CAST(symbol AS VARCHAR) AS symbol,
  CAST(STRPTIME(CAST(trade_date AS VARCHAR), '%Y%m%d') AS DATE) AS trade_date,
  CAST(open AS DOUBLE) AS open,
  CAST(high AS DOUBLE) AS high,
  CAST(low AS DOUBLE) AS low,
  CAST(close AS DOUBLE) AS close,
  CAST(volume AS DOUBLE) AS volume,
  CAST(amount AS DOUBLE) AS amount,
  CAST(amplitude AS DOUBLE) AS amplitude,
  CAST(change_percent AS DOUBLE) AS change_percent,
  CAST(change_amount AS DOUBLE) AS change_amount,
  CAST(turnover_rate AS DOUBLE) AS turnover_rate,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_fund_etf_history;