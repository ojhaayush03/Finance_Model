CREATE DATABASE IF NOT EXISTS financial_db;

USE financial_db;

DROP TABLE IF EXISTS stock_indicators;

CREATE EXTERNAL TABLE IF NOT EXISTS stock_indicators (
    `Date` STRING,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    Close_Dup FLOAT,
    SMA_10 FLOAT,
    EMA_10 FLOAT,
    Daily_Return FLOAT,
    Volatility_10 FLOAT,
    RSI_14 FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/ayush/financial_project/processed_data/'
TBLPROPERTIES ("skip.header.line.count"="1");
