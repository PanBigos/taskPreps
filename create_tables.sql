-- Table for events 'IncreasePosition', 'DecreasePosition', 'LiquidatePosition'
CREATE TABLE protocols.arbi_gmx_fix_main_parsed (
    hash            TEXT      NOT NULL,
    block_number    INTEGER,
    block_timestamp TIMESTAMP,
    log_index       INTEGER   NOT NULL,
    hash_log_index  TEXT,
    address         TEXT,
    protocol        TEXT,
    contract        TEXT,
    event_name      TEXT,
    fee             FLOAT,
    key             TEXT,
    price           FLOAT,
    isLong          BOOLEAN,
    account         TEXT,
    sizeDelta       FLOAT,
    indexToken      TEXT,
    collateralDelta FLOAT,
    collateralToken TEXT,
    markPrice       FLOAT,
    PRIMARY KEY (hash, log_index)
);

-- Table for events 'UpdatePosition', 'ClosePosition'
CREATE TABLE protocols.arbi_gmx_fix_additional_parsed (
    hash              TEXT      NOT NULL,
    block_number      INTEGER,
    block_timestamp   TIMESTAMP,
    log_index         INTEGER   NOT NULL,
    hash_log_index    TEXT,
    address           TEXT,
    protocol          TEXT,
    contract          TEXT,
    event_name        TEXT,
    key               TEXT,
    size              FLOAT,
    collateral        FLOAT,
    realisedPnl       FLOAT,
    averagePrice      FLOAT,
    reserveAmount     FLOAT,
    entryFundingRate  FLOAT,
    PRIMARY KEY (hash, log_index)
);


-- function to calcualate and set precision
CREATE OR REPLACE FUNCTION set_precision(value float) RETURNS float AS $$
BEGIN
    RETURN ((value * 10000)/1000000000000000000000000000000)/10000;
END;
$$ LANGUAGE plpgsql;

INSERT INTO protocols.arbi_gmx_fix_main_parsed
SELECT
    hash, 
    block_number, 
    block_timestamp, 
    log_index, 
    hash_log_index, 
    address, 
    protocol, 
    contract, 
    event_name,
    set_precision((args->>'fee')::FLOAT) AS fee,
    args->>'key' AS key,
    set_precision((args->>'price')::FLOAT) AS price,
    (args->>'isLong')::BOOLEAN AS isLong,
    args->>'account' AS account,
    set_precision((args->>'sizeDelta')::FLOAT) AS sizeDelta,
    args->>'indexToken' AS indexToken,
    set_precision((args->>'collateralDelta')::FLOAT) AS collateralDelta,
    args->>'collateralToken' AS collateralToken,
    set_precision((args->>'markPrice')::FLOAT) AS markPrice
FROM
    protocols.arbi_gmx_r
WHERE
    event_name IN ('IncreasePosition', 'DecreasePosition', 'LiquidatePosition');

INSERT INTO protocols.arbi_gmx_fix_additional_parsed
SELECT
    hash, 
    block_number, 
    block_timestamp, 
    log_index, 
    hash_log_index, 
    address, 
    protocol, 
    contract, 
    event_name,
    args->>'key' AS key,
    set_precision((args->>'size')::FLOAT) AS size,
    set_precision((args->>'collateral')::FLOAT) AS collateral,
    set_precision((args->>'realisedPnl')::FLOAT) AS realisedPnl,
    set_precision((args->>'averagePrice')::FLOAT) AS averagePrice,
    (args->>'reserveAmount')::FLOAT AS reserveAmount,
    (args->>'entryFundingRate')::FLOAT AS entryFundingRate
FROM
    protocols.arbi_gmx_r 
WHERE 
    event_name IN ('UpdatePosition', 'ClosePosition');


--Results table
CREATE TABLE protocols.results_table (
    account                 TEXT NOT NULL,
    key                     TEXT NOT NULL,
    trade_no                INTEGER,
    volume                  FLOAT,
    last_size_token         FLOAT,
    last_size_usd           FLOAT,
    collateral              FLOAT,
    leverage                FLOAT,
    realised_pnl            FLOAT,
    pct_profit              FLOAT,
    total_fees              FLOAT,
    max_collateral          FLOAT,
    max_size                FLOAT,
    open_block              INTEGER,
    close_block             INTEGER,
    open_ts                 TIMESTAMP,
    close_ts                TIMESTAMP,
    entry_price             FLOAT,
    close_price             FLOAT,
    last_avg_price          FLOAT,
    liquidation_mark_price  FLOAT,
    is_open                 BOOLEAN,
    last_update_ts          TIMESTAMP,
    last_update_block       INTEGER,
    market                  TEXT,
    is_long                 BOOLEAN
);