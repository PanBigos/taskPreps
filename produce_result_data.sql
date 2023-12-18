-- STEP 1 - join main events with additional events
-- I don't know if keys can repeat in different preps, but if they can, we should consider adding both the key and possibly other fields to the join, not just the key and block.
DROP TABLE IF EXISTS parsed_table;
CREATE TEMPORARY TABLE parsed_table AS
SELECT 
    a.*, 
    b.event_name AS additional_event_name,
    b.size,
    b.collateral,
    b.realisedPnl,
    b.averagePrice,
    b.reserveAmount,
    b.entryFundingRate
FROM 
    protocols.arbi_gmx_fix_main_parsed AS a
JOIN 
    protocols.arbi_gmx_fix_additional_parsed AS b ON a.key = b.key AND a.block_number = b.block_number;
	
-- STEP 2 - Find start and end of each
DROP TABLE IF EXISTS trade_table;
CREATE TEMPORARY TABLE trade_table AS
SELECT 
    full_table.*,
    (
        SELECT MIN(pt.block_number)
        FROM parsed_table pt 
        WHERE 
            full_table.key = pt.key 
            AND pt.block_number >= full_table.block_number 
            AND (pt.event_name = 'LiquidatePosition' OR pt.additional_event_name = 'ClosePosition')
    ) AS first_non_increase_event_after
FROM 
    parsed_table AS full_table
ORDER BY 
    full_table.block_number ASC;	

-- STEP 3 - Calculate first part of metrics
DROP TABLE IF EXISTS precalculated;

CREATE TEMPORARY TABLE precalculated AS
SELECT DISTINCT
    account,
    key,
    SUM(size) OVER tradeWindow AS volume,
    CASE 
        WHEN first_non_increase_event_after IS NULL THEN 
            (LAST_VALUE(size) OVER tradeWindow) / (LAST_VALUE(averageprice) OVER tradeWindow)
        ELSE 0 
    END AS last_size_token,
    CASE 
        WHEN first_non_increase_event_after IS NULL THEN 
            LAST_VALUE(size) OVER tradeWindow
        ELSE 0 
    END AS last_size_usd,
    LAST_VALUE(collateral) OVER tradeWindow AS collateral,
    CASE 
        WHEN first_non_increase_event_after IS NULL THEN 
            (LAST_VALUE(size) OVER tradeWindow) * 10000 / (LAST_VALUE(collateral) OVER tradeWindow) / 10000
        ELSE 0 
    END AS leverage,
    SUM(realisedpnl) OVER tradeWindow AS realised_pnl,
    SUM(fee) OVER tradeWindow AS total_fees,
    MAX(collateral) OVER tradeWindow AS max_collateral,
    MAX(size) OVER tradeWindow AS max_size,
    MIN(block_number) OVER tradeWindow AS open_block,
    CASE 
        WHEN first_non_increase_event_after IS NOT NULL THEN 
            first_non_increase_event_after 
    END AS close_block,
    MIN(block_timestamp) OVER tradeWindow AS open_ts,
    CASE 
        WHEN first_non_increase_event_after IS NOT NULL THEN 
            MAX(block_timestamp) OVER tradeWindow 
    END AS close_ts,
    MAX(block_number) OVER tradeWindow AS last_update_block,
    MAX(block_timestamp) OVER tradeWindow AS last_update_ts,
    FIRST_VALUE(price) OVER tradeWindow AS entry_price,
    LAST_VALUE(averageprice) OVER tradeWindow AS last_avg_price,
    CASE 
        WHEN LAST_VALUE(event_name) OVER tradeWindow = 'LiquidatePosition' THEN 
            LAST_VALUE(markPrice) OVER tradeWindow 
    END AS liquidation_mark_price,
    CASE 
        WHEN first_non_increase_event_after IS NOT NULL THEN 
            LAST_VALUE(price) OVER tradeWindow 
    END AS close_price,
    CASE 
        WHEN first_non_increase_event_after IS NULL THEN 
            TRUE 
        ELSE FALSE 
    END AS is_open,
	indextoken || '/' || collateraltoken AS market,
    isLong AS is_long
FROM 
    trade_table
WINDOW 
    tradeWindow AS (PARTITION BY account, first_non_increase_event_after);

-- STEP 4 - Calculate second part of metrics and filter trades with complete data
INSERT INTO protocols.results_table
SELECT 
    account,
    key,
    ROW_NUMBER() OVER (PARTITION BY account ORDER BY open_block) AS trade_no,
    volume,
    last_size_token,
    last_size_usd,
    collateral,
    leverage,
    realised_pnl,
    (realised_pnl / max_collateral) AS pct_profit,
    total_fees,
    max_collateral,
    max_size,
    open_block,
    close_block,
    open_ts,
    close_ts,
    entry_price,
    close_price,
    last_avg_price,
    liquidation_mark_price,
    is_open,
    last_update_ts,
    last_update_block,
    market,
    is_long   
FROM 
    precalculated
WHERE
	open_block <> close_block OR close_block IS NULL
ORDER BY 
    account, trade_no;