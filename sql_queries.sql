USE ecommerce_pm;

CREATE TABLE user_events (
    timestamp BIGINT,
    visitorid INT,
    event VARCHAR(50),
    itemid INT,
    transactionid INT,
    datetime DATETIME
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_events.csv'
INTO TABLE user_events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE category_tree (
    categoryid INT,
    parentid INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_category_tree.csv'
INTO TABLE category_tree
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE item_properties (
    timestamp BIGINT,
    itemid INT,
    property VARCHAR(255),
    value TEXT,
    datetime DATETIME
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_item_properties.csv'
INTO TABLE item_properties
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'user_events' AS table_name, COUNT(*) AS row_count FROM user_events
UNION ALL
SELECT 'item_properties' AS table_name, COUNT(*) AS row_count FROM item_properties
UNION ALL
SELECT 'category_tree' AS table_name, COUNT(*) AS row_count FROM category_tree;

SELECT 
    event, COUNT(*) AS total_events, 
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM user_events
GROUP BY event
ORDER BY total_events DESC;

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN visitorid IS NULL THEN 1 ELSE 0 END) AS null_visitorid,
    SUM(CASE WHEN itemid IS NULL THEN 1 ELSE 0 END) AS null_itemid,
    SUM(CASE WHEN event IS NULL THEN 1 ELSE 0 END) AS null_event,
    SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END) AS null_datetime,
    SUM(CASE WHEN transactionid = 0 THEN 1 ELSE 0 END) AS zero_transactionid
FROM user_events;

SELECT 
    MIN(datetime) AS earliest_event,
    MAX(datetime) AS latest_event,
    DATEDIFF(MAX(datetime), MIN(datetime)) AS span_days
FROM user_events;

SELECT 
    property, COUNT(*) AS row_count
FROM item_properties
GROUP BY property
ORDER BY row_count DESC
LIMIT 10;

SELECT 
    COUNT(*)                                              AS total_categories,
    SUM(CASE WHEN parentid = -1 THEN 1 ELSE 0 END)       AS root_categories,
    SUM(CASE WHEN parentid != -1 THEN 1 ELSE 0 END)      AS child_categories
FROM category_tree;

use ecommerce_pm;

CREATE TABLE item_category_map AS
SELECT 
    itemid,
    CAST(value AS UNSIGNED)  AS categoryid,
    MAX(datetime)            AS last_updated   
FROM item_properties
WHERE property = 'categoryid'
GROUP BY itemid, value;

SELECT COUNT(*) AS mapped_items FROM item_category_map;
SELECT * FROM item_category_map LIMIT 5;

CREATE TABLE category_hierarchy AS
SELECT
    c.categoryid,
    c.parentid,
    CASE 
        WHEN c.parentid = -1 THEN c.categoryid   
        ELSE c.parentid                           
    END AS top_level_categoryid
FROM category_tree c;

SELECT * FROM category_hierarchy LIMIT 5;

CREATE TABLE master_events AS
SELECT
    e.visitorid,
    e.event,
    e.itemid,
    e.transactionid,
    e.datetime,
    DATE(e.datetime) AS event_date,
    YEAR(e.datetime) AS event_year,
    MONTH(e.datetime) AS event_month,
    WEEK(e.datetime, 1) AS event_week,       
    DATE_FORMAT(e.datetime, '%Y-%u') AS year_week,        
    icm.categoryid,
    ch.parentid,
    ch.top_level_categoryid
FROM user_events e
LEFT JOIN item_category_map    icm ON e.itemid     = icm.itemid
LEFT JOIN category_hierarchy   ch  ON icm.categoryid = ch.categoryid;

SELECT COUNT(*) AS master_rows FROM master_events;

SELECT
    COUNT(*) AS total_events,
    SUM(CASE WHEN categoryid IS NOT NULL THEN 1 ELSE 0 END) AS events_with_category,
    ROUND(SUM(CASE WHEN categoryid IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
    AS enrichment_pct
FROM master_events;

ALTER TABLE master_events ADD INDEX idx_event (event);
ALTER TABLE master_events ADD INDEX idx_visitorid (visitorid);
ALTER TABLE master_events ADD INDEX idx_event_date (event_date);
ALTER TABLE master_events ADD INDEX idx_year_week (year_week);
ALTER TABLE master_events ADD INDEX idx_categoryid (categoryid);
ALTER TABLE master_events ADD INDEX idx_user_journey (visitorid, event, datetime);
ALTER TABLE master_events ADD INDEX idx_category_funnel (categoryid, event);
ALTER TABLE master_events ADD INDEX idx_itemid (itemid);

SELECT 
    year_week,
    top_level_categoryid AS department_id,
    COUNT(CASE WHEN event = 'view' THEN 1 END) AS total_views,
    COUNT(CASE WHEN event = 'addtocart' THEN 1 END) AS total_carts,
    COUNT(CASE WHEN event = 'transaction' THEN 1 END) AS total_purchases
FROM master_events
WHERE top_level_categoryid IS NOT NULL
GROUP BY year_week, top_level_categoryid;

SELECT 
    'Entire Platform' AS scope,
    COUNT(CASE WHEN event = 'view' THEN 1 END) AS total_views,
    COUNT(CASE WHEN event = 'addtocart' THEN 1 END) AS total_carts,
    COUNT(CASE WHEN event = 'transaction' THEN 1 END) AS total_purchases,
    ROUND(COUNT(CASE WHEN event = 'addtocart' THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN event = 'view' THEN 1 END), 0), 2) AS view_to_cart_pct,
    ROUND(COUNT(CASE WHEN event = 'transaction' THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN event = 'addtocart' THEN 1 END), 0), 2) AS cart_to_purchase_pct
FROM master_events;

WITH DepartmentRates AS (
    SELECT 
        top_level_categoryid AS department_id,
        COUNT(CASE WHEN event = 'view' THEN 1 END) AS total_views,
        COUNT(CASE WHEN event = 'addtocart' THEN 1 END) AS total_carts,
        COUNT(CASE WHEN event = 'transaction' THEN 1 END) AS total_purchases,
        ROUND(COUNT(CASE WHEN event = 'addtocart' THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN event = 'view' THEN 1 END), 0), 2) AS view_to_cart_pct,
        ROUND(COUNT(CASE WHEN event = 'transaction' THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN event = 'addtocart' THEN 1 END), 0), 2) AS cart_to_purchase_pct
    FROM master_events
    WHERE top_level_categoryid IS NOT NULL 
    GROUP BY top_level_categoryid
    HAVING COUNT(CASE WHEN event = 'view' THEN 1 END) > 5000 
       AND COUNT(CASE WHEN event = 'addtocart' THEN 1 END) > 50
),

RankedDepartments AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY cart_to_purchase_pct DESC) AS rank_best,
        ROW_NUMBER() OVER (ORDER BY cart_to_purchase_pct ASC) AS rank_worst
    FROM DepartmentRates
)

SELECT 
    CASE 
        WHEN rank_best <= 10 THEN 'Top 10: Cash Cows'
        WHEN rank_worst <= 10 THEN 'Bottom 10: Checkout Drop-offs'
    END AS performance_category,
    department_id,
    total_views,
    total_carts,
    total_purchases,
    view_to_cart_pct,
    cart_to_purchase_pct
FROM RankedDepartments
WHERE rank_best <= 10 OR rank_worst <= 10
ORDER BY performance_category DESC, cart_to_purchase_pct DESC;

SELECT 
    MIN(event_date) AS first_day,
    MAX(event_date) AS last_day,
    ROUND(DATEDIFF(MAX(event_date), MIN(event_date)) / 30, 1) AS total_months
FROM master_events;

SELECT
    year_week,
    COUNT(DISTINCT visitorid) AS weekly_active_users,
    COUNT(DISTINCT CASE WHEN event = 'view' THEN visitorid END) AS weekly_viewers,
    COUNT(DISTINCT CASE WHEN event = 'addtocart' THEN visitorid END) AS weekly_cart_users,
    COUNT(DISTINCT CASE WHEN event = 'transaction' THEN visitorid END) AS weekly_buyers
FROM master_events
GROUP BY year_week
ORDER BY year_week;

WITH weekly_users AS (
    SELECT year_week, COUNT(DISTINCT visitorid) AS wau
    FROM master_events
    GROUP BY year_week
)
SELECT
    year_week, wau, LAG(wau) OVER (ORDER BY year_week) AS prev_week_wau,
    ROUND((wau - LAG(wau) OVER (ORDER BY year_week)) * 100.0 /
           NULLIF(LAG(wau) OVER (ORDER BY year_week), 0), 2)                                                            AS wow_growth_pct
FROM weekly_users
ORDER BY year_week;
