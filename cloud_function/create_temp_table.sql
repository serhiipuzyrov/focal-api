DROP TABLE IF EXISTS client_name.temp_table;

CREATE TABLE client_name.temp_table AS
SELECT
TRIM(LEADING '0' FROM meta.product_upc) AS product_id,
RIGHT(meta.product_upc, 14) AS upc,
meta.item_description AS name,
meta.item_number AS item_number,
prices.price,
meta.supplier,
inventory.inventory_level,
STR_TO_DATE(CONCAT(inventory.report_date, ' ', inventory.report_time),'%Y-%m-%d %H:%i:%s') AS inventory_updated_at,
NOW() AS created_at,
NOW() AS updated_at
FROM client_name.meta
LEFT JOIN client_name.prices ON TRIM(LEADING '0' FROM meta.product_upc) = prices.product_upc
LEFT JOIN client_name.inventory USING(item_number);
