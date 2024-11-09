UPDATE client_name.products
JOIN client_name.temp_table ON client_name.products.upc = client_name.temp_table.upc
SET
products.name = temp_table.name,
products.item_number = temp_table.item_number,
products.price = temp_table.price,
products.supplier = temp_table.supplier,
products.inventory_level = temp_table.inventory_level,
products.updated_at = NOW()
WHERE
products.name != temp_table.name OR
products.item_number != temp_table.item_number OR
products.price != temp_table.price OR
products.supplier != temp_table.supplier OR
products.inventory_level != temp_table.inventory_level;

INSERT INTO client_name.products
SELECT
temp_table.product_id,
temp_table.upc,
temp_table.name,
temp_table.item_number,
temp_table.price,
temp_table.supplier,
temp_table.inventory_level,
temp_table.inventory_updated_at,
NOW() AS created_at,
NOW() AS updated_at
FROM client_name.temp_table
LEFT JOIN products ON temp_table.product_id = products.product_id
WHERE products.product_id IS NULL;

DROP TABLE IF EXISTS client_name.temp_table;
