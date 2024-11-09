DROP TABLE IF EXISTS client_name.product_alternates;

CREATE TABLE client_name.product_alternates AS
SELECT
CAST(TRIM(LEADING '0' FROM meta.product_upc) AS SIGNED) AS product_id,
RIGHT(meta.product_upc, 14) AS upc,
meta.case_upc AS alternate_type,
meta.case_pack AS case_pack,
NOW() AS created_at
FROM client_name.meta;

ALTER TABLE client_name.product_alternates
ADD COLUMN product_alternate_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE product_alternates
ADD CONSTRAINT fk_product_id
FOREIGN KEY (product_id)
REFERENCES products(product_id)
ON DELETE CASCADE
ON UPDATE RESTRICT;
