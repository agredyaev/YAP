
CREATE TABLE IF NOT EXISTS dds.l_order_product (
	hk_order_product_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_order_product_pk PRIMARY KEY (hk_order_product_pk)
);


ALTER TABLE dds.l_order_product ADD CONSTRAINT l_order_product_fk_h_order FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE dds.l_order_product ADD CONSTRAINT l_order_product_fk_h_product FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE CASCADE ON UPDATE CASCADE;