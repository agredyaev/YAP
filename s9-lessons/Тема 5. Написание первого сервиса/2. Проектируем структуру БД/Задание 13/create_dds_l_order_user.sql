CREATE TABLE IF NOT EXISTS dds.l_product_category (
	hk_product_category_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	h_category_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_product_category_pk PRIMARY KEY (hk_product_category_pk)
);


ALTER TABLE dds.l_product_category ADD CONSTRAINT l_product_category_fk_h_category FOREIGN KEY (h_category_pk) REFERENCES dds.h_category(h_category_pk) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE dds.l_product_category ADD CONSTRAINT l_product_category_fk_h_product FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE CASCADE ON UPDATE CASCADE;