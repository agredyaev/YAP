CREATE TABLE IF NOT EXISTS dds.l_product_restaurant (
	hk_product_restaurant_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	h_restaurant_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_product_restaurant_pk PRIMARY KEY (hk_product_restaurant_pk)
);

ALTER TABLE dds.l_product_restaurant ADD CONSTRAINT l_product_restaurant_fk_h_product FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE dds.l_product_restaurant ADD CONSTRAINT l_product_restaurant_fk_h_restaurant FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk) ON DELETE CASCADE ON UPDATE CASCADE;