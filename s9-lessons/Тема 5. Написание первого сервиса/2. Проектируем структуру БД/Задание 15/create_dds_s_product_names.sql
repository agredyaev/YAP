CREATE TABLE dds.s_product_names (
	hk_product_names_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT s_product_names_pk PRIMARY KEY (hk_product_names_pk)
);

ALTER TABLE dds.s_product_names ADD CONSTRAINT s_product_names_fk_h_products FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE CASCADE ON UPDATE CASCADE;