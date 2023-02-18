CREATE TABLE IF NOT EXISTS dds.h_product (
	h_product_pk uuid NOT NULL,
	product_id varchar NOT null UNIQUE,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_product_pk PRIMARY KEY (h_product_pk)
);
