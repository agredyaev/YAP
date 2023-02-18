CREATE TABLE IF NOT EXISTS dds.h_category (
	h_category_pk uuid NOT NULL,
	category_name varchar NOT null unique,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_category_pk PRIMARY KEY (h_category_pk)
);
