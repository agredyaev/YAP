CREATE TABLE IF NOT EXISTS dds.h_order (
	h_order_pk uuid NOT NULL,
	order_id int NOT null unique,
	order_dt timestamp NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_order_pk PRIMARY KEY (h_order_pk)
);
