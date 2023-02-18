CREATE TABLE IF NOT EXISTS dds.h_restaurant (
	h_restaurant_pk uuid NOT NULL,
	restaurant_id varchar NOT null unique,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_restaurant_pk PRIMARY KEY (h_restaurant_pk)
);
