CREATE TABLE IF NOT EXISTS dds.s_restaurant_names (
	hk_restaurant_names_pk uuid NOT NULL,
	h_restaurant_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT s_restaurant_names_pk PRIMARY KEY (hk_restaurant_names_pk)
);

ALTER TABLE dds.s_restaurant_names ADD CONSTRAINT s_restaurant_names_fk_h_restautrant FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk) ON DELETE CASCADE ON UPDATE CASCADE;