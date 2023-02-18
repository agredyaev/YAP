CREATE TABLE IF NOT EXISTS cdm.user_product_counters (
	id serial NOT NULL,
	user_id uuid NOT NULL,
	product_id uuid NOT NULL,
	product_name varchar NOT NULL,
	order_cnt int NOT NULL,
	CONSTRAINT user_product_counters_check CHECK ((order_cnt >= 0)),
	CONSTRAINT user_product_counters_pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX user_product_counters_user_id_idx ON cdm.user_product_counters USING btree (user_id, product_id);