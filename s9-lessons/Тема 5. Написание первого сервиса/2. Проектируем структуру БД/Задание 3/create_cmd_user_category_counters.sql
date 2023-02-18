CREATE TABLE cdm.user_category_counters (
	id serial NOT NULL,
	user_id uuid NOT NULL,
	category_id uuid NOT NULL,
	category_name varchar NOT NULL,
	order_cnt int NOT NULL,
	CONSTRAINT user_category_counters_check CHECK ((order_cnt >= 0)),
	CONSTRAINT user_category_counters_pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX user_category_counters_user_id_idx ON cdm.user_category_counters USING btree (user_id, category_id);