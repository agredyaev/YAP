CREATE TABLE IF NOT EXISTS stg.order_events (
	id serial NOT NULL,
	object_id int NOT null UNIQUE,
	object_type varchar NOT NULL,
	sent_dttm timestamp NOT NULL,
	payload json NOT NULL,
	CONSTRAINT order_events_pk PRIMARY KEY (id)
);
