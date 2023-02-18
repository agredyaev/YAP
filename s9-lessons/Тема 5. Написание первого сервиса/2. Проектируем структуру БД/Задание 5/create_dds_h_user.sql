CREATE TABLE dds.h_user (
	h_user_pk uuid NOT NULL,
	user_id varchar NOT null UNIQUE,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT order_events_pk PRIMARY KEY (h_user_pk)
);
