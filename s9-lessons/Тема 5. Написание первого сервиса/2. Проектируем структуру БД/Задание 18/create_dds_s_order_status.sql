CREATE TABLE dds.s_order_status (
	hk_order_status_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	status varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT s_order_status_pk PRIMARY KEY (hk_order_status_pk)
);


ALTER TABLE dds.s_order_status ADD CONSTRAINT s_order_status_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE CASCADE ON UPDATE CASCADE;