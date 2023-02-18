CREATE TABLE dds.l_order_user (
	hk_order_user_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	h_user_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_order_user_pk PRIMARY KEY (hk_order_user_pk)
);

ALTER TABLE dds.l_order_user ADD CONSTRAINT l_order_user_fk_h_order FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE dds.l_order_user ADD CONSTRAINT l_order_user_fk_h_user FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk) ON DELETE CASCADE ON UPDATE CASCADE;