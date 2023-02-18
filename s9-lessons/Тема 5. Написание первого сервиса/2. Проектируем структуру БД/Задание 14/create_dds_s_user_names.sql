
CREATE TABLE IF NOT EXISTS dds.s_user_names (
	hk_user_names_pk uuid NOT NULL,
	h_user_pk uuid NOT NULL,
	username varchar NOT NULL,
	userlogin varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT s_user_names_pk PRIMARY KEY (hk_user_names_pk)
);

ALTER TABLE dds.s_user_names ADD CONSTRAINT s_user_names_fk_h_users FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk) ON DELETE CASCADE ON UPDATE CASCADE;