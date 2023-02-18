
CREATE TABLE dds.s_order_cost (
	hk_order_cost_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	"cost" numeric(19, 5) NOT NULL,
	payment numeric(19, 5) NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT s_order_cost_pk PRIMARY KEY (hk_order_cost_pk),
	CONSTRAINT s_order_cost_check CHECK ((cost >= (0)::numeric)),
	CONSTRAINT s_order_payment_check CHECK ((payment >= (0)::numeric))
);


ALTER TABLE dds.s_order_cost ADD CONSTRAINT s_order_cost_fk_h_order FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE CASCADE ON UPDATE CASCADE;