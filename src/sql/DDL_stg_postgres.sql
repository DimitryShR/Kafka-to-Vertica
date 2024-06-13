-- ************************ stg ************************
DROP SCHEMA IF EXISTS stg;
CREATE SCHEMA stg;

DROP TABLE IF EXISTS stg.transactions;
CREATE TABLE stg.transactions (
	id serial4 NOT NULL PRIMARY KEY,
	object_id varchar NULL,
	object_type varchar NULL,
	msg_value varchar NOT NULL,
	topic varchar NOT NULL,
	"partition" int NOT NULL,
	"offset" int NOT NULL,
	msg_dttm timestamp NOT NULL,
	UNIQUE (topic, "partition", "offset")
);


DROP TABLE IF EXISTS stg.wf_settings;
CREATE TABLE stg.wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL PRIMARY KEY,
	workflow_settings JSON NOT NULL
);