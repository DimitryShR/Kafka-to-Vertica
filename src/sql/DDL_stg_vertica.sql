DROP TABLE IF EXISTS STV2023121128__STAGING.transactions;
CREATE TABLE STV2023121128__STAGING.transactions (
	id int NOT NULL PRIMARY KEY,
	object_id varchar NOT NULL,
	msg_dttm timestamp NOT NULL,
	sent_dttm timestamp NOT NULL,
	operation_id varchar NOT NULL,
	account_number_from int NOT NULL,
	account_number_to int NOT NULL,
	currency_code int NOT NULL,
	country varchar NOT NULL,
	status varchar NOT NULL,
	transaction_type varchar NOT NULL,
    amount NUMERIC (14,2) NOT NULL,
    transaction_dt timestamp
)
ORDER BY object_id, sent_dttm
SEGMENTED BY HASH(object_id, sent_dttm) ALL NODES
PARTITION BY sent_dttm::date
GROUP BY calendar_hierarchy_day(sent_dttm::date, 3, 2);


DROP TABLE IF EXISTS STV2023121128__STAGING.currency;
CREATE TABLE STV2023121128__STAGING.currency (
	id int NOT NULL PRIMARY KEY,
	object_id varchar NOT NULL,
	msg_dttm timestamp NOT NULL,
	sent_dttm timestamp NOT NULL,
	date_update timestamp NOT NULL,
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	currency_with_div NUMERIC(14,13) NOT NULL
)
ORDER BY object_id, sent_dttm
SEGMENTED BY HASH(object_id, sent_dttm) ALL NODES
PARTITION BY sent_dttm::date
GROUP BY calendar_hierarchy_day(sent_dttm::date, 3, 2);