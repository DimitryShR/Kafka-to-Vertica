DROP TABLE IF EXISTS STV2023121128__DWH.global_metrics;

CREATE TABLE STV2023121128__DWH.global_metrics (
	date_update date NOT NULL,
	currency_from int NOT NULL,
	amount_total numeric(14,2) NOT NULL,
	cnt_transactions int NOT NULL,
	avg_transactions_per_account numeric(14,2) NOT NULL,
	cnt_accounts_make_transactions int NOT NULL,
	PRIMARY KEY (date_update, currency_from)
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update
GROUP BY calendar_hierarchy_day(date_update, 3, 2);