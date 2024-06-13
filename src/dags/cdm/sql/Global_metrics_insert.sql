INSERT INTO STV2023121128__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
                WITH transaction_tab AS (
                SELECT
                    t.id,
                    t.operation_id,
                    t.msg_dttm,
                    t.account_number_from,
                    t.currency_code,
                    t.amount,
                    t.transaction_dt::DATE	AS dt,
                    CASE WHEN t.currency_code = 420 THEN 1 ELSE c.currency_with_div END AS currency_with_div,
                    max(id) OVER (PARTITION BY operation_id) AS max_id
                FROM STV2023121128__STAGING.transactions t
                LEFT JOIN
                    (SELECT DISTINCT
                    currency_code,
                    currency_with_div,
                    date_update::DATE
                    FROM STV2023121128__STAGING.currency
                    WHERE currency_code_with = 420) AS c
                ON t.currency_code = c.currency_code AND t.transaction_dt::DATE = c.date_update
                WHERE account_number_from <> -1
                AND t.status = 'done'
                AND t.transaction_dt::DATE = :dt_load
                )
                SELECT
                    dt								AS date_update,
                    currency_code					AS currency_from,
                    sum(amount * currency_with_div)	AS amount_total,
                    count(*)						AS cnt_transactions,
                    round(sum(amount * currency_with_div) / count(account_number_from), 2)	AS avg_transactions_per_account,
                    count(DISTINCT account_number_from)	AS cnt_accounts_make_transactions
                FROM transaction_tab
                WHERE id = max_id
                AND NOT EXISTS(
                    SELECT date_update, currency_from
                    FROM STV2023121128__DWH.global_metrics gm
                    WHERE gm.date_update = transaction_tab.dt
                    AND gm.currency_from = transaction_tab.currency_code
                    )
                GROUP BY dt, currency_code;