-- Проверка исходного слоя
SELECT count(*) AS ft_balance_rows
FROM ds.ft_balance_f;

SELECT count(*) AS ft_posting_rows
FROM ds.ft_posting_f;

-- Проверка витрины оборотов
SELECT count(*) AS turnover_rows
FROM dm.dm_account_turnover_f;

SELECT on_date, count(*) AS account_count
FROM dm.dm_account_turnover_f
GROUP BY on_date
ORDER BY on_date;

SELECT on_date,
       account_rk,
       credit_amount,
       credit_amount_rub,
       debet_amount,
       debet_amount_rub
FROM dm.dm_account_turnover_f
ORDER BY on_date, account_rk
LIMIT 20;

-- Проверка витрины остатков
SELECT count(*) AS balance_rows
FROM dm.dm_account_balance_f;

SELECT on_date, count(*) AS account_count
FROM dm.dm_account_balance_f
GROUP BY on_date
ORDER BY on_date;

SELECT on_date,
       account_rk,
       balance_out,
       balance_out_rub
FROM dm.dm_account_balance_f
ORDER BY on_date, account_rk
LIMIT 20;

-- Проверка логов
SELECT run_id,
       dag_id,
       run_type,
       status,
       started_at,
       ended_at,
       duration_sec,
       total_rows_read,
       total_rows_loaded,
       error_message
FROM logs.etl_run_log
ORDER BY started_at DESC
LIMIT 20;

SELECT table_name,
       source_file,
       load_mode,
       status,
       rows_read,
       rows_loaded,
       started_at,
       ended_at,
       error_message
FROM logs.etl_table_log
ORDER BY started_at DESC
LIMIT 40;
