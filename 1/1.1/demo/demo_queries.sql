--Данные запросы применяются для проверки верности решения

-- Проверка перед запуском DAG
SELECT count(*) AS ft_balance_rows
FROM ds.ft_balance_f;

-- Демонстрация заполнения 
SELECT on_date, account_rk, currency_rk, balance_out
FROM ds.ft_balance_f
ORDER BY account_rk
LIMIT 10;

-- Демонстрация изменения баланса на выбранном аккаунте
SELECT on_date, account_rk, currency_rk, balance_out
FROM ds.ft_balance_f
WHERE account_rk = 24656;

-- Проверка логов
SELECT run_id, dag_id, status, started_at, ended_at, duration_sec,
       total_rows_read, total_rows_loaded, error_message
FROM logs.etl_run_log
ORDER BY started_at DESC
LIMIT 5;

-- Проверка логов для последнего прохода DAG
SELECT table_name, source_file, load_mode, status, rows_read, rows_loaded,
       bad_date_rows, started_at, ended_at, error_message
FROM logs.etl_table_log
WHERE run_id = (
    SELECT run_id
    FROM logs.etl_run_log
    ORDER BY started_at DESC
    LIMIT 1
)
ORDER BY started_at;
