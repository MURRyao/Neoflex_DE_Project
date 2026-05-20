CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.etl_run_log (
    run_id uuid PRIMARY KEY,
    dag_id text NOT NULL,
    run_type text NOT NULL DEFAULT 'manual',
    status text NOT NULL,
    started_at timestamp NOT NULL,
    ended_at timestamp,
    duration_sec numeric(16, 3),
    total_rows_read bigint DEFAULT 0,
    total_rows_loaded bigint DEFAULT 0,
    error_message text
);

CREATE TABLE IF NOT EXISTS logs.etl_table_log (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES logs.etl_run_log(run_id),
    table_name text NOT NULL,
    source_file text NOT NULL,
    load_mode text NOT NULL,
    status text NOT NULL,
    started_at timestamp NOT NULL,
    ended_at timestamp,
    rows_read bigint DEFAULT 0,
    rows_loaded bigint DEFAULT 0,
    bad_date_rows bigint DEFAULT 0,
    error_message text
);

CREATE INDEX IF NOT EXISTS ix_etl_table_log_run_id
    ON logs.etl_table_log(run_id);

CREATE TABLE IF NOT EXISTS stg.deal_info_csv
(LIKE rd.deal_info INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS stg.product_csv
(LIKE rd.product INCLUDING DEFAULTS);

WITH csv_periods AS (
    SELECT
        'deal_info' AS table_name,
        effective_from_date,
        effective_to_date,
        count(*) AS rows_count
    FROM stg.deal_info_csv
    GROUP BY effective_from_date, effective_to_date

    UNION ALL

    SELECT
        'product' AS table_name,
        effective_from_date,
        effective_to_date,
        count(*) AS rows_count
    FROM stg.product_csv
    GROUP BY effective_from_date, effective_to_date
),
rd_periods AS (
    SELECT
        'deal_info' AS table_name,
        effective_from_date,
        effective_to_date,
        count(*) AS rows_count
    FROM rd.deal_info
    GROUP BY effective_from_date, effective_to_date

    UNION ALL

    SELECT
        'product' AS table_name,
        effective_from_date,
        effective_to_date,
        count(*) AS rows_count
    FROM rd.product
    GROUP BY effective_from_date, effective_to_date
)
SELECT
    coalesce(c.table_name, r.table_name) AS table_name,
    coalesce(c.effective_from_date, r.effective_from_date) AS effective_from_date,
    coalesce(c.effective_to_date, r.effective_to_date) AS effective_to_date,
    c.rows_count AS csv_rows_count,
    r.rows_count AS rd_rows_count,
    CASE
        WHEN r.table_name IS NULL THEN 'missing_in_rd'
        WHEN c.table_name IS NULL THEN 'extra_in_rd'
        WHEN c.rows_count <> r.rows_count THEN 'row_count_differs'
    END AS problem
FROM csv_periods AS c
FULL JOIN rd_periods AS r
  ON r.table_name = c.table_name
 AND r.effective_from_date IS NOT DISTINCT FROM c.effective_from_date
 AND r.effective_to_date IS NOT DISTINCT FROM c.effective_to_date
WHERE r.table_name IS NULL
   OR c.table_name IS NULL
    OR c.rows_count <> r.rows_count
ORDER BY
    table_name,
    effective_from_date,
    effective_to_date;

DROP PROCEDURE IF EXISTS dm.reload_loan_holiday_info();
DROP PROCEDURE IF EXISTS dm.reload_loan_holiday_info(date, date);

CREATE OR REPLACE PROCEDURE dm.reload_loan_holiday_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id uuid := gen_random_uuid();
    v_rows_read bigint := 0;
    v_rows_loaded bigint := 0;
    v_error_message text;
BEGIN
    INSERT INTO logs.etl_run_log (
        run_id, dag_id, run_type, status, started_at
    )
    VALUES (
        v_run_id, 'dm.reload_loan_holiday_info', 'procedure', 'STARTED', clock_timestamp()
    );

    INSERT INTO logs.etl_table_log (
        run_id, table_name, source_file, load_mode, status, started_at
    )
    VALUES (
        v_run_id,
        'dm.loan_holiday_info',
        'procedure:dm.reload_loan_holiday_info',
        'full_reload',
        'STARTED',
        clock_timestamp()
    );

    SELECT count(*)
      INTO v_rows_read
      FROM rd.deal_info AS d
      LEFT JOIN rd.loan_holiday AS lh
        ON lh.deal_rk = d.deal_rk
       AND lh.effective_from_date = d.effective_from_date
      LEFT JOIN rd.product AS p
        ON p.product_rk = d.product_rk
       AND d.effective_from_date BETWEEN p.effective_from_date AND p.effective_to_date;

    TRUNCATE TABLE dm.loan_holiday_info;

    INSERT INTO dm.loan_holiday_info (
        deal_rk,
        effective_from_date,
        effective_to_date,
        agreement_rk,
        account_rk,
        client_rk,
        department_rk,
        product_rk,
        product_name,
        deal_type_cd,
        deal_start_date,
        deal_name,
        deal_number,
        deal_sum,
        loan_holiday_type_cd,
        loan_holiday_start_date,
        loan_holiday_finish_date,
        loan_holiday_fact_finish_date,
        loan_holiday_finish_flg,
        loan_holiday_last_possible_date
    )
    SELECT
        d.deal_rk,
        coalesce(lh.effective_from_date, d.effective_from_date) AS effective_from_date,
        coalesce(lh.effective_to_date, d.effective_to_date) AS effective_to_date,
        d.agreement_rk,
        d.account_rk,
        d.client_rk,
        d.department_rk,
        d.product_rk,
        p.product_name,
        d.deal_type_cd,
        d.deal_start_date,
        d.deal_name,
        d.deal_num AS deal_number,
        d.deal_sum,
        lh.loan_holiday_type_cd,
        lh.loan_holiday_start_date,
        lh.loan_holiday_finish_date,
        lh.loan_holiday_fact_finish_date,
        lh.loan_holiday_finish_flg,
        lh.loan_holiday_last_possible_date
    FROM rd.deal_info AS d
    LEFT JOIN rd.loan_holiday AS lh
      ON lh.deal_rk = d.deal_rk
     AND lh.effective_from_date = d.effective_from_date
    LEFT JOIN rd.product AS p
      ON p.product_rk = d.product_rk
     AND d.effective_from_date BETWEEN p.effective_from_date AND p.effective_to_date;

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    UPDATE logs.etl_table_log
       SET status = 'SUCCESS',
           ended_at = clock_timestamp(),
           rows_read = v_rows_read,
           rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id
       AND table_name = 'dm.loan_holiday_info'
       AND status = 'STARTED';

    UPDATE logs.etl_run_log
       SET status = 'SUCCESS',
           ended_at = clock_timestamp(),
           duration_sec = extract(epoch FROM clock_timestamp() - started_at),
           total_rows_read = v_rows_read,
           total_rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id;
EXCEPTION
    WHEN OTHERS THEN
        v_error_message := left(SQLERRM, 4000);

        UPDATE logs.etl_table_log
           SET status = 'FAILED',
               ended_at = clock_timestamp(),
               error_message = v_error_message
         WHERE run_id = v_run_id
           AND table_name = 'dm.loan_holiday_info'
           AND status = 'STARTED';

        UPDATE logs.etl_run_log
           SET status = 'FAILED',
               ended_at = clock_timestamp(),
               duration_sec = extract(epoch FROM clock_timestamp() - started_at),
               error_message = v_error_message
         WHERE run_id = v_run_id;

        RAISE;
END;
$$;
