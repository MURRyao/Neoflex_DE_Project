CREATE EXTENSION IF NOT EXISTS pgcrypto;
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


SELECT
    curr.account_rk,
    curr.effective_date,
    curr.account_in_sum AS wrong_account_in_sum,
    prev.account_out_sum AS correct_account_in_sum,
    prev.effective_date AS previous_effective_date
FROM rd.account_balance AS curr
JOIN rd.account_balance AS prev
  ON prev.account_rk = curr.account_rk
 AND prev.effective_date = curr.effective_date - 1
WHERE curr.account_in_sum IS DISTINCT FROM prev.account_out_sum
ORDER BY
    curr.account_rk,
    curr.effective_date;


SELECT
    prev.account_rk,
    prev.effective_date,
    prev.account_out_sum AS wrong_account_out_sum,
    curr.account_in_sum AS correct_account_out_sum,
    curr.effective_date AS next_effective_date
FROM rd.account_balance AS prev
JOIN rd.account_balance AS curr
  ON curr.account_rk = prev.account_rk
 AND curr.effective_date = prev.effective_date + 1
WHERE prev.account_out_sum IS DISTINCT FROM curr.account_in_sum
ORDER BY
    prev.account_rk,
    prev.effective_date;


WITH fixes AS (
    SELECT
        curr.account_rk,
        curr.effective_date,
        prev.account_out_sum AS correct_account_in_sum
    FROM rd.account_balance AS curr
    JOIN rd.account_balance AS prev
      ON prev.account_rk = curr.account_rk
     AND prev.effective_date = curr.effective_date - 1
    WHERE curr.account_in_sum IS DISTINCT FROM prev.account_out_sum
),
updated AS (
    UPDATE rd.account_balance AS ab
       SET account_in_sum = f.correct_account_in_sum
      FROM fixes AS f
     WHERE ab.account_rk = f.account_rk
       AND ab.effective_date = f.effective_date
    RETURNING ab.account_rk
)
SELECT count(*) AS updated_rows
FROM updated;


SELECT count(*) AS mismatched_rows_after_fix
FROM rd.account_balance AS curr
JOIN rd.account_balance AS prev
  ON prev.account_rk = curr.account_rk
 AND prev.effective_date = curr.effective_date - 1
WHERE curr.account_in_sum IS DISTINCT FROM prev.account_out_sum;

DROP PROCEDURE IF EXISTS dm.reload_account_balance_turnover();
DROP PROCEDURE IF EXISTS dm.reload_account_balance_turnover(date, date);

CREATE OR REPLACE PROCEDURE dm.reload_account_balance_turnover()
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
        v_run_id, 'dm.reload_account_balance_turnover', 'procedure', 'STARTED', clock_timestamp()
    );

    INSERT INTO logs.etl_table_log (
        run_id, table_name, source_file, load_mode, status, started_at
    )
    VALUES (
        v_run_id,
        'dm.account_balance_turnover',
        'procedure:dm.reload_account_balance_turnover',
        'full_reload',
        'STARTED',
        clock_timestamp()
    );

    SELECT count(*)
      INTO v_rows_read
      FROM rd.account AS a
      LEFT JOIN rd.account_balance AS ab
        ON ab.account_rk = a.account_rk
      LEFT JOIN dm.dict_currency AS dc
        ON dc.currency_cd = a.currency_cd
     WHERE ab.effective_date IS NOT NULL;

    TRUNCATE TABLE dm.account_balance_turnover;

    INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
    SELECT
        a.account_rk,
        coalesce(dc.currency_name, '-1') AS currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    FROM rd.account AS a
    LEFT JOIN rd.account_balance AS ab
      ON ab.account_rk = a.account_rk
    LEFT JOIN dm.dict_currency AS dc
      ON dc.currency_cd = a.currency_cd
    WHERE ab.effective_date IS NOT NULL;

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    UPDATE logs.etl_table_log
       SET status = 'SUCCESS',
           ended_at = clock_timestamp(),
           rows_read = v_rows_read,
           rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id
       AND table_name = 'dm.account_balance_turnover'
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
           AND table_name = 'dm.account_balance_turnover'
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

