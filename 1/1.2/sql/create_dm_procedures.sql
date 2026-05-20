CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_ondate date)
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
        v_run_id, 'ds.fill_account_turnover_f', 'procedure', 'STARTED', now()
    );

    INSERT INTO logs.etl_table_log (
        run_id, table_name, source_file, load_mode, status, started_at
    )
    VALUES (
        v_run_id,
        'dm.dm_account_turnover_f',
        'procedure:ds.fill_account_turnover_f',
        'refresh_by_date',
        'STARTED',
        now()
    );

    SELECT count(*)
      INTO v_rows_read
      FROM ds.ft_posting_f
     WHERE oper_date = i_ondate;

    DELETE FROM dm.dm_account_turnover_f
     WHERE on_date = i_ondate;

    WITH credit_turnover AS (
        SELECT credit_account_rk AS account_rk,
               sum(coalesce(credit_amount, 0)) AS credit_amount
          FROM ds.ft_posting_f
         WHERE oper_date = i_ondate
           AND credit_account_rk IS NOT NULL
         GROUP BY credit_account_rk
    ),
    debet_turnover AS (
        SELECT debet_account_rk AS account_rk,
               sum(coalesce(debet_amount, 0)) AS debet_amount
          FROM ds.ft_posting_f
         WHERE oper_date = i_ondate
           AND debet_account_rk IS NOT NULL
         GROUP BY debet_account_rk
    ),
    turnover AS (
        SELECT coalesce(c.account_rk, d.account_rk) AS account_rk,
               coalesce(c.credit_amount, 0) AS credit_amount,
               coalesce(d.debet_amount, 0) AS debet_amount
          FROM credit_turnover c
          FULL JOIN debet_turnover d
            ON d.account_rk = c.account_rk
    )
    INSERT INTO dm.dm_account_turnover_f (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    SELECT i_ondate AS on_date,
           t.account_rk,
           t.credit_amount,
           t.credit_amount * coalesce(er.reduced_cource, 1) AS credit_amount_rub,
           t.debet_amount,
           t.debet_amount * coalesce(er.reduced_cource, 1) AS debet_amount_rub
      FROM turnover t
      LEFT JOIN LATERAL (
            SELECT a.currency_rk
              FROM ds.md_account_d a
             WHERE a.account_rk = t.account_rk
               AND i_ondate BETWEEN a.data_actual_date
                               AND coalesce(a.data_actual_end_date, DATE '5999-12-31')
             ORDER BY a.data_actual_date DESC
             LIMIT 1
      ) acc ON TRUE
      LEFT JOIN LATERAL (
            SELECT r.reduced_cource
              FROM ds.md_exchange_rate_d r
             WHERE r.currency_rk = acc.currency_rk
               AND i_ondate BETWEEN r.data_actual_date
                               AND coalesce(r.data_actual_end_date, DATE '5999-12-31')
             ORDER BY r.data_actual_date DESC
             LIMIT 1
      ) er ON TRUE;

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    UPDATE logs.etl_table_log
       SET status = 'SUCCESS',
           ended_at = now(),
           rows_read = v_rows_read,
           rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id
       AND table_name = 'dm.dm_account_turnover_f'
       AND status = 'STARTED';

    UPDATE logs.etl_run_log
       SET status = 'SUCCESS',
           ended_at = now(),
           duration_sec = extract(epoch FROM now() - started_at),
           total_rows_read = v_rows_read,
           total_rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id;
EXCEPTION
    WHEN OTHERS THEN
        v_error_message := left(SQLERRM, 4000);

        UPDATE logs.etl_table_log
           SET status = 'FAILED',
               ended_at = now(),
               error_message = v_error_message
         WHERE run_id = v_run_id
           AND table_name = 'dm.dm_account_turnover_f'
           AND status = 'STARTED';

        UPDATE logs.etl_run_log
           SET status = 'FAILED',
               ended_at = now(),
               duration_sec = extract(epoch FROM now() - started_at),
               error_message = v_error_message
         WHERE run_id = v_run_id;

        RAISE WARNING 'ds.fill_account_turnover_f(%) failed: %', i_ondate, v_error_message;
END;
$$;

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_ondate date)
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
        v_run_id, 'ds.fill_account_balance_f', 'procedure', 'STARTED', now()
    );

    INSERT INTO logs.etl_table_log (
        run_id, table_name, source_file, load_mode, status, started_at
    )
    VALUES (
        v_run_id,
        'dm.dm_account_balance_f',
        'procedure:ds.fill_account_balance_f',
        'refresh_by_date',
        'STARTED',
        now()
    );

    WITH active_accounts AS (
        SELECT DISTINCT ON (a.account_rk)
               a.account_rk,
               a.char_type
          FROM ds.md_account_d a
         WHERE i_ondate BETWEEN a.data_actual_date
                           AND coalesce(a.data_actual_end_date, DATE '5999-12-31')
         ORDER BY a.account_rk, a.data_actual_date DESC
    )
    SELECT count(*)
      INTO v_rows_read
      FROM active_accounts;

    DELETE FROM dm.dm_account_balance_f
     WHERE on_date = i_ondate;

    WITH active_accounts AS (
        SELECT DISTINCT ON (a.account_rk)
               a.account_rk,
               a.char_type
          FROM ds.md_account_d a
         WHERE i_ondate BETWEEN a.data_actual_date
                           AND coalesce(a.data_actual_end_date, DATE '5999-12-31')
         ORDER BY a.account_rk, a.data_actual_date DESC
    )
    INSERT INTO dm.dm_account_balance_f (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    SELECT i_ondate AS on_date,
           a.account_rk,
           CASE
               WHEN a.char_type IN ('П', 'P') THEN
                   coalesce(prev.balance_out, 0)
                   - coalesce(turn.debet_amount, 0)
                   + coalesce(turn.credit_amount, 0)
               ELSE
                   coalesce(prev.balance_out, 0)
                   + coalesce(turn.debet_amount, 0)
                   - coalesce(turn.credit_amount, 0)
           END AS balance_out,
           CASE
               WHEN a.char_type IN ('П', 'P') THEN
                   coalesce(prev.balance_out_rub, 0)
                   - coalesce(turn.debet_amount_rub, 0)
                   + coalesce(turn.credit_amount_rub, 0)
               ELSE
                   coalesce(prev.balance_out_rub, 0)
                   + coalesce(turn.debet_amount_rub, 0)
                   - coalesce(turn.credit_amount_rub, 0)
           END AS balance_out_rub
      FROM active_accounts a
      LEFT JOIN dm.dm_account_balance_f prev
        ON prev.on_date = i_ondate - 1
       AND prev.account_rk = a.account_rk
      LEFT JOIN dm.dm_account_turnover_f turn
        ON turn.on_date = i_ondate
       AND turn.account_rk = a.account_rk;

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    UPDATE logs.etl_table_log
       SET status = 'SUCCESS',
           ended_at = now(),
           rows_read = v_rows_read,
           rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id
       AND table_name = 'dm.dm_account_balance_f'
       AND status = 'STARTED';

    UPDATE logs.etl_run_log
       SET status = 'SUCCESS',
           ended_at = now(),
           duration_sec = extract(epoch FROM now() - started_at),
           total_rows_read = v_rows_read,
           total_rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id;
EXCEPTION
    WHEN OTHERS THEN
        v_error_message := left(SQLERRM, 4000);

        UPDATE logs.etl_table_log
           SET status = 'FAILED',
               ended_at = now(),
               error_message = v_error_message
         WHERE run_id = v_run_id
           AND table_name = 'dm.dm_account_balance_f'
           AND status = 'STARTED';

        UPDATE logs.etl_run_log
           SET status = 'FAILED',
               ended_at = now(),
               duration_sec = extract(epoch FROM now() - started_at),
               error_message = v_error_message
         WHERE run_id = v_run_id;

        RAISE WARNING 'ds.fill_account_balance_f(%) failed: %', i_ondate, v_error_message;
END;
$$;

GRANT EXECUTE ON PROCEDURE ds.fill_account_turnover_f(date) TO postgres;
GRANT EXECUTE ON PROCEDURE ds.fill_account_balance_f(date) TO postgres;
