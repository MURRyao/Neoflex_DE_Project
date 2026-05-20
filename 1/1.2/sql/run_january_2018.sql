DO $$
DECLARE
    v_run_id uuid := gen_random_uuid();
    v_rows_read bigint := 0;
    v_rows_loaded bigint := 0;
BEGIN
    INSERT INTO logs.etl_run_log (
        run_id, dag_id, run_type, status, started_at
    )
    VALUES (
        v_run_id, 'dm.seed_account_balance_2017_12_31', 'script', 'STARTED', now()
    );

    INSERT INTO logs.etl_table_log (
        run_id, table_name, source_file, load_mode, status, started_at
    )
    VALUES (
        v_run_id,
        'dm.dm_account_balance_f',
        'script:1.2/sql/run_january_2018.sql',
        'seed_2017_12_31',
        'STARTED',
        now()
    );

    SELECT count(*)
      INTO v_rows_read
      FROM ds.ft_balance_f
     WHERE on_date = DATE '2017-12-31';

    DELETE FROM dm.dm_account_balance_f
     WHERE on_date = DATE '2017-12-31';

    INSERT INTO dm.dm_account_balance_f (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    SELECT b.on_date,
           b.account_rk,
           b.balance_out,
           b.balance_out * coalesce(er.reduced_cource, 1) AS balance_out_rub
      FROM ds.ft_balance_f b
      LEFT JOIN LATERAL (
            SELECT r.reduced_cource
              FROM ds.md_exchange_rate_d r
             WHERE r.currency_rk = b.currency_rk
               AND DATE '2017-12-31' BETWEEN r.data_actual_date
                                      AND coalesce(r.data_actual_end_date, DATE '5999-12-31')
             ORDER BY r.data_actual_date DESC
             LIMIT 1
      ) er ON TRUE
     WHERE b.on_date = DATE '2017-12-31';

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    UPDATE logs.etl_table_log
       SET status = 'SUCCESS',
           ended_at = now(),
           rows_read = v_rows_read,
           rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id
       AND status = 'STARTED';

    UPDATE logs.etl_run_log
       SET status = 'SUCCESS',
           ended_at = now(),
           duration_sec = extract(epoch FROM now() - started_at),
           total_rows_read = v_rows_read,
           total_rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id;
END;
$$;

DO $$
DECLARE
    v_date date := DATE '2018-01-01';
BEGIN
    WHILE v_date <= DATE '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(v_date);
        v_date := v_date + 1;
    END LOOP;
END;
$$;

DO $$
DECLARE
    v_date date := DATE '2018-01-01';
BEGIN
    WHILE v_date <= DATE '2018-01-31' LOOP
        CALL ds.fill_account_balance_f(v_date);
        v_date := v_date + 1;
    END LOOP;
END;
$$;
