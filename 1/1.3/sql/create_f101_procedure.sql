CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_ondate date)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id uuid := gen_random_uuid();
    v_from_date date := date_trunc('month', i_ondate - INTERVAL '1 month')::date;
    v_to_date date := (date_trunc('month', i_ondate)::date - 1);
    v_rows_read bigint := 0;
    v_rows_loaded bigint := 0;
    v_error_message text;
BEGIN
    INSERT INTO logs.etl_run_log (
        run_id, dag_id, run_type, status, started_at
    )
    VALUES (
        v_run_id, 'dm.fill_f101_round_f', 'procedure', 'STARTED', clock_timestamp()
    );

    INSERT INTO logs.etl_table_log (
        run_id, table_name, source_file, load_mode, status, started_at
    )
    VALUES (
        v_run_id,
        'dm.dm_f101_round_f',
        'procedure:dm.fill_f101_round_f',
        'refresh_by_period',
        'STARTED',
        clock_timestamp()
    );

    WITH active_accounts AS (
        SELECT DISTINCT ON (a.account_rk)
               a.account_rk
          FROM ds.md_account_d a
         WHERE a.data_actual_date <= v_to_date
           AND coalesce(a.data_actual_end_date, DATE '5999-12-31') >= v_from_date
         ORDER BY a.account_rk, a.data_actual_date DESC
    )
    SELECT count(*)
      INTO v_rows_read
      FROM active_accounts;

    DELETE FROM dm.dm_f101_round_f
     WHERE from_date = v_from_date
       AND to_date = v_to_date;

    WITH active_accounts AS (
        SELECT DISTINCT ON (a.account_rk)
               a.account_rk,
               substring(a.account_number::text FROM 1 FOR 5) AS ledger_account,
               trim(a.char_type) AS characteristic,
               trim(a.currency_code) AS currency_code
          FROM ds.md_account_d a
         WHERE a.data_actual_date <= v_to_date
           AND coalesce(a.data_actual_end_date, DATE '5999-12-31') >= v_from_date
           AND a.account_number IS NOT NULL
         ORDER BY a.account_rk, a.data_actual_date DESC
    ),
    account_base AS (
        SELECT a.account_rk,
               a.ledger_account,
               coalesce(l.chapter, '') AS chapter,
               a.characteristic,
               CASE
                   WHEN a.currency_code IN ('810', '643') THEN TRUE
                   ELSE FALSE
               END AS is_rub
          FROM active_accounts a
          LEFT JOIN LATERAL (
                SELECT ls.chapter
                  FROM ds.md_ledger_account_s ls
                 WHERE ls.ledger_account::text = a.ledger_account
                   AND v_to_date BETWEEN ls.start_date
                                     AND coalesce(ls.end_date, DATE '5999-12-31')
                 ORDER BY ls.start_date DESC
                 LIMIT 1
          ) l ON TRUE
         WHERE a.ledger_account IS NOT NULL
           AND length(a.ledger_account) = 5
           AND a.characteristic IS NOT NULL
    ),
    monthly_turnover AS (
        SELECT t.account_rk,
               sum(coalesce(t.debet_amount_rub, 0)) AS debet_amount_rub,
               sum(coalesce(t.credit_amount_rub, 0)) AS credit_amount_rub
          FROM dm.dm_account_turnover_f t
         WHERE t.on_date BETWEEN v_from_date AND v_to_date
         GROUP BY t.account_rk
    )
    INSERT INTO dm.dm_f101_round_f (
        from_date,
        to_date,
        chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    )
    SELECT v_from_date AS from_date,
           v_to_date AS to_date,
           nullif(ab.chapter, '') AS chapter,
           ab.ledger_account,
           ab.characteristic,
           sum(CASE WHEN ab.is_rub THEN coalesce(b_in.balance_out_rub, 0) ELSE 0 END) AS balance_in_rub,
           sum(CASE WHEN NOT ab.is_rub THEN coalesce(b_in.balance_out_rub, 0) ELSE 0 END) AS balance_in_val,
           sum(coalesce(b_in.balance_out_rub, 0)) AS balance_in_total,
           sum(CASE WHEN ab.is_rub THEN coalesce(t.debet_amount_rub, 0) ELSE 0 END) AS turn_deb_rub,
           sum(CASE WHEN NOT ab.is_rub THEN coalesce(t.debet_amount_rub, 0) ELSE 0 END) AS turn_deb_val,
           sum(coalesce(t.debet_amount_rub, 0)) AS turn_deb_total,
           sum(CASE WHEN ab.is_rub THEN coalesce(t.credit_amount_rub, 0) ELSE 0 END) AS turn_cre_rub,
           sum(CASE WHEN NOT ab.is_rub THEN coalesce(t.credit_amount_rub, 0) ELSE 0 END) AS turn_cre_val,
           sum(coalesce(t.credit_amount_rub, 0)) AS turn_cre_total,
           sum(CASE WHEN ab.is_rub THEN coalesce(b_out.balance_out_rub, 0) ELSE 0 END) AS balance_out_rub,
           sum(CASE WHEN NOT ab.is_rub THEN coalesce(b_out.balance_out_rub, 0) ELSE 0 END) AS balance_out_val,
           sum(coalesce(b_out.balance_out_rub, 0)) AS balance_out_total
      FROM account_base ab
      LEFT JOIN dm.dm_account_balance_f b_in
        ON b_in.account_rk = ab.account_rk
       AND b_in.on_date = v_from_date - 1
      LEFT JOIN monthly_turnover t
        ON t.account_rk = ab.account_rk
      LEFT JOIN dm.dm_account_balance_f b_out
        ON b_out.account_rk = ab.account_rk
       AND b_out.on_date = v_to_date
     GROUP BY ab.chapter,
              ab.ledger_account,
              ab.characteristic;

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    UPDATE logs.etl_table_log
       SET status = 'SUCCESS',
           ended_at = clock_timestamp(),
           rows_read = v_rows_read,
           rows_loaded = v_rows_loaded
     WHERE run_id = v_run_id
       AND table_name = 'dm.dm_f101_round_f'
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
           AND table_name = 'dm.dm_f101_round_f'
           AND status = 'STARTED';

        UPDATE logs.etl_run_log
           SET status = 'FAILED',
               ended_at = clock_timestamp(),
               duration_sec = extract(epoch FROM clock_timestamp() - started_at),
               error_message = v_error_message
         WHERE run_id = v_run_id;

        RAISE WARNING 'dm.fill_f101_round_f(%) failed: %', i_ondate, v_error_message;
END;
$$;

GRANT EXECUTE ON PROCEDURE dm.fill_f101_round_f(date) TO postgres;
