SELECT from_date,
       to_date,
       count(*) AS rows_count
  FROM dm.dm_f101_round_f
 GROUP BY from_date, to_date
 ORDER BY from_date, to_date;

SELECT from_date,
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
  FROM dm.dm_f101_round_f
 WHERE from_date = DATE '2018-01-01'
   AND to_date = DATE '2018-01-31'
 ORDER BY ledger_account, characteristic
 LIMIT 30;

SELECT dag_id,
       run_type,
       status,
       started_at,
       ended_at,
       duration_sec,
       total_rows_read,
       total_rows_loaded,
       error_message
  FROM logs.etl_run_log
 WHERE dag_id = 'dm.fill_f101_round_f'
 ORDER BY started_at DESC
 LIMIT 5;

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
 WHERE table_name = 'dm.dm_f101_round_f'
 ORDER BY started_at DESC
 LIMIT 5;


CALL dm.fill_f101_round_f(DATE '2018-02-01');

SELECT from_date,
       to_date,
       count(*) AS rows_count
  FROM dm.dm_f101_round_f
 WHERE from_date = DATE '2018-01-01'
   AND to_date = DATE '2018-01-31'
 GROUP BY from_date, to_date;
