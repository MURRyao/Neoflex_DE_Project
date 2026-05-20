SELECT count(*) AS source_rows_count
  FROM dm.dm_f101_round_f;

SELECT count(*) AS imported_rows_count
  FROM dm.dm_f101_round_f_v2;

SELECT from_date,
       to_date,
       chapter,
       ledger_account,
       characteristic,
       balance_in_total,
       turn_deb_total,
       balance_out_total
  FROM dm.dm_f101_round_f
 ORDER BY from_date, to_date, ledger_account, characteristic
 LIMIT 10;

SELECT from_date,
       to_date,
       chapter,
       ledger_account,
       characteristic,
       balance_in_total,
       turn_deb_total,
       balance_out_total
  FROM dm.dm_f101_round_f_v2
 ORDER BY from_date, to_date, ledger_account, characteristic
 LIMIT 10;

SELECT src.from_date,
       src.to_date,
       src.ledger_account,
       src.characteristic,
       src.balance_in_total AS source_balance_in_total,
       v2.balance_in_total AS imported_balance_in_total,
       src.turn_deb_total AS source_turn_deb_total,
       v2.turn_deb_total AS imported_turn_deb_total,
       src.balance_out_total AS source_balance_out_total,
       v2.balance_out_total AS imported_balance_out_total
  FROM dm.dm_f101_round_f src
  JOIN dm.dm_f101_round_f_v2 v2
    ON src.from_date = v2.from_date
   AND src.to_date = v2.to_date
   AND src.ledger_account = v2.ledger_account
   AND src.characteristic = v2.characteristic
 WHERE src.balance_in_total IS DISTINCT FROM v2.balance_in_total
    OR src.turn_deb_total IS DISTINCT FROM v2.turn_deb_total
    OR src.balance_out_total IS DISTINCT FROM v2.balance_out_total
 ORDER BY src.from_date, src.to_date, src.ledger_account, src.characteristic;

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
 WHERE dag_id IN ('python.export_f101_to_csv', 'python.import_f101_from_csv')
 ORDER BY started_at DESC
 LIMIT 10;

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
 WHERE table_name IN ('dm.dm_f101_round_f', 'dm.dm_f101_round_f_v2')
   AND source_file LIKE 'csv:%'
 ORDER BY started_at DESC
 LIMIT 10;
