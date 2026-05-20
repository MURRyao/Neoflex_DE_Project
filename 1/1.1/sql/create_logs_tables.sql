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

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA logs TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA logs TO postgres;
