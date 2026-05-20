CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS dm AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f (
    on_date date NOT NULL,
    account_rk numeric(20, 0) NOT NULL,
    credit_amount numeric(23, 8),
    credit_amount_rub numeric(23, 8),
    debet_amount numeric(23, 8),
    debet_amount_rub numeric(23, 8),
    CONSTRAINT pk_dm_account_turnover_f PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f (
    on_date date NOT NULL,
    account_rk numeric(20, 0) NOT NULL,
    balance_out numeric(23, 8),
    balance_out_rub numeric(23, 8),
    CONSTRAINT pk_dm_account_balance_f PRIMARY KEY (on_date, account_rk)
);

COMMENT ON TABLE dm.dm_account_turnover_f IS 'DM: daily account turnovers';
COMMENT ON TABLE dm.dm_account_balance_f IS 'DM: daily account balances';

GRANT USAGE, CREATE ON SCHEMA dm TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA dm TO postgres;
