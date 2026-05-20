CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    on_date date NOT NULL,
    account_rk numeric(20, 0) NOT NULL,
    currency_rk numeric(20, 0),
    balance_out numeric(23, 8),
    CONSTRAINT pk_ft_balance_f PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
    oper_date date NOT NULL,
    credit_account_rk numeric(20, 0),
    debet_account_rk numeric(20, 0),
    credit_amount numeric(23, 8),
    debet_amount numeric(23, 8)
);

CREATE TABLE IF NOT EXISTS ds.md_account_d (
    data_actual_date date NOT NULL,
    data_actual_end_date date,
    account_rk numeric(20, 0) NOT NULL,
    account_number numeric(25, 0),
    char_type varchar(1),
    currency_rk numeric(20, 0),
    currency_code varchar(3),
    CONSTRAINT pk_md_account_d PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    currency_rk numeric(20, 0) NOT NULL,
    data_actual_date date NOT NULL,
    data_actual_end_date date,
    currency_code varchar(3),
    code_iso_char varchar(3),
    CONSTRAINT pk_md_currency_d PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
    data_actual_date date NOT NULL,
    data_actual_end_date date,
    currency_rk numeric(20, 0) NOT NULL,
    reduced_cource numeric(23, 8),
    code_iso_num varchar(3),
    CONSTRAINT pk_md_exchange_rate_d PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
    chapter varchar(1),
    chapter_name varchar(255),
    section_number integer,
    section_name varchar(255),
    subsection_name varchar(255),
    ledger1_account integer,
    ledger1_account_name varchar(255),
    ledger_account integer NOT NULL,
    ledger_account_name varchar(255),
    characteristic varchar(1),
    start_date date NOT NULL,
    end_date date,
    CONSTRAINT pk_md_ledger_account_s PRIMARY KEY (ledger_account, start_date)
);

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ds TO postgres;
