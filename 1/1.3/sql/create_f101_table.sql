CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS dm AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f (
    from_date date NOT NULL,
    to_date date NOT NULL,
    chapter varchar(1),
    ledger_account varchar(5) NOT NULL,
    characteristic varchar(1) NOT NULL,
    balance_in_rub numeric(23, 8),
    balance_in_val numeric(23, 8),
    balance_in_total numeric(23, 8),
    turn_deb_rub numeric(23, 8),
    turn_deb_val numeric(23, 8),
    turn_deb_total numeric(23, 8),
    turn_cre_rub numeric(23, 8),
    turn_cre_val numeric(23, 8),
    turn_cre_total numeric(23, 8),
    balance_out_rub numeric(23, 8),
    balance_out_val numeric(23, 8),
    balance_out_total numeric(23, 8),
    CONSTRAINT pk_dm_f101_round_f PRIMARY KEY (
        from_date,
        to_date,
        ledger_account,
        characteristic
    )
);


GRANT USAGE, CREATE ON SCHEMA dm TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA dm TO postgres;
