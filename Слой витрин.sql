CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE dm.dm_account_turnover_f(
	on_date DATE,
	account_rk INT,
	credit_amount NUMERIC(23, 8),
	credit_amount_rub NUMERIC(23, 8),
	debet_amount NUMERIC(23, 8),
	debet_amount_rub NUMERIC(23, 8)
);

CREATE TABLE dm.dm_account_balance_f(
	on_date date,
	account_rk INT,
	balance_out NUMERIC(23, 8),
	balance_out_rub NUMERIC(23, 8)
);

CREATE TABLE dm.dm_f101_round_f(
	from_date DATE,
	to_date DATE,
	chapter VARCHAR(1),
	ledger_account VARCHAR(5),
	characteristic VARCHAR(1),
	balance_in_rub NUMERIC(23, 8),
	balance_in_val NUMERIC(23, 8),
	balance_in_total NUMERIC(23, 8),
	turn_deb_rub NUMERIC(23, 8),
	turn_deb_val NUMERIC(23, 8),
	turn_deb_total NUMERIC(23, 8),
	turn_cre_rub NUMERIC(23, 8),
	turn_cre_val NUMERIC(23, 8),
	turn_cre_total NUMERIC(23, 8),
	balance_out_rub NUMERIC(23, 8),
	balance_out_val NUMERIC(23, 8),
	balance_out_total NUMERIC(23, 8)
);


CREATE TABLE dm.dm_f101_round_f_v2(
	from_date DATE,
	to_date DATE,
	chapter VARCHAR(1),
	ledger_account VARCHAR(5),
	characteristic VARCHAR(1),
	balance_in_rub NUMERIC(23, 8),
	balance_in_val NUMERIC(23, 8),
	balance_in_total NUMERIC(23, 8),
	turn_deb_rub NUMERIC(23, 8),
	turn_deb_val NUMERIC(23, 8),
	turn_deb_total NUMERIC(23, 8),
	turn_cre_rub NUMERIC(23, 8),
	turn_cre_val NUMERIC(23, 8),
	turn_cre_total NUMERIC(23, 8),
	balance_out_rub NUMERIC(23, 8),
	balance_out_val NUMERIC(23, 8),
	balance_out_total NUMERIC(23, 8)
);