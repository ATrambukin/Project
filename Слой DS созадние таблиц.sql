CREATE SCHEMA IF NOT EXISTS ds;

CREATE TABLE IF NOT EXISTS ds.ft_balance_f(
	on_date DATE NOT NULL,
	account_rk INT NOT NULL,
	currency_rk INT,
	balance_out NUMERIC (15,2),
	
	PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f(
	oper_date DATE NOT NULL,
	credit_account_rk INT NOT NULL,
	debet_account_rk INT,
	credit_amount NUMERIC (15,2),
	debet_amount NUMERIC (15,2)
);

CREATE TYPE account_type AS ENUM ('А', 'П');

CREATE TABLE IF NOT EXISTS ds.md_account_d(
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE NOT NULL,
	account_rk INT NOT NULL,
	account_number VARCHAR(20),
	char_type VARCHAR(1),
	currency_rk INT NOT NULL,
	currency_code VARCHAR(3) NOT NULL,
	
	PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d(
	currency_rk INT NOT NULL,
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_code VARCHAR(3),
	code_iso_char VARCHAR(3),
	
	PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d(
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_rk INT NOT NULL,
	reduced_cource NUMERIC,
	code_iso_num VARCHAR(3),
	
	PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s(
	chapter VARCHAR(1),
	chapter_name VARCHAR(16),
	section_number INT,
	section_name VARCHAR(22),
	subsection_name VARCHAR(21),
	ledger1_account INT,
	ledger1_account_name VARCHAR(47),
	ledger_account INT NOT NULL,
	ledger_account_name VARCHAR(155),
	characteristic VARCHAR(1), 
	start_date DATE NOT NULL,
	end_date date,
	
	PRIMARY KEY (ledger_account, start_date)
);











