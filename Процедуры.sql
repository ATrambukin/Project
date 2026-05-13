CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_on_date date)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_on_date;

    INSERT INTO dm.dm_account_turnover_f (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    WITH
    credit_turnover AS (
        SELECT
            f.credit_account_rk AS account_rk,
            SUM(f.credit_amount) AS credit_amount,
            SUM(f.credit_amount * COALESCE(er.reduced_cource, 1)) AS credit_amount_rub
        FROM ds.ft_posting_f f
        LEFT JOIN ds.md_account_d a
            ON f.credit_account_rk = a.account_rk
            AND i_on_date BETWEEN a.data_actual_date AND a.data_actual_end_date
        LEFT JOIN ds.md_exchange_rate_d er
            ON a.currency_rk = er.currency_rk
            AND i_on_date BETWEEN er.data_actual_date AND er.data_actual_end_date
        WHERE f.oper_date = i_on_date
        GROUP BY f.credit_account_rk
    ),
    debet_turnover AS (
        SELECT
            f.debet_account_rk AS account_rk,
            SUM(f.debet_amount) AS debet_amount,
            SUM(f.debet_amount * COALESCE(er.reduced_cource, 1)) AS debet_amount_rub
        FROM ds.ft_posting_f f
        LEFT JOIN ds.md_account_d a
            ON f.debet_account_rk = a.account_rk
            AND i_on_date BETWEEN a.data_actual_date AND a.data_actual_end_date
        LEFT JOIN ds.md_exchange_rate_d er
            ON a.currency_rk = er.currency_rk
            AND i_on_date BETWEEN er.data_actual_date AND er.data_actual_end_date
        WHERE f.oper_date = i_on_date
        GROUP BY f.debet_account_rk
    )
    SELECT
        i_on_date,
        COALESCE(ct.account_rk, dt.account_rk) AS account_rk,
        COALESCE(ct.credit_amount, 0),
        COALESCE(ct.credit_amount_rub, 0),
        COALESCE(dt.debet_amount, 0),
        COALESCE(dt.debet_amount_rub, 0)
    FROM credit_turnover ct
    FULL JOIN debet_turnover dt ON ct.account_rk = dt.account_rk;
END;
$$;


CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_on_date date)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_on_date;

    INSERT INTO dm.dm_account_balance_f (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    SELECT
        i_on_date,
        mad.account_rk,
        CASE
            WHEN mad.char_type = 'А' THEN COALESCE(prev.balance_out, 0) + COALESCE(turn.debet_amount, 0) - COALESCE(turn.credit_amount, 0)
            WHEN mad.char_type = 'П' THEN COALESCE(prev.balance_out, 0) - COALESCE(turn.debet_amount, 0) + COALESCE(turn.credit_amount, 0)
        END as balance_out,
        (CASE
            WHEN mad.char_type = 'А' THEN COALESCE(prev.balance_out, 0) + COALESCE(turn.debet_amount, 0) - COALESCE(turn.credit_amount, 0)
            WHEN mad.char_type = 'П' THEN COALESCE(prev.balance_out, 0) - COALESCE(turn.debet_amount, 0) + COALESCE(turn.credit_amount, 0)
        END) * COALESCE(cur.reduced_cource, 1) as balance_out_rub

    FROM ds.md_account_d mad
    LEFT JOIN dm.dm_account_balance_f prev
        ON mad.account_rk = prev.account_rk
        AND prev.on_date = (i_on_date - INTERVAL '1 day')
    LEFT JOIN dm.dm_account_turnover_f turn
        ON mad.account_rk = turn.account_rk
        AND turn.on_date = i_on_date
    LEFT JOIN ds.md_exchange_rate_d cur
        ON mad.currency_rk = cur.currency_rk
        AND i_on_date BETWEEN cur.data_actual_date AND cur.data_actual_end_date
    WHERE i_on_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date
      AND (prev.balance_out IS NOT NULL OR turn.account_rk IS NOT NULL);
END;
$$;


CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_on_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	v_start_date DATE := date_trunc('month', i_on_date - INTERVAL '1 day');
	v_end_date DATE := (i_on_date  - INTERVAL '1 day');
	v_out_date DATE := (v_start_date - INTERVAL '1 day');
BEGIN

	DELETE FROM dm.dm_f101_round_f WHERE from_date = v_start_date AND to_date = v_end_date;

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
	SELECT
		v_start_date,
		v_end_date,
		mlas.chapter,
		substr(mad.account_number, 1, 5) as ledger_account,
		mad.char_type,
		COALESCE(SUM(CASE WHEN mad.currency_code IN ('810', '643') THEN dabf_in.balance_out_rub ELSE 0 END), 0) as balance_in_rub,
        COALESCE(SUM(CASE WHEN mad.currency_code NOT IN ('810', '643') THEN dabf_in.balance_out_rub ELSE 0 END), 0) as balance_in_val,
        COALESCE(SUM(dabf_in.balance_out_rub), 0) as balance_in_total,
        COALESCE(SUM(CASE WHEN mad.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END), 0) as turn_deb_rub,
        COALESCE(SUM(CASE WHEN mad.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END), 0) as turn_deb_val,
        COALESCE(SUM(t.debet_amount_rub), 0) as turn_deb_total,
        COALESCE(SUM(CASE WHEN mad.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END), 0) as turn_cre_rub,
        COALESCE(SUM(CASE WHEN mad.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END), 0) as turn_cre_val,
        COALESCE(SUM(t.credit_amount_rub), 0) as turn_cre_total,
        COALESCE(SUM(CASE WHEN mad.currency_code IN ('810', '643') THEN dabf_out.balance_out_rub ELSE 0 END), 0) as balance_out_rub,
        COALESCE(SUM(CASE WHEN mad.currency_code NOT IN ('810', '643') THEN dabf_out.balance_out_rub ELSE 0 END), 0) as balance_out_val,
        COALESCE(SUM(dabf_out.balance_out_rub), 0) as balance_out_total
        FROM ds.md_account_d mad
        JOIN ds.md_ledger_account_s mlas ON substr(mad.account_number, 1, 5) = mlas.ledger_account::text
		LEFT JOIN dm.dm_account_balance_f dabf_in
        ON mad.account_rk = dabf_in.account_rk AND dabf_in.on_date = v_out_date
        LEFT JOIN dm.dm_account_balance_f dabf_out
        ON mad.account_rk = dabf_out.account_rk AND dabf_out.on_date = v_end_date
        LEFT JOIN (
        SELECT account_rk,
               SUM(debet_amount_rub) as debet_amount_rub,
               SUM(credit_amount_rub) as credit_amount_rub
        FROM dm.dm_account_turnover_f
        WHERE on_date BETWEEN v_start_date AND v_end_date
        GROUP BY account_rk
        ) t ON mad.account_rk = t.account_rk
        WHERE v_end_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date
    	GROUP BY mlas.chapter, substr(mad.account_number, 1, 5), mad.char_type;
	END;
$$;



