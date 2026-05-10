CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f (i_on_date DATE)
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
    SELECT
        i_on_date,
        COALESCE(t1.cr_ac_rk, t2.db_ac_rk) as account_rk,
        COALESCE(t1.cr_am, 0) as credit_amount,
        COALESCE(t1.cr_am, 0) * COALESCE(merd.reduced_cource, 1) as credit_amount_rub,
        COALESCE(t2.db_am, 0) as debet_amount,
        COALESCE(t2.db_am, 0) * COALESCE(merd.reduced_cource, 1) as debet_amount_rub
    FROM 
        (SELECT 
            credit_account_rk as cr_ac_rk, 
            SUM(credit_amount) as cr_am 
         FROM ds.ft_posting_f 
         WHERE oper_date = i_on_date 
         GROUP BY credit_account_rk) t1
    FULL JOIN 
        (SELECT 
            debet_account_rk as db_ac_rk, 
            SUM(debet_amount) as db_am 
         FROM ds.ft_posting_f 
         WHERE oper_date = i_on_date 
         GROUP BY debet_account_rk) t2
    ON t1.cr_ac_rk = t2.db_ac_rk
    LEFT JOIN ds.md_account_d mad 
        ON COALESCE(t1.cr_ac_rk, t2.db_ac_rk) = mad.account_rk 
        AND i_on_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date
    LEFT JOIN ds.md_exchange_rate_d merd 
        ON mad.currency_rk = merd.currency_rk 
        AND i_on_date BETWEEN merd.data_actual_date AND merd.data_actual_end_date;
END;
$$;



CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f (i_on_date DATE)
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
				WHEN mad.char_type = 'А' THEN COALESCE(dabf.balance_out, 0) + COALESCE(datf.debet_amount, 0) - COALESCE(datf.credit_amount, 0)
				WHEN mad.char_type = 'П' THEN COALESCE(dabf.balance_out, 0) - COALESCE(datf.debet_amount, 0) + COALESCE(datf.credit_amount, 0)
			END balance_out,
			CASE
				WHEN mad.char_type = 'А' THEN COALESCE(dabf.balance_out_rub, 0) + COALESCE(datf.debet_amount_rub, 0) - COALESCE(datf.credit_amount_rub, 0)
				WHEN mad.char_type = 'П' THEN COALESCE(dabf.balance_out_rub, 0) - COALESCE(datf.debet_amount_rub, 0) + COALESCE(datf.credit_amount_rub, 0)
			END as balance_out_rub
		FROM ds.md_account_d mad
		LEFT JOIN dm.dm_account_balance_f dabf ON mad.account_rk = dabf.account_rk
		AND dabf.on_date = (i_on_date - INTERVAL '1 day')
		LEFT JOIN dm.dm_account_turnover_f datf ON mad.account_rk = datf.account_rk AND datf.on_date = i_on_date
		WHERE i_on_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date;
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

