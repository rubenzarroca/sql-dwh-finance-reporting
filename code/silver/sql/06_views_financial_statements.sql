/*
===========================================================================
DDL Script: Silver Layer Financial Statement Views
===========================================================================
Purpose:
  Create views for standard financial statements: Income Statement (P&L) 
  and Balance Sheet to provide easy access to financial reporting data.
===========================================================================
*/

-- Income Statement View (P&L)
DROP VIEW IF EXISTS silver.vw_income_statement;
CREATE VIEW silver.vw_income_statement AS
select
  fp.period_year,
  fp.period_quarter,
  fp.period_month,
  fp.period_name,
  ac.account_type,
  ac.account_group,
  ac.account_number,
  ac.account_name,
  ab.period_debit,
  ab.period_credit,
  case
    when ac.account_type::text = 'Income'::text then ab.period_credit - ab.period_debit
    when ac.account_type::text = 'Expense'::text then ab.period_debit - ab.period_credit
    else 0::numeric
  end as net_amount
from
  silver.account_balances ab
  join silver.accounts ac on ab.account_id::text = ac.account_id::text
  join silver.fiscal_periods fp on ab.period_id = fp.period_id
where
  ac.account_type::text = any (
    array[
      'Income'::character varying,
      'Expense'::character varying
    ]::text[]
  );

COMMENT ON VIEW silver.vw_income_statement IS 'Estado de resultados que muestra ingresos y gastos por período fiscal con cálculo de importes netos según tipo de cuenta';

-- Balance Sheet View
DROP VIEW IF EXISTS silver.vw_balance_sheet;
CREATE VIEW silver.vw_balance_sheet AS
select
  fp.period_year,
  fp.period_quarter,
  fp.period_month,
  fp.period_name,
  ac.account_type,
  ac.account_group,
  ac.account_number,
  ac.account_name,
  ab.end_balance,
  case
    when ac.account_type::text = any (
      array[
        'Asset'::character varying,
        'Expense'::character varying
      ]::text[]
    ) then case
      when ab.end_balance >= 0::numeric then ab.end_balance
      else 0::numeric
    end
    else case
      when ab.end_balance < 0::numeric then - ab.end_balance
      else 0::numeric
    end
  end as debit_balance,
  case
    when ac.account_type::text = any (
      array[
        'Liability'::character varying,
        'Equity'::character varying,
        'Income'::character varying
      ]::text[]
    ) then case
      when ab.end_balance >= 0::numeric then ab.end_balance
      else 0::numeric
    end
    else case
      when ab.end_balance < 0::numeric then - ab.end_balance
      else 0::numeric
    end
  end as credit_balance
from
  silver.account_balances ab
  join silver.accounts ac on ab.account_id::text = ac.account_id::text
  join silver.fiscal_periods fp on ab.period_id = fp.period_id
where
  ac.account_type::text = any (
    array[
      'Asset'::character varying,
      'Liability'::character varying,
      'Equity'::character varying
    ]::text[]
  );

COMMENT ON VIEW silver.vw_balance_sheet IS 'Balance general mostrando activos, pasivos y patrimonio con sus saldos finales clasificados como débito o crédito según la naturaleza de la cuenta';