create or replace table ctc_simulation_income_brackets as (

-- run 'ctc_simulation_household_children.sql' first
with ctc_simulation_household_children as (
    from read_parquet('output/ctc_simulation_household_children.parquet')
),

raw_ctc_simulation_income_distribution as (
    select *
        exclude ("Median income (dollars)", "Mean income (dollars)")
        replace (Municipio as municipio)
    from read_csv_auto('input/ctc_simulation_income_distribution_input_2023.csv', header = true)
),

unpivoted_to_income_bracket as (

    unpivot raw_ctc_simulation_income_distribution
    on COLUMNS(* exclude ("municipio"))
    into
        name income_bracket
        value percentage_households
    
),

extracted_bracket_limits as (
    select
        municipio,
        income_bracket,
        income_bracket.regexp_extract('^([\$,0-9]+)').regexp_replace('[\$,]','', 'g') as income_bracket_lower_limit,
        income_bracket.regexp_extract('([\$,0-9]+)$').regexp_replace('[\$,]','', 'g') as income_bracket_upper_limit,
        (percentage_households.replace('%', '')::FLOAT / 100. ) as percentage_households, -- not normalized pq mi editor me esta cortando sig figs??
    from unpivoted_to_income_bracket
),

filled_bracket_limits as (
    select * replace (
        case when income_bracket_lower_limit = '' then '0' else income_bracket_lower_limit end as income_bracket_lower_limit,
        case when income_bracket_upper_limit = '' then income_bracket_lower_limit else income_bracket_upper_limit end as income_bracket_upper_limit,
    )
    from extracted_bracket_limits
)

select * replace (
    income_bracket_lower_limit::INT as income_bracket_lower_limit,
    income_bracket_upper_limit::INT as income_bracket_upper_limit,
)
from filled_bracket_limits

);

copy ctc_simulation_income_brackets to 'output/ctc_simulation_income_brackets.parquet' (format parquet);

from ctc_simulation_income_brackets;