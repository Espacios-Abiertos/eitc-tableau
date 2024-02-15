create or replace table ctc_simulation as (
with ctc_simulation_household_children as (
    from read_parquet('output/ctc_simulation_household_children.parquet')
),

ctc_simulation_income_brackets as (
    from read_parquet('output/ctc_simulation_income_brackets.parquet')
),

 -- from ctc_simulation_household_children
 -- select
 --     municipio,
 --     total_households_with_people_under_18,
 --     total_households_with_min_people_under_18,
 --     total_households_with_max_people_under_18,
 --     min_people_under_18_per_household_with_people_under_18,
 --     max_people_under_18_per_household_with_people_under_18,
 --     (total_households_with_min_people_under_18 + total_households_with_max_people_under_18) as sanity_check,
 --     sanity_check = total_households_with_people_under_18 as passes_test

households_allocated_into_number_of_children as (
    from ctc_simulation_household_children
    select
        municipio,
        -- es haciendo un UNPIVOT medio manualmente. Primero crea dos filas porque hay dos structs, y luego expande
        -- los keys del struct a nuevas columnas a traves del recursive kwarg del unnest
        unnest([
            {'total_people_under_18_per_household_with_people_under_18': min_people_under_18_per_household_with_people_under_18,
            'total_households_with_people_under_18': total_households_with_min_people_under_18},
            
            {'total_people_under_18_per_household_with_people_under_18': max_people_under_18_per_household_with_people_under_18,
            'total_households_with_people_under_18': total_households_with_max_people_under_18}
        ], recursive := true)
),

households_allocated_into_income_brackets as (
    select
        municipio,
        (0.5*(income_bracket_lower_limit + income_bracket_upper_limit)).round()::INT as income_bracket_middle_value,
        (percentage_households * ctc_simulation_household_children.total_households_with_people_under_18).round()::INT as total_households_with_people_under_18
    from ctc_simulation_income_brackets
    full join
    ctc_simulation_household_children
    using (municipio)
),

-- same al que tiene su nombre parecido, pero el total de filas matchea el total de ocurrencias de la combinacion
-- y se anadieron indices para reflejar en que "orden" las muestras deben ser consolidadas.
-- Recuerda que mas ninos en poco ingreso paga igual CTC que menos ninos en igual de poco ingreso.
rowwise_households_allocated_into_number_of_children as (
    select * exclude (repetition_counter),
        row_number() over (partition by municipio order by total_people_under_18_per_household_with_people_under_18, repetition_counter) as partitioned_sample_number_less_kids_first,
        row_number() over (partition by municipio order by total_people_under_18_per_household_with_people_under_18 desc, repetition_counter) as partitioned_sample_number_more_kids_first,
    from (
        select *,
            unnest(generate_series(1, total_households_with_people_under_18)) as repetition_counter, -- used as a hack to duplicate number of rows correctly
        from households_allocated_into_number_of_children
    )
    order by municipio, total_people_under_18_per_household_with_people_under_18, partitioned_sample_number_less_kids_first
),

rowwise_households_allocated_into_income_brackets as (
    select * exclude (repetition_counter),
        row_number() over (partition by municipio order by income_bracket_middle_value, repetition_counter) as partitioned_sample_number_less_income_first,
        row_number() over (partition by municipio order by income_bracket_middle_value desc, repetition_counter) as partitioned_sample_number_more_income_first,
    from (
        select *,
            unnest(generate_series(1, total_households_with_people_under_18)) as repetition_counter, -- used as a hack to duplicate number of rows correctly
        from households_allocated_into_income_brackets
    )
    order by municipio, income_bracket_middle_value, partitioned_sample_number_less_income_first
),

rowwise_household_ctc_samples as (
    -- smfh
    select
        rowwise_households_allocated_into_number_of_children.municipio,
        total_people_under_18_per_household_with_people_under_18 as num_kids,
        income_bracket_middle_value as income,
    from rowwise_households_allocated_into_number_of_children
    full join
    rowwise_households_allocated_into_income_brackets
    on
        rowwise_households_allocated_into_number_of_children.municipio = rowwise_households_allocated_into_income_brackets.municipio and
        
        -- CHOOSE one of the following:
        partitioned_sample_number_more_kids_first = partitioned_sample_number_less_income_first -- more kids into lower income means less ctc disbursed in the estimate (lower estimate)
        -- partitioned_sample_number_less_kids_first = partitioned_sample_number_less_income_first -- less kids into lower income means more ctc disbursed in the estimate (upper estimate)
        
),

final_ctc_simulation as (
select *,
    count(*) as total_households,
    1600 * num_kids as ctc_kids_cap,
    .0765 * income as ctc_income_cap,
    case when ctc_income_cap < ctc_kids_cap then ctc_income_cap else ctc_kids_cap end as ctc_credit, -- pick the lowest of the two
from rowwise_household_ctc_samples
group by all
order by municipio, num_kids, income
)

from final_ctc_simulation
);

copy ctc_simulation to 'output/ctc_simulation.parquet' (format parquet);

-- Municipio aggregate results
-- Manualmente mover a Excel, usa python to export to a dataframe or un archivo or just use harlequin y copypaste a un Excel
create or replace table ctc_simulation_municipios as (
   select
       municipio,
       sum(ctc_credit * total_households) as total_ctc_credit
   from ctc_simulation
   where municipio is not null
   group by municipio
   order by municipio
);

copy ctc_simulation_municipios to 'output/ctc_simulation_municipios.parquet' (format parquet);