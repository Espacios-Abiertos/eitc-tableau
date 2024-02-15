create or replace table ctc_simulation_household_children as (
with initial_import as (

select *
    replace (
        "Under 18Y".replace(',','')::INT as "Under 18Y",
        "Under 16Y".replace(',','')::INT as "Under 16Y",
        "Under 17Y".replace(',','')::INT as "Under 17Y",
    )
from read_csv_auto('input/ctc_simulation_input.csv', header=true)
),

renamed_columns as (
    select
        Municipio as "municipio",
        "Total households" as total_households,
        "Households with one or more people under 18 years" as percentage_households_with_people_under_18,

        -- CHOOSE ONE OF THE FOLLOWING:
        -- Whichever elijas tiene que mantener el nombre de "under_18" para que el resto del script funcione pero pichea
        -- "Under 18Y" as total_people_under_18,
        -- "Under 16Y" as total_people_under_18,
        -- "Under 17Y" as total_people_under_18,
        ("Under 16Y" + 0.5*("Under 18Y" - "Under 16Y")).ceil() as total_people_under_18,

    from initial_import
),

casted_columns as (
    select * replace (
        total_households.replace(',','')::INT  as total_households,
        percentage_households_with_people_under_18.replace('%', '')::FLOAT / 100  as percentage_households_with_people_under_18,
        total_people_under_18.replace(',', '')::INT as total_people_under_18
    )
    from renamed_columns
),

-- Add mas variables

allocate_children_to_households as (
select *,
    (total_households * percentage_households_with_people_under_18).round()::INT as total_households_with_people_under_18,
    total_people_under_18 / total_households_with_people_under_18 as average_people_under_18_per_household_with_people_under_18,
    floor(average_people_under_18_per_household_with_people_under_18)::INT as min_people_under_18_per_household_with_people_under_18,
    ceil(average_people_under_18_per_household_with_people_under_18)::INT as max_people_under_18_per_household_with_people_under_18,
    
    ((max_people_under_18_per_household_with_people_under_18 - average_people_under_18_per_household_with_people_under_18) * total_households_with_people_under_18)::INT as total_households_with_min_people_under_18,
    (-1*(min_people_under_18_per_household_with_people_under_18 - average_people_under_18_per_household_with_people_under_18) * total_households_with_people_under_18)::INT as total_households_with_max_people_under_18,
    
from casted_columns

)

from allocate_children_to_households
);

copy ctc_simulation_household_children to 'output/ctc_simulation_household_children.parquet' (format parquet);

from ctc_simulation_household_children;