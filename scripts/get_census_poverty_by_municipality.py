# Para 

import os
import sys
import duckdb
import requests

# TODO: Download S1901 and S1701 for municipalities


overwrite_downloads = False
downloads_dir = './datos/downloads'

acs5_year = 2023
groups_to_download = ['S1701']

# Documentation link template such as:
# https://api.census.gov/data/2022/acs/acs5/subject/groups/S1701.html

links_to_download = [
    f'https://api.census.gov/data/{acs5_year}/acs/acs5/subject?get=group({g})&ucgid=pseudo(0400000US72$0500000)'
    for g in groups_to_download
]

# Download ACS tables
downloads_data = []
for g in groups_to_download:
    fname = f'ACS5Y_{acs5_year}_{g}_by_municipality.json'
    fpath = os.path.join(downloads_dir, fname)
    link = f'https://api.census.gov/data/{acs5_year}/acs/acs5/subject?get=group({g})&ucgid=pseudo(0400000US72$0500000)'

    downloads_data.append(dict(fpath=fpath, fname=fname, group=g, acs5_year=acs5_year, api_link=link))
    if os.path.exists(fpath) and not overwrite_downloads:
        continue

    print(f'Downloading {fname}...')

    r = requests.get(link)
    r.raise_for_status()

    with open(fpath, 'wb') as f:
        f.write(r.content)

# Process tables
con = duckdb.connect(database=':memory:')

for downloaded_file in downloads_data:
    print(f'Processing {downloaded_file["fpath"]}...')

    con.execute(f'''
       create or replace table acs_data as (
        from '{downloaded_file["fpath"]}'
        )         
    ''')

    con.execute('''
       set variable acs_column_names = (from acs_data limit 1)         
    ''')

    acs_column_names = con.sql("select getvariable('acs_column_names')").fetchone()[0]
    # print(acs_column_names)

    acs_column_aliases = ',\n'.join([f'json[{i+1}] as {col}' for i, col in enumerate(acs_column_names)])

    con.execute(f'''
        create or replace table acs_data_expanded as (
            select
                {acs_column_aliases}
            from acs_data
            offset 1
        )
    ''')

    fname_csv = downloaded_file['fname'].replace('.json', '') + '.csv'
    fpath_csv = os.path.join('./input', fname_csv)

    con.sql('from acs_data_expanded').to_csv(fpath_csv)
    print(f'Wrote {fpath_csv}')

    print()
    pass

# S1701_C01_001E - Estimate!!Total!!Population for whom poverty status is determined
# S1701_C01_002E - Estimate!!Total!!Population for whom poverty status is determined!!AGE!!Under 18 years
# S1701_C01_034E - Estimate!!Total!!Population for whom poverty status is determined!!WORK EXPERIENCE!!Population 16 years and over (para calcular 15 and under)

# Y luego usa el siguiente query para formatear los datos
# para copy paste al excel de "Poverty5Y2022Municipios.xlsx"
"""
with acs_poverty_status as (
from './input/ACS5Y_2022_S1701_by_municipality.csv'
),

chosen_cols as (

select geo_id, name,
    S1701_C01_001E as total_population,
    S1701_C01_002E as total_population_under_18,
    S1701_C01_034E as total_population_16_and_over,
    total_population - total_population_16_and_over as total_population_15_and_under,
    S1701_C03_001E as percent_population_below_poverty,
    S1701_C03_002E as percent_population_under_18_below_poverty,
    S1701_C02_001E as total_population_below_poverty,
    S1701_C02_002E as total_population_under_18_below_poverty,
from acs_poverty_status
)

select * replace (
   format('{:,}', total_population) as total_population,
   percent_population_below_poverty::VARCHAR || '%' as percent_population_below_poverty,
   percent_population_under_18_below_poverty::VARCHAR || '%' as percent_population_under_18_below_poverty,
   format('{:,}', total_population_below_poverty) as total_population_below_poverty,
   format('{:,}', total_population_under_18_below_poverty) as total_population_under_18_below_poverty,
)
from chosen_cols
"""