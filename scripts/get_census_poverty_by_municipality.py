# Para 

import os
import sys
import duckdb
import requests

# TODO: Download S1901 and S1701 for municipalities


overwrite_downloads = False
downloads_dir = './datos/downloads'

acs5_year = 2022
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