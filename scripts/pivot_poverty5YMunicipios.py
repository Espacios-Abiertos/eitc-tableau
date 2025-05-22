# Para reorientar el archivo de pobreza al formato que usaremos en Tableau

import polars as pl

acs5_year = 2023
pl.read_excel(f'input/Poverty5Y{acs5_year}Municipios.xlsx').write_csv(f'input/Poverty5Y{acs5_year}Municipios.csv')
df = (
    pl.read_excel(f'input/Poverty5Y{acs5_year}Municipios.xlsx')
    .rename({
        'Municipio': 'municipio',
        'Population served': 'population_served',
        'Total Population': 'total_population',
        'Under 18Y': 'under_18y',
        '15 and under': 'under_16y',
        '16 and under 2020 census': 'under_17y',
    })
    
)
print('df')
print(df)

df_total_pivot = (
    df
    .pivot(index='municipio', columns='population_served', values='total_population')
    .rename({
        'Total': 'total_population',
        'Percent below poverty level': 'percent_below_poverty_level',
        'Below poverty level': 'below_poverty_level',
    })
    )
df_under18_pivot = (df
                    .pivot(index='municipio', columns='population_served', values='under_18y')
                    .rename({
                        'Total': 'under_18y_total_population',
                        'Percent below poverty level': 'under_18y_percent_below_poverty_level',
                        'Below poverty level': 'under_18y_below_poverty_level',
                    
                    })
                    )
df_under16_pivot = (df
                    .pivot(index='municipio', columns='population_served', values='under_16y')
                    .rename({
                        'Total': 'under_16y_total_population',
                        'Percent below poverty level': 'under_16y_percent_below_poverty_level',
                        'Below poverty level': 'under_16y_below_poverty_level',
                    
                    })
                    )
df_under17_pivot = (df
                    .pivot(index='municipio', columns='population_served', values='under_17y')
                    .rename({
                        'Total': 'under_17y_total_population',
                        'Percent below poverty level': 'under_17y_percent_below_poverty_level',
                        'Below poverty level': 'under_17y_below_poverty_level',
                    
                    })
                    )
print('df_total_pivot')
print(df_total_pivot)
print('df_under18_pivot')
print(df_under18_pivot)
print('df_under16_pivot')
print(df_under16_pivot)
print('df_under17_pivot')
print(df_under17_pivot)

df_pivoted = (
    df_total_pivot
    .join(df_under18_pivot, on='municipio', how='outer_coalesce', )
    .join(df_under16_pivot, on='municipio', how='outer_coalesce', )
    .join(df_under17_pivot, on='municipio', how='outer_coalesce', )
)
print('df_pivoted:')
with pl.Config(tbl_cols=-1):
    print(df_pivoted)


df_pivoted_casted = (
    df_pivoted
    .with_columns([
        pl.col('total_population').str.replace(',','').cast(int),
        pl.col('under_18y_total_population').str.replace(',','').cast(int),
        # pl.col('under_16y_total_population').str.replace(',','').cast(int),
        # pl.col('under_17y_total_population').str.replace(',','').cast(int),

        pl.col('percent_below_poverty_level').str.replace('%','').cast(float)/100,
        pl.col('under_18y_percent_below_poverty_level').str.replace('%','').cast(float)/100,
        # pl.col('under_16y_percent_below_poverty_level').str.replace('%','').cast(float)/100,
        # pl.col('under_17y_percent_below_poverty_level').str.replace('%','').cast(float)/100,

        pl.col('below_poverty_level').str.replace(',','').cast(int),
        pl.col('under_18y_below_poverty_level').str.replace(',','').cast(int),
        # pl.col('under_16y_below_poverty_level').str.replace(',','').cast(int),
        # pl.col('under_17y_below_poverty_level').str.replace(',','').cast(int),
    ])
    .with_columns([
        (pl.col('under_16y_total_population') + 0.5*(pl.col('under_18y_total_population') - pl.col('under_16y_total_population'))).ceil().cast(int).alias(f'under_17y_total_population_{acs5_year}'),
    ])
)
print('df_pivoted_casted:')
print(df_pivoted_casted)

# Save to excel and csv
df_pivoted_casted.write_csv(f'input/Poverty5Y{acs5_year}Municipios_pivoted.csv')
df_pivoted_casted.write_excel(f'input/Poverty5Y{acs5_year}Municipios_pivoted.xlsx', worksheet=f'Poverty5Y{acs5_year}Municipios_pivoted')