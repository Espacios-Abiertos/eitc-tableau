# Para reorientar el archivo de pobreza al formato que usaremos en Tableau

import polars as pl

pl.read_excel('input/Poverty5Y2022Municipios.xlsx').write_csv('input/Poverty5Y2022Municipios.csv')
df = (
    pl.read_excel('input/Poverty5Y2022Municipios.xlsx')
    .rename({
        'Municipio': 'municipio',
        'Population served': 'population_served',
        'Total Population': 'total_population',
        'Under 18Y': 'under_18y',
    })
    
)

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
print(df_total_pivot)
print(df_under18_pivot)

df_pivoted = (
    df_total_pivot
    .join(df_under18_pivot, on='municipio', how='outer_coalesce', )
)
print(df_pivoted)

df_pivoted_casted = (
    df_pivoted
    .with_columns([
        pl.col('total_population').str.replace(',','').cast(int),
        pl.col('under_18y_total_population').str.replace(',','').cast(int),

        pl.col('percent_below_poverty_level').str.replace('%','').cast(float)/100,
        pl.col('under_18y_percent_below_poverty_level').str.replace('%','').cast(float)/100,

        pl.col('below_poverty_level').str.replace(',','').cast(int),
        pl.col('under_18y_below_poverty_level').str.replace(',','').cast(int),
    ])
)
print(df_pivoted_casted)

# Save to excel and csv
df_pivoted_casted.write_csv('input/Poverty5Y2022Municipios_pivoted.csv')
df_pivoted_casted.write_excel('input/Poverty5Y2022Municipios_pivoted.xlsx', worksheet='Poverty5Y2022Municipios_pivoted')