import polars as pl

poblacion = (
    pl.read_excel('input/Poverty5Y2022Municipios.xlsx')
    .rename({
        'Municipio': 'municipio',
        'Population served': 'population_served',
        'Total Population': 'total_population',
        'Under 18Y': 'under_18y',
        '15 and under': 'under_16y',
        '16 and under 2020 census': 'under_17y',
    })
    .filter(pl.col('population_served') == 'Total')
    .select('municipio',
            pl.col('total_population').str.replace(',','').cast(int),
    )
    .with_columns(
        num_legisladores = 
        pl.when(pl.col('municipio') == 'San Juan')
            .then(pl.lit(17))
        .when(pl.col('municipio') == 'Culebra')
            .then(pl.lit(5))
        .when(pl.col('total_population') >= 40_000)
            .then(pl.lit(16))
        .when(pl.col('total_population') >= 20_000)
            .then(pl.lit(14))
        .when(pl.col('total_population') < 20_000)
            .then(pl.lit(12))
    )
    .rename({
        'municipio': 'Municipio',
        'total_population': 'Población',
        'num_legisladores': 'Legisladores'
    })
    
    
)

print('Poblacion por municipio:')
print(poblacion)
print()

poblacion.write_excel('output/total_legisladores_municipales.xlsx')