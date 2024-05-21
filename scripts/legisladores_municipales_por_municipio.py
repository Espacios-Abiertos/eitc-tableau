import polars as pl

poblacion = (
    pl.read_excel('input/2020PopulationEstimate.xlsx')
    .rename({
        'Geographic Area': 'municipio',
        'April 1, 2020 Estimates Base': 'total_population',
    })
    .filter(pl.col('municipio') != 'Puerto Rico')
    .with_columns(
        municipio = pl.col('municipio').str.extract(r'^\.(.+)\sMunicipio')
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