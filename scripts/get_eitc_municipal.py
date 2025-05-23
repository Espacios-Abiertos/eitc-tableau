# Genera eitc_municipal_2023.xlsx
# Empezando con los datos de 2023, los de 2022 ya estaban set.

import duckdb
import polars as pl

db_fpath = 'datos/databases/eitc_2023.db'
con = duckdb.connect(db_fpath, read_only=True)

column_names = con.sql(
'''
describe municipio
'''
).fetchall()

column_names = [col[0] for col in column_names]
# print(*column_names, sep='\n')

# Current names:
# municipio
# year_contributivo
# ingreso_ajustado_casados
# ingreso_ajustado_casados_planillas
# ingreso_ajustado_individual
# ingreso_ajustado_individual_planillas
# ingreso_ganado_casados
# ingreso_ganado_casados_planillas
# ingreso_ganado_individual
# ingreso_ganado_individual_planillas
# ct_ingreso_salarios
# ct_ingreso_salarios_planillas
# ct_ingreso_pension
# ct_ingreso_pension_planillas
# ct_ingreso_industria
# ct_ingreso_industria_planillas
# ct_ingreso_ganado
# ct_ingreso_ganado_planillas
# credito_trabajo_casados
# credito_trabajo_casados_planillas
# credito_trabajo_individual
# credito_trabajo_individual_planillas
# ct_cantidad_dependientes
# ct_cantidad_dependientes_planillas
# ingreso_ajustado
# ingreso_ajustado_planillas
# ingreso_ganado
# ingreso_ganado_planillas
# credito_trabajo
# credito_trabajo_planillas
# credito_trabajo_promedio
# credito_trabajo_promedio_individual
# credito_trabajo_promedio_casados
# ingreso_ajustado_promedio
# ingreso_ajustado_promedio_individual
# ingreso_ajustado_promedio_casados
# ingreso_ganado_promedio
# ingreso_ganado_promedio_individual
# ingreso_ganado_promedio_casados
# ct_cantidad_dependientes_promedio

# But only need these with these names:
# Municipios
# Año contributivo
# Ingreso Bruto Ajustado - Casados
# Planillas - Ingreso Bruto Ajustado - Casados
# Ingreso Bruto Ajustado - Contribuyente Individual
# Planillas - Ingreso Bruto Ajustado - Contribuyente Individual
# Casados - Total Ingreso bruto ganado para la determinación del EITC
# Planillas Casados - Total Ingreso bruto ganado para la determinación del EITC
# Contribuyente Individual - Total Ingreso bruto ganado para la determinación del EITC
# Planillas Contribuyente Individual - Total Ingreso bruto ganado para la determinación del EITC
# Anejo CT: Ingresos Salarios
# Planillas - Anejo CT: Ingresos Salarios
# Anejo CT: Ingresos Pensión
# Planillas - Anejo CT: Ingresos Pensión
# Anejo CT: Ingresos Industria o Negocio
# Planillas - Anejo CT: Ingresos Industria o Negocio
# Anejo CT Total ingreso Bruto Ganado
# Planillas - Anejo CT Total ingreso Bruto Ganado
# Crédito por Trabajo - Casados
# Planillas Casados - Crédito por Trabajo
# Crédito por Trabajo - Contribuyente Individual
# Planillas Contribuyente Individual - Crédito por Trabajo
# Anejo CT Cantidad de dependientes cualificados
# Planillas - Anejo CT Cantidad de dependientes cualificados
# Crédito por Trabajo - Total
# Planillas Total - Crédito por Trabajo
# Crédito por Trabajo - Promedio

rel = con.sql(
'''
select
    municipio_table.municipio as Municipios,
    year_contributivo as "Año contributivo",
    ingreso_ajustado_casados as "Ingreso Bruto Ajustado - Casados",
    ingreso_ajustado_casados_planillas as "Planillas - Ingreso Bruto Ajustado - Casados",
    ingreso_ajustado_individual as "Ingreso Bruto Ajustado - Contribuyente Individual",
    ingreso_ajustado_individual_planillas as "Planillas - Ingreso Bruto Ajustado - Contribuyente Individual",
    ingreso_ganado_casados as "Casados - Total Ingreso bruto ganado para la determinación del EITC",
    ingreso_ganado_casados_planillas as "Planillas Casados - Total Ingreso bruto ganado para la determinación del EITC",
    ingreso_ganado_individual as "Contribuyente Individual - Total Ingreso bruto ganado para la determinación del EITC",
    ingreso_ganado_individual_planillas as "Planillas Contribuyente Individual - Total Ingreso bruto ganado para la determinación del EITC",
    ct_ingreso_salarios as "Anejo CT: Ingresos Salarios",
    ct_ingreso_salarios_planillas as "Planillas - Anejo CT: Ingresos Salarios",
    ct_ingreso_pension as "Anejo CT: Ingresos Pensión",
    ct_ingreso_pension_planillas as "Planillas - Anejo CT: Ingresos Pensión",
    ct_ingreso_industria as "Anejo CT: Ingresos Industria o Negocio",
    ct_ingreso_industria_planillas as "Planillas - Anejo CT: Ingresos Industria o Negocio",
    ct_ingreso_ganado as "Anejo CT Total ingreso Bruto Ganado",
    ct_ingreso_ganado_planillas as "Planillas - Anejo CT Total ingreso Bruto Ganado",
    credito_trabajo_casados as "Crédito por Trabajo - Casados",
    credito_trabajo_casados_planillas as "Planillas Casados - Crédito por Trabajo",
    credito_trabajo_individual as "Crédito por Trabajo - Contribuyente Individual",
    credito_trabajo_individual_planillas as "Planillas Contribuyente Individual - Crédito por Trabajo",
    ct_cantidad_dependientes as "Anejo CT Cantidad de dependientes cualificados",
    ct_cantidad_dependientes_planillas as "Planillas - Anejo CT Cantidad de dependientes cualificados",
    credito_trabajo as "Crédito por Trabajo - Total",
    credito_trabajo_planillas as "Planillas Total - Crédito por Trabajo",
    credito_trabajo_promedio as "Crédito por Trabajo - Promedio"
from municipio as municipio_table
'''
)

eitc_municipal_2023 = rel.pl()
eitc_municipal_2023.write_excel('input/eitc_municipal_2023.xlsx')
print('Wrote input/eitc_municipal_2023.xlsx')