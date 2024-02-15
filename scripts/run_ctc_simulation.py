import duckdb

def read_file(fpath):
    with open(fpath, 'r') as f:
        return f.read()
    
con = duckdb.connect('output/ctc_simulation.db')

print(con.sql(read_file('scripts/ctc_simulation_household_children.sql')))
#con.commit()

print(con.sql(read_file('scripts/ctc_simulation_income_brackets.sql')))
print(con.sql(read_file('scripts/ctc_simulation.sql')))

# print(con.sql('from ctc_simulation'))

print('Totales:')
print(con.sql(
'''
select
    sum(ctc_credit * total_households) as total_ctc_credit
from ctc_simulation
where municipio is not null
'''
))


con.close()