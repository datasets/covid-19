import csv
from dataflows import Flow, load, unpivot, find_replace, set_type, dump_to_path
import datapackage

BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
CONFIRMED = 'time_series_19-covid-Confirmed.csv'
DEATH = 'time_series_19-covid-Deaths.csv'
RECOVERED = 'time_series_19-covid-Recovered.csv'

def to_normal_date(row):
    old_date = row['date']
    month, day, year = row['date'].split('-')
    day = f'0{day}' if len(day) == 1 else day
    month = f'0{month}' if len(month) == 1 else month
    row['date'] = '-'.join([day, month, year])

unpivoting_fields = [
    { 'name': '([0-9]+\/[0-9]+\/[0-9]+)', 'keys': {'date': r'\1'} }
]

extra_keys = [{'name': 'date', 'type': 'string'} ]
extra_value = {'name': 'case', 'type': 'string'}

for case in [CONFIRMED, DEATH, RECOVERED]:
    Flow(
          load(f'{BASE_URL}{case}'),
          unpivot(unpivoting_fields, extra_keys, extra_value),
          find_replace([{'name': 'date', 'patterns': [{'find': '/', 'replace': '-'}]}]),
          to_normal_date,
          set_type('date', type='date', format='%d-%m-%y'),
          set_type('case', type='number'),
          dump_to_path()
    ).results()[0]
