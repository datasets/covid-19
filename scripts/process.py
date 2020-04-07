from dataflows import Flow, load, unpivot, find_replace, set_type, dump_to_path
from dataflows import update_package, update_resource, update_schema, join
from dataflows import join_with_self, add_computed_field, delete_fields
from dataflows import checkpoint, duplicate, filter_rows, sort_rows, printer

BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
CONFIRMED = 'time_series_covid19_confirmed_global.csv'
DEATH = 'time_series_covid19_deaths_global.csv'
RECOVERED = 'time_series_covid19_recovered_global.csv'
CONFIRMED_US = 'time_series_covid19_confirmed_US.csv'
DEATH_US = 'time_series_covid19_deaths_US.csv'

def to_normal_date(row):
    old_date = row['Date']
    month, day, year = row['Date'].split('-')
    day = f'0{day}' if len(day) == 1 else day
    month = f'0{month}' if len(month) == 1 else month
    row['Date'] = '-'.join([day, month, year])

unpivoting_fields = [
    { 'name': '([0-9]+\/[0-9]+\/[0-9]+)', 'keys': {'Date': r'\1'} }
]

extra_keys = [{'name': 'Date', 'type': 'string'} ]
extra_value = {'name': 'Case', 'type': 'number'}


def pivot_key_countries(package):
    key_countries = ['China', 'US', 'United_Kingdom', 'Italy', 'France', 'Germany', 'Spain', 'Iran']
    for country in key_countries:
        package.pkg.descriptor['resources'][1]['schema']['fields'].append(dict(
            name=country,
            type='integer',
            description='Cumulative total confirmed cases to date.'
        ))
    yield package.pkg
    resources = iter(package)

    data_by_province = next(resources)
    yield data_by_province

    data_by_key_countries = next(resources)
    def process_rows(rows):
        new_row = dict(Date=None, China=None, US=None, United_Kingdom=None, Italy=None, France=None, Germany=None, Spain=None, Iran=None)
        for row in rows:
            country = row['Country'].replace(' ', '_')
            if country in key_countries:
                new_row['Date'] = row['Date']
                new_row[country] = row['Confirmed']
            if None not in new_row.values():
                yield new_row
                new_row = dict(Date=None, China=None, US=None, United_Kingdom=None, Italy=None, France=None, Germany=None, Spain=None, Iran=None)

    yield process_rows(data_by_key_countries)

    data_by_country = next(resources)
    yield data_by_country

    worldwide = next(resources)
    yield worldwide

    us_data = next(resources)
    yield us_data


def calculate_increase_rate(package):
    package.pkg.descriptor['resources'][1]['schema']['fields'].append(dict(
        name='Increase rate',
        type='number',
        description='Inrease rate from the previous day in percentage.'
    ))
    yield package.pkg
    resources = iter(package)
    first_resource = next(resources)
    yield first_resource

    worldwide_data = next(resources)
    def process_rows(rows):
        previous_row = None
        for row in rows:
            if previous_row:
                row['Increase rate'] = (row['Confirmed'] - previous_row['Confirmed']) / previous_row['Confirmed'] * 100
            previous_row = row
            yield row
    yield process_rows(worldwide_data)

    last_resource = next(resources)
    yield last_resource

def fix_canada_recovered_data(rows):
    expected = {
        'Date': None,
        'Province/State': None,
        'Country/Region': None,
        'Lat': None,
        'Long': None,
        'Case': None,
        'Confirmed': None,
        'Recovered': None
        }
    for row in rows:
        if row.get('Country/Region') == 'Canada' and \
            row.get('Province/State') == 'Recovered' and not \
            row.get('Recovered'):
            continue
        if row.get('Country/Region') == 'Canada' and not row.get('Province/State'):
            row['Province/State'] = 'Recovery aggregated'
            row['Lat'] = row.get('Lat', '56.1304')
            row['Long'] = row.get('Long', '-106.3468')
        yield {**expected, **row}

Flow(
      load(f'{BASE_URL}{CONFIRMED}'),
      load(f'{BASE_URL}{RECOVERED}'),
      load(f'{BASE_URL}{DEATH}'),
      load(f'{BASE_URL}{CONFIRMED_US}'),
      load(f'{BASE_URL}{DEATH_US}'),
      checkpoint('load_data'),
      unpivot(unpivoting_fields, extra_keys, extra_value),
      find_replace([{'name': 'Date', 'patterns': [{'find': '/', 'replace': '-'}]}]),
      to_normal_date,
      set_type('Date', type='date', format='%d-%m-%y', resources=None),
      set_type('Case', type='number', resources=None),
      join(
        source_name='time_series_covid19_confirmed_global',
        source_key=['Province/State', 'Country/Region', 'Date'],
        source_delete=True,
        target_name='time_series_covid19_deaths_global',
        target_key=['Province/State', 'Country/Region', 'Date'],
        fields=dict(Confirmed={
            'name': 'Case',
            'aggregate': 'first'
        })
      ),
      join(
        source_name='time_series_covid19_recovered_global',
        source_key=['Province/State', 'Country/Region', 'Date'],
        source_delete=True,
        target_name='time_series_covid19_deaths_global',
        target_key=['Province/State', 'Country/Region', 'Date'],
        fields=dict(Recovered={
            'name': 'Case',
            'aggregate': 'first'
        }),
        mode='full-outer'
      ),
      join(
        source_name='time_series_covid19_confirmed_US',
        source_key=['Province_State', 'Country_Region', 'Date'],
        source_delete=True,
        target_name='time_series_covid19_deaths_US',
        target_key=['Province_State', 'Country_Region', 'Date'],
        fields=dict(Confirmed={
            'name': 'Case',
            'aggregate': 'first'
        })
      ),
      # Add missing columns, e.g., after 'full-outer' join, the rows structure
      # is inconsistent
      fix_canada_recovered_data,
      add_computed_field(
        target={'name': 'Deaths', 'type': 'number'},
        operation='format',
        with_='{Case}'
      ),
      delete_fields(['Case']),
      update_resource('time_series_covid19_deaths_global', name='time-series-19-covid-combined', path='data/time-series-19-covid-combined.csv'),
      update_resource('time_series_covid19_deaths_US', name='us', path='data/us.csv'),
      update_schema('time-series-19-covid-combined', missingValues=['None', ''], fields=[
        {
        "format": "%Y-%m-%d",
        "name": "Date",
        "type": "date"
        },
        {
          "format": "default",
          "name": "Country/Region",
          "type": "string"
        },
        {
          "format": "default",
          "name": "Province/State",
          "type": "string"
        },
        {
          "decimalChar": ".",
          "format": "default",
          "groupChar": "",
          "name": "Lat",
          "type": "number"
        },
        {
          "decimalChar": ".",
          "format": "default",
          "groupChar": "",
          "name": "Long",
          "type": "number"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Confirmed",
          "title": "Cumulative total confirmed cases to date",
          "type": "integer"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Recovered",
          "title": "Cumulative total recovered cases to date",
          "type": "integer"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Deaths",
          "title": "Cumulative total deaths to date",
          "type": "integer"
        }
      ]),
      update_schema('us', missingValues=['None', '']),
      checkpoint('processed_data'),
      printer(),
      # Sort rows by date and country
      sort_rows('{Country/Region}{Province/State}{Date}', resources='time-series-19-covid-combined'),
      # Duplicate the stream to create aggregated data
      duplicate(
        source='time-series-19-covid-combined',
        target_name='worldwide-aggregated',
        target_path='data/worldwide-aggregated.csv'
      ),
      join_with_self(
        resource_name='worldwide-aggregated',
        join_key=['Date'],
        fields=dict(
            Date={
                'name': 'Date'
            },
            Confirmed={
                'name': 'Confirmed',
                'aggregate': 'sum'
            },
            Recovered={
                'name': 'Recovered',
                'aggregate': 'sum'
            },
            Deaths={
                'name': 'Deaths',
                'aggregate': 'sum'
            }
        )
      ),
      printer(),
      update_schema('worldwide-aggregated', missingValues=['None', ''], fields=[
        {
          "format": "%Y-%m-%d",
          "name": "Date",
          "type": "date"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Confirmed",
          "title": "Cumulative total confirmed cases to date",
          "type": "integer"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Recovered",
          "title": "Cumulative total recovered cases to date",
          "type": "integer"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Deaths",
          "title": "Cumulative total deaths to date",
          "type": "integer"
        }
      ]),
      checkpoint('processed_worldwide_data'),
      # Add daily increase rate field in the worldwide data
      calculate_increase_rate,
      # Create another resource with key countries pivoted
      duplicate(
        source='time-series-19-covid-combined',
        target_name='key-countries-pivoted',
        target_path='data/key-countries-pivoted.csv'
      ),
      join_with_self(
        resource_name='key-countries-pivoted',
        join_key=['Date', 'Country/Region'],
        fields=dict(
            Date={
                'name': 'Date'
            },
            Country={
                'name': 'Country/Region'
            },
            Confirmed={
                'name': 'Confirmed',
                'aggregate': 'sum'
            },
            Recovered={
                'name': 'Recovered',
                'aggregate': 'sum'
            },
            Deaths={
                'name': 'Deaths',
                'aggregate': 'sum'
            }
        )
      ),
      update_schema('key-countries-pivoted', missingValues=['None', ''], fields=[
        {
          "format": "%Y-%m-%d",
          "name": "Date",
          "type": "date"
        },
        {
          "format": "default",
          "name": "Country",
          "type": "string"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Confirmed",
          "title": "Cumulative total confirmed cases to date",
          "type": "integer"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Recovered",
          "title": "Cumulative total recovered cases to date",
          "type": "integer"
        },
        {
          "format": "default",
          "groupChar": "",
          "name": "Deaths",
          "title": "Cumulative total deaths to date",
          "type": "integer"
        }
      ]),
      printer(),
      checkpoint('processed_country_data'),
      # All countries aggregated
      duplicate(
        source='key-countries-pivoted',
        target_name='countries-aggregated',
        target_path='data/countries-aggregated.csv'
      ),
      pivot_key_countries,
      delete_fields(['Country', 'Confirmed', 'Recovered', 'Deaths'], resources='key-countries-pivoted'),
      # Prepare data package (name, title) and add views
      update_package(
        name='covid-19',
        title='Novel Coronavirus 2019',
        views=[
            {
              "title": "Total world to date",
              "resources": ["worldwide-aggregated"],
              "specType": "simple",
              "spec": {
                "group": "Date",
                "series": ["Confirmed", "Deaths"],
                "type": "line"
              }
            },
            {
                "title": "Number of confirmed cases in key countries",
                "resources": ["key-countries-pivoted"],
                "specType": "simple",
                "spec": {
                    "group": "Date",
                    "series": ["China", "US", "United_Kingdom", "Italy", "France", "Germany", "Spain", "Iran"],
                    "type": "line"
                }
            },
            {
                "title": "Mortality rate in percentage",
                "resources": [
                    {
                        "name": "worldwide-aggregated",
                        "transform": [
                            {
                                "type": "formula",
                                "expressions": [
                                    "data['Deaths'] / data['Confirmed'] * 100 + '%'"
                                ],
                                "asFields": ["Mortality rate"]
                            }
                        ]
                    }
                ],
                "specType": "simple",
                "spec": {
                    "group": "Date",
                    "series": ["Mortality rate"],
                    "type": "bar"
                }
            },
            {
                "title": "Increase rate from previous day in confirmed cases worldwide",
                "resources": ["worldwide-aggregated"],
                "specType": "simple",
                "spec": {
                    "group": "Date",
                    "series": ["Increase rate"],
                    "type": "bar"
                }
            }
        ]
      ),
      printer(),
      dump_to_path()
).results()[0]
