import csv
import pandas as pd
import datapackage

URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
CONFIRMED = 'time_series_19-covid-Confirmed.csv'
DEATH = 'time_series_19-covid-Deaths.csv'
RECOVERED = 'time_series_19-covid-Recovered.csv'

confirmed_cases = pd.read_csv(URL+CONFIRMED)
total_deaths = pd.read_csv(URL+DEATH)
total_recovered = pd.read_csv(URL+RECOVERED) 

def generate_region_file():
    subset = confirmed_cases.loc[:,'Province/State':'Long']
    subset.insert(0, 'ID', range(1000, 1000 + len(subset)))
    subset.to_csv('data/regions.csv', index = False)

def process_data(input_df, output_filename):
    output = 'data/' + output_filename
    subset = input_df.loc[:,'1/22/20':]
    subset.insert(0, 'ID', range(1000, 1000 + len(subset)))
    subset = subset.set_index('ID').T
    subset = subset.rename(columns = {'ID':'Date'})
    subset.to_csv(output, index = False)

def generate_data_package():
    package = datapackage.Package()
    package.infer('data/*.csv')
    package.save('datapackage.json')

generate_region_file()
process_data(confirmed_cases, 'confirmed.csv')
process_data(total_deaths, 'deaths.csv')
process_data(total_recovered, 'recovered.csv')
generate_data_package()
