import pandas as pd
import numpy as np

base_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
confirmed_url = 'time_series_covid19_confirmed_global.csv'
dead_url = 'time_series_covid19_deaths_global.csv'
recovered_url = 'time_series_covid19_recovered_global.csv'

df = pd.read_csv(base_url+confirmed_url).drop(['Lat', 'Long'], axis=1)
to_concat = df[df['Province/State'].notna()]['Country/Region'].unique()
# We have to combine the numbers from the countries that are split up into provinces
for country in to_concat:
    new_row = df[df['Country/Region']==country].sum()
    new_row['Country/Region'] = country
    new_row['Province/State'] = np.NaN
    df = df.drop(df[df['Country/Region']==country].index)
    df = df.append(new_row, ignore_index=True)
    
confirmed = df[df['Province/State'].isna()].drop('Province/State', axis=1) # take only countries (no territories)
#print(to_concat)

df = pd.read_csv(base_url+dead_url).drop(['Lat', 'Long'], axis=1)
to_concat = df[df['Province/State'].notna()]['Country/Region'].unique()
for country in to_concat:
    new_row = df[df['Country/Region']==country].sum()
    new_row['Country/Region'] = country
    new_row['Province/State'] = np.NaN
    df = df.drop(df[df['Country/Region']==country].index)
    df = df.append(new_row, ignore_index=True)
    
dead = df[df['Province/State'].isna()].drop('Province/State', axis=1) # take only countries (no territories)
#print(to_concat)

df = pd.read_csv(base_url+recovered_url).drop(['Lat', 'Long'], axis=1)
to_concat = df[df['Province/State'].notna()]['Country/Region'].unique()
for country in to_concat:
    #print(df[df['Country/Region']==country])
    new_row = df[df['Country/Region']==country].sum()
    new_row['Country/Region'] = country
    new_row['Province/State'] = np.NaN
    df = df.drop(df[df['Country/Region']==country].index)
    df = df.append(new_row, ignore_index=True)

recovered = df[df['Province/State'].isna()].drop('Province/State', axis=1) # take only countries (no territories)
#print(to_concat)

dates = np.intersect1d(np.intersect1d(confirmed.drop('Country/Region', axis=1).columns, dead.columns), recovered.columns)
df = df[df['Province/State'].isna()].drop('Province/State', axis=1) # take only countries (no territories)
countries = np.intersect1d(np.intersect1d(confirmed['Country/Region'].unique(), dead['Country/Region'].unique()), confirmed['Country/Region'].unique())
#print(countries)





data = pd.DataFrame()
for country in countries:
    print(country)
    #confirmed cases
    cntry_c = confirmed[confirmed['Country/Region'] == country].transpose().drop('Country/Region')
    cntry_c.columns = ['Confirmed']
    cntry_c['Date'] = cntry_c.index

    #total deaths
    cntry_d = dead[dead['Country/Region'] == country].transpose().drop('Country/Region')
    cntry_d.columns = ['Deaths']
    cntry_d['Date'] = cntry_d.index

    #recovered
    cntry_r = recovered[recovered['Country/Region'] == country].transpose().drop('Country/Region')
    cntry_r.columns = ['Recovered']
    cntry_r['Date'] = cntry_r.index

    #concatenate (not the best way)
    cntry = cntry_c
    cntry['Deaths'] = cntry_d['Deaths']
    cntry['Recovered'] = cntry_r['Recovered']

    cntry = cntry.reset_index(drop=True)
    cntry['Country'] = country
    #print(cntry)
    data = data.append(cntry[['Date', 'Country', 'Confirmed', 'Recovered', 'Deaths']])


def adjust_date(s):
    l = s.split('/')
    return f'20{l[2]}-{int(l[1]):02d}-{int(l[0]):02d}'

#data=data.sort_values(by=['Date', 'Country'])
data['Date'] = data['Date'].map(adjust_date)
data = data.reset_index(drop=True)
data.to_csv('international_time_series.csv', index=False)