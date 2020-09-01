import pandas as pd 
from tqdm import tqdm

base_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
confirmed_url = 'time_series_covid19_confirmed_US.csv'
dead_url ='time_series_covid19_deaths_US.csv'

#===============
# Confirmed Cases
print("===============\nWorking on Confirmed Cases\n\n")
confirmed = pd.read_csv(base_url+confirmed_url)
combined_keys = confirmed['Combined_Key'].unique()
#print(confirmed.columns[:15])
#print(combined_keys)

def adjust_date(s):
    l = s.split('/')
    return f'20{l[2]}-{int(l[0]):02d}-{int(l[1]):02d}'

df = pd.DataFrame()
for key in tqdm(combined_keys):
    #print(key)
    df_c = confirmed[confirmed['Combined_Key']==key]

    #Save static info for location
    UID = df_c['UID'].values[0]
    iso2 = df_c['iso2'].values[0]
    iso3 = df_c['iso3'].values[0]
    code3 = df_c['code3'].values[0]
    FIPS = df_c['FIPS'].values[0]
    Admin2 = df_c['Admin2'].values[0]
    Province_State = df_c['Province_State'].values[0]
    Country_Region = df_c['Country_Region'].values[0]
    Lat = df_c['Lat'].values[0]
    Long = df_c['Long_'].values[0]

    # transpose
    df_c = df_c.drop(['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_', 'Combined_Key'], axis=1).transpose()
    df_c.columns = ['Case']

    # Repopulate static info
    df_c['UID'] = UID
    df_c['iso2'] = iso2 
    df_c['iso3'] = iso3
    df_c['code3'] = code3 
    df_c['FIPS'] = FIPS
    df_c['Admin2'] = Admin2 
    df_c['Province/State'] = Province_State 
    df_c['Country/Region'] = Country_Region 
    df_c['Lat'] = Lat 
    df_c['Long'] =  Long 
    df_c['Combined_Key'] = key 

    # Add date as column and rearrange to YYYY-DD-MM
    df_c['Date'] = df_c.index
    df_c['Date'] = df_c['Date'].map(adjust_date)
    #print(UID, iso2, iso3, code3, FIPS, Admin2, Province_State, Country_Region, Lat, Long)

    df = df.append(df_c[['UID', 'iso2','iso3','code3','FIPS','Admin2','Lat','Combined_Key','Date','Case','Long','Country/Region','Province/State']])
    #break

df = df.reset_index(drop=True)
print(df)
df.to_csv('data/us_confirmed.csv', index=False)


#===============
# Deaths
print('\n\n===============\nWorking on Deaths\n\n')

confirmed = pd.read_csv(base_url+dead_url)
combined_keys = confirmed['Combined_Key'].unique()
#print(confirmed.columns[:15])
#print(combined_keys)

df = pd.DataFrame()
for key in tqdm(combined_keys):
    print(key)
    df_c = confirmed[confirmed['Combined_Key']==key]

    #Save static info for location
    UID = df_c['UID'].values[0]
    iso2 = df_c['iso2'].values[0]
    iso3 = df_c['iso3'].values[0]
    code3 = df_c['code3'].values[0]
    FIPS = df_c['FIPS'].values[0]
    Admin2 = df_c['Admin2'].values[0]
    Province_State = df_c['Province_State'].values[0]
    Country_Region = df_c['Country_Region'].values[0]
    Lat = df_c['Lat'].values[0]
    Long = df_c['Long_'].values[0]
    Population = df_c['Population'].values[0]

    # transpose
    df_c = df_c.drop(['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_', 'Combined_Key', 'Population'], axis=1).transpose()
    df_c.columns = ['Case']

    # Repopulate static info
    df_c['UID'] = UID
    df_c['iso2'] = iso2 
    df_c['iso3'] = iso3
    df_c['code3'] = code3 
    df_c['FIPS'] = FIPS
    df_c['Admin2'] = Admin2 
    df_c['Province/State'] = Province_State 
    df_c['Country/Region'] = Country_Region 
    df_c['Lat'] = Lat 
    df_c['Long'] =  Long 
    df_c['Combined_Key'] = key 
    df_c['Population'] = Population 

    # Add date as column and rearrange to YYYY-DD-MM
    df_c['Date'] = df_c.index
    df_c['Date'] = df_c['Date'].map(adjust_date)
    #print(UID, iso2, iso3, code3, FIPS, Admin2, Province_State, Country_Region, Lat, Long)

    df = df.append(df_c[['UID', 'iso2','iso3','code3','FIPS','Admin2','Lat','Combined_Key','Population','Date','Case','Long','Country/Region','Province/State']])
    #break

df = df.reset_index(drop=True)
print(df)
df.to_csv('data/us_deaths.csv', index=False)
