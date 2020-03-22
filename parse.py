#!/usr/bin/python3

import csv
import json
import os

def main():
    csv_path="time-series-19-covid-combined.csv"
    parse_data(csv_path)

def parse_data(csv_path):

    json_path="country_comparison.json"
    data = {}
    countries = {}

    with open(csv_path) as csvFile:
        csvReader = csv.DictReader(csvFile)
        for rows in csvReader:
            countries[rows['Country/Region']] = ""
            for country in countries:
                info = {'Confirmed': 0, 'Recovered':0, 'Deaths':0}
                info['Confirmed'] = info['Confirmed'] + int(rows['Confirmed'])
                info['Recovered'] = info['Recovered'] + int(rows['Recovered'])
                info['Deaths'] = info['Deaths'] + int(rows['Deaths'])
                dates = {}
                dates[rows['Date']] = info
                try:
                    data[country].update(dates)
                except:
                    data[country] = dates

    with open(json_path, 'w') as jsonFile:
        jsonFile.write(json.dumps(data, indent=4))

if __name__ == "__main__":
    main()