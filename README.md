# Novel Corona virus - COVID19

The new strain of Coronavirus has had a worldwide effect. It has effected people from different countries. The dataset provides, a time series data tracking the number of people effected by the virus, how many deaths has the virus caused and the number of reported people who have recovered.

## Run the script

We use [dataflows](https://github.com/datahq/dataflows) to process and normalize the data

```
pip install dataflows
```

Run the sript

```
python process.py
```

## Data

Data is coming from https://github.com/CSSEGISandData/COVID-19 updated daily. We have normalized data a bit - unpivoted and transfered dates to be more machine readable.
