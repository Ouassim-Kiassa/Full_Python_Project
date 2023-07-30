# see https://github.com/gbif/pygbif
# install request-cache-latest
from pygbif import maps
from pygbif import occurrences as occ
import requests
import pandas as pd
import numpy as np

# constants
gbif_api_occurrence_base = 'https://api.gbif.org/v1/occurrence/search?'
data_path = '../data/'
max_limit = 300

# still more fields exist
columns_of_interest = ['key', 'taxonKey', 'acceptedTaxonKey', 'scientificName', 'basisOfRecord', 'occurrenceStatus',
                       'scientificName', 'kingdom', 'phylum', 'order', 'taxonomicStatus', 'iucnRedListCategory',
                       'year', 'month', 'day', 'country', 'countryCode', 'decimalLongitude', 'decimalLatitude',
                       'coordinateUncertaintyInMeters']


# using pygbif package
# requires a specific request-cache version: pip install requests-cache==0.5.2
# print(occ.search(scientificName = 'Loxodonta cyclotis'))


def get_occurrence_by_name(scientific_name, start_year, end_year, limit=50, offset=0):
    url = f'{gbif_api_occurrence_base}' \
          f'scientificName={scientific_name}&' \
          f'year={str(start_year)},{str(end_year)}&' \
          f'hasCoordinate=true&' \
          f'limit={limit}&' \
          f'offset={offset}'
    api_response = requests.get(url)
    api_data = api_response.json()
    df = pd.DataFrame.from_dict(api_data['results'])

    for col in columns_of_interest:
        if col not in df:
            df[col] = np.NaN

    return df[columns_of_interest]


def get_occurrence_by_country(country_code, start_year, end_year, limit=50, offset=0):
    url = f'{gbif_api_occurrence_base}' \
          f'country={country_code}&' \
          f'year={str(start_year)},{str(end_year)}&' \
          f'hasCoordinate=true&' \
          f'iucnRedListCategory=EN&' \
          f'iucnRedListCategory=CR&' \
          f'limit={limit}&' \
          f'offset={offset}'
    api_response = requests.get(url)
    api_data = api_response.json()
    df = pd.DataFrame.from_dict(api_data['results'])

    for col in columns_of_interest:
        if col not in df:
            df[col] = np.NaN

    return df[columns_of_interest]


def query_occurrence_by_name(scientific_name, start_year, end_year):
    url = f'{gbif_api_occurrence_base}' \
          f'scientificName={scientific_name}&' \
          f'year={str(start_year)},{str(end_year)}&' \
          f'hasCoordinate=true&' \
          f'limit=1&' \
          f'offset=0'
    nr_of_entries = int(requests.get(url).json()['count'])
    nr_of_batches = nr_of_entries // max_limit + 1

    results_df = []
    for i in range(0, nr_of_batches):
        df_i = get_occurrence_by_name(scientific_name, start_year, end_year, max_limit, i * max_limit)
        results_df.append(df_i)
    occ_df = pd.concat(results_df)
    return occ_df


def query_occurrence_by_country(country_code, start_year, end_year):
    url = f'{gbif_api_occurrence_base}' \
          f'country={country_code}&' \
          f'year={str(start_year)},{str(end_year)}&' \
          f'hasCoordinate=true&' \
          f'iucnRedListCategory=EN&' \
          f'iucnRedListCategory=CR&' \
          f'limit=1&' \
          f'offset=0'
    nr_of_entries = int(requests.get(url).json()['count'])
    nr_of_batches = nr_of_entries // max_limit + 1

    results_df = []
    for i in range(0, nr_of_batches):
        df_i = get_occurrence_by_country(country_code, start_year, end_year, max_limit, i * max_limit)
        results_df.append(df_i)
    occ_df = pd.concat(results_df)
    return occ_df


#res_df = query_occurrence_by_name('Loxodonta cyclotis', 2000, 2021)
#print(res_df.shape)
#print(res_df.head(20))

occ_country_df = query_occurrence_by_country('CA', 2015, 2021)
print(occ_country_df.shape)
print(occ_country_df.head(20))
print(occ_country_df['scientificName'].nunique())
