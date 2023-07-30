import glob
import pandas as pd
import os

# constant
base_path = 'data/iucn_api_data/'
species_path = base_path + 'iucn_species_info/'
countries_path = base_path + 'iucn_country_occurrences_info/'
history_path = base_path + 'iucn_historical_info/'
habitat_path = base_path + 'iucn_habitats_info/'
threats_path = base_path + 'iucn_threats_info/'


# Helpers
def check_duplicate(df, row_name='taxonid_iucn'):
    df = df.copy()
    row = df[row_name]
    row_unique = row.duplicated()
    df_duplicated = df[row_unique]
    if df_duplicated.shape[0] > 0:
        print(f"found duplicates: {df_duplicated.shape[0]}")
        print(df_duplicated)
    else:
        print("No duplicates")

# helpers end


def load_csv_from_batches(path, keep_default_na=True):
    files = []
    for file in glob.glob(path + '*.csv'):
        df_i = pd.read_csv(file, keep_default_na=keep_default_na)
        files.append(df_i)
    df = pd.concat(files)
    df = df.set_index('taxonid_iucn')
    return df


def save_df_as_csv(dataframe, data_path, filename):
    dir_exists = os.path.exists(data_path)
    if not dir_exists:
        print("\ncreating dir...")
        os.makedirs(data_path)
    dataframe.to_csv(data_path + filename, index=False)


def get_merged_country_info():
    df = load_csv_from_batches(countries_path, keep_default_na=False).reset_index()
    country_continent_df = pd.read_csv('data/countryContinent.csv', keep_default_na=False)
    country_continent_df = country_continent_df[['code_2', 'continent', 'sub_region', 'code_3']]
    df = df.merge(country_continent_df, left_on='code', right_on='code_2')
    df = df.drop(columns=['code_2'])
    df = df.set_index('taxonid_iucn')
    return df


def get_merged_historical_info():
    return load_csv_from_batches(history_path)


def get_merged_habitat_info():
    return load_csv_from_batches(habitat_path)


def get_merged_threats_info():
    return load_csv_from_batches(threats_path)


def get_animalia_df():
    df = pd.read_csv(base_path + 'animalia.csv')
    df = df.set_index('taxonid_iucn')
    return df
