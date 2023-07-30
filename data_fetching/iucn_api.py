import requests
import pandas as pd
import numpy as np
import time
import os
#import iucn_data

# constants
token = '9bb4facb6d23f48efbf424bb05c0c1ef1cf6f468393bc745d42179ac4aca5fee'
iucn_api_base = 'https://apiv3.iucnredlist.org/api/v3/'
data_path = '../data/iucn_api_data/'


def get_species_by_category(categories):
    url = iucn_api_base + 'species/category/' + categories + '/?token=' + token
    response = requests.get(url)
    return response


def get_species_by_id(taxon_id):
    url = iucn_api_base + 'species/id/' + str(taxon_id) + '/?token=' + token
    response = requests.get(url)
    return response


def get_country_for_species_id(taxon_id):
    url = iucn_api_base + 'species/countries/id/' + str(taxon_id) + '/?token=' + token
    response = requests.get(url)
    return response


def get_habitats_for_species_id(taxon_id):
    url = iucn_api_base + 'habitats/species/id/' + str(taxon_id) + '/?token=' + token
    response = requests.get(url)
    return response


def get_historical_for_species_id(taxon_id):
    url = iucn_api_base + 'species/history/id/' + str(taxon_id) + '/?token=' + token
    response = requests.get(url)
    return response


def get_threats_for_species_id(taxon_id):
    url = iucn_api_base + 'threats/species/id/' + str(taxon_id) + '/?token=' + token
    response = requests.get(url)
    return response


# split work helper
def get_batches(df_to_process):
    batch_size = len(df_to_process) // 10
    batches = []
    for g, df in df_to_process.groupby(np.arange(len(df_to_process)) // batch_size):
        batches.append(df)
    return batches


def print_progress(current, total, post_fix=""):
    percent = int(current/total*100)
    print("", end="\r%i/%i | %i pct | %s" % (current, total, percent, post_fix))


# start aggregation functions
def query_species_to_csv():
    categories = ['CR', 'EN']
    results_df = []
    for cat in categories:
        api_response = get_species_by_category(cat)
        api_data = api_response.json()
        df = pd.DataFrame.from_dict(api_data['result'])
        df['threatLevelCategory'] = cat
        results_df.append(df)
    iucn_df = pd.concat(results_df)
    iucn_df.rename(columns={'taxonid': 'taxonid_iucn'}, inplace=True)
    iucn_df.to_csv(data_path + 'iucn_species_list.csv', index=False)


def query_species_detail_data(start, end, delay=3):
    species_list = pd.read_csv(data_path + 'iucn_species_list.csv')
    batches = get_batches(species_list)
    for i in range(start, end):
        df_i = batches[i]
        results_df = []
        for index, row in df_i.iterrows():
            if index > 0 and index % 100 == 0:
                print(f'\nwaiting for {delay} sec')
                time.sleep(delay)
            name = row['scientific_name']
            taxon_id = row['taxonid_iucn']
            print("", end=f'\r getting: {name}; {index} of {df_i.shape[0]*(i+1)}')
            api_result = get_species_by_id(taxon_id)
            api_data = api_result.json()
            df_rest = pd.DataFrame.from_dict(api_data['result'])
            results_df.append(df_rest)
        result_df = pd.concat(results_df)
        result_df.rename(columns={'taxonid': 'taxonid_iucn'}, inplace=True)
        # write and check existence
        path_to_dir = data_path + '/iucn_species_info'
        dir_exists = os.path.exists(path_to_dir)
        if not dir_exists:
            print("\ncreating dir...")
            os.makedirs(path_to_dir)
        result_df.to_csv(path_to_dir + f'/batch_{i}.csv', index=False)


def query_historical_data_for_species(start, end, species_list, delay=3):
    batches = get_batches(species_list)
    for i in range(start, end):
        df_i = batches[i]
        results_df = []
        j = 1
        for index, row in df_i.iterrows():
            if j > 1 and j % 100 == 0:
                print(f'\nwaiting for {delay} sec')
                time.sleep(delay)
            name = row['scientific_name']
            taxon_id = row['taxonid_iucn']
            print_progress(j, df_i.shape[0], name)
            j += 1
            api_result = get_historical_for_species_id(taxon_id)
            api_data = api_result.json()
            df_rest = pd.DataFrame.from_dict(api_data['result'])
            df_rest['taxonid_iucn'] = taxon_id
            results_df.append(df_rest)
        result_df = pd.concat(results_df)
        # write and check existence
        path_to_dir = data_path + '/iucn_historical_info'
        dir_exists = os.path.exists(path_to_dir)
        if not dir_exists:
            print("\ncreating dir...")
            os.makedirs(path_to_dir)
        result_df.to_csv(path_to_dir + f'/batch_{i}.csv', index=False)


def query_country_occurrences_for_species(start, end, species_list, delay=3):
    batches = get_batches(species_list)
    for i in range(start, end):
        df_i = batches[i]
        results_df = []
        j = 1
        for index, row in df_i.iterrows():
            if j > 1 and j % 100 == 0:
                print(f'\nwaiting for {delay} sec')
                time.sleep(delay)
            name = row['scientific_name']
            taxon_id = row['taxonid_iucn']
            print_progress(j, df_i.shape[0], name)
            j += 1
            api_result = get_country_for_species_id(taxon_id)
            api_data = api_result.json()
            df_rest = pd.DataFrame.from_dict(api_data['result'])
            df_rest['taxonid_iucn'] = taxon_id
            results_df.append(df_rest)
        result_df = pd.concat(results_df)
        # write and check existence
        path_to_dir = data_path + '/iucn_country_occurrences_info'
        dir_exists = os.path.exists(path_to_dir)
        if not dir_exists:
            print("\ncreating dir...")
            os.makedirs(path_to_dir)
        result_df.to_csv(path_to_dir + f'/batch_{i}.csv', index=False)


def query_habitats_for_species(start, end, species_list, delay=3):
    batches = get_batches(species_list)
    for i in range(start, end):
        df_i = batches[i]
        results_df = []
        j = 1
        for index, row in df_i.iterrows():
            if j > 1 and j % 100 == 0:
                print(f'\nwaiting for {delay} sec')
                time.sleep(delay)
            name = row['scientific_name']
            taxon_id = row['taxonid_iucn']
            print_progress(j, df_i.shape[0], name)
            j += 1
            api_result = get_habitats_for_species_id(taxon_id)
            api_data = api_result.json()
            df_rest = pd.DataFrame.from_dict(api_data['result'])
            df_rest['taxonid_iucn'] = taxon_id
            results_df.append(df_rest)
        result_df = pd.concat(results_df)
        # write and check existence
        path_to_dir = data_path + '/iucn_habitats_info'
        dir_exists = os.path.exists(path_to_dir)
        if not dir_exists:
            print("\ncreating dir...")
            os.makedirs(path_to_dir)
        result_df.to_csv(path_to_dir + f'/batch_{i}.csv', index=False)


def query_threat_data_for_species(start, end, species_list, delay=3):
    batches = get_batches(species_list)
    for i in range(start, end):
        df_i = batches[i]
        results_df = []
        j = 1
        for index, row in df_i.iterrows():
            if j > 1 and j % 100 == 0:
                print(f'\nwaiting for {delay} sec')
                time.sleep(delay)
            name = row['scientific_name']
            taxon_id = row['taxonid_iucn']
            print_progress(j, df_i.shape[0], name)
            j += 1
            api_result = get_threats_for_species_id(taxon_id)
            api_data = api_result.json()
            df_rest = pd.DataFrame.from_dict(api_data['result'])
            df_rest['taxonid_iucn'] = taxon_id
            results_df.append(df_rest)
        result_df = pd.concat(results_df)
        # write and check existence
        path_to_dir = data_path + '/iucn_threats_info'
        dir_exists = os.path.exists(path_to_dir)
        if not dir_exists:
            print("\ncreating dir...")
            os.makedirs(path_to_dir)
        result_df.to_csv(path_to_dir + f'/batch_{i}.csv', index=False)


# use reduced data set for additional infos, only query stuff for animalia
#data_filtered = iucn_data.get_merged_data()

#query_threat_data_for_species(0, 1, data_filtered, 10)


