


import pandas as pd

from insert_to_elastic import inset_to_elastic
from datetime import datetime
def get_before_2016(df):

    return df[(df['release_year'] < 2016)]


def add_current_time(df):
    df= df.assign(current_time=lambda t: datetime.now().utcnow())
    df['current_time'] = df['current_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def add_catagories_count(df):

    for index in df['listed_in'].index:
        df.at[index,"listed_in"]+="|"+str(len(df.at[index,"listed_in"].split(',')))

    return df



def add_duration(df):
    return df.assign(duration=(df['listed_in'].str.split(',')).str.len())


#
# def set_duraction(data):
#     # if value.split(" ")[1]=='min'
#     print(data.values[1])
#     # duration_data=value.str.split(' ')
#     #
#     #
#     #
#     # if duration_data.str[1] == 'min':
#     #     print(duration_data.str[1])
#     print("start")
#     for items in data.values:
#          try:
#             duration_data =(items.split(' '))
#             if duration_data[1]=='min':
#                 print(duration_data)
#          except AttributeError:
#              print("data was faulty, Attribute error")

#


def determain_duraction(data):

    try:

        duration_data = data.split(' ')

        if [1] == 'min':
            return int(duration_data[0])*60
        else:

            return int(duration_data[0])*300

    except AttributeError:

        print("data was faulty, Attribute error")
        return 0

def set_duration_in_seconds(df):
    return df.assign(duration_in_seconds=lambda d: d['duration'].apply(determain_duraction))



def get_director_avg_time(df):
    print(df.groupby('director').mean()['duration_in_seconds'])

def get_non_unique_categories_per_country(df):
    print(df.groupby('country')['duration_in_seconds'].mean()['duration_in_seconds'])



    ## Replace NaN (null) Values with Zero
    df.fillna(0, inplace=True)

    # Rename the Columns to be Camel Case
    # def camel_case_string(string):
    #     string = sub(r"(_|-)+", " ", string).title().replace(" ", "")
    #     string = string[0].lower() + string[1:]
    #     return string
    #
    # df.columns = [camel_case_string(x) for x in df.columns]

    ## Save the Data into Elasticsearch
    ed.pandas_to_eland(
        pd_df=df,
        es_client=es,
        # Where the data will live in Elasticsearch
        es_dest_index="netflix_show",
        # Type overrides for certain columns, the default is keyword
        # name has been set to free text and year to a date field.
        # es_type_overrides={
        #     "name": "text",
        #     "year": "date"
        # },
        # If the index already exists replace it
        es_if_exists="replace",
        # Wait for data to be indexed before returning
        es_refresh=True,
    )

if __name__ == '__main__':
    df = pd.read_csv('netflix_titles.csv')
    df = get_before_2016(df)
    df = add_current_time(df)
    df = add_catagories_count(df)
    df = set_duration_in_seconds(df)
    #get_director_avg_time(df)
    #get_non_unique_categories_per_country(df)
    inset_to_elastic(df)
    #print(df.head(10).to_string())
