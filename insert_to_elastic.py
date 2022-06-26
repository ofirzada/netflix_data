import pandas as pd
from elasticsearch import helpers
from elasticsearch import Elasticsearch
import json

def safe_value(field_val):
    return field_val if not pd.isna(field_val) else "Other"

def doc_generator(df):
    es_client = Elasticsearch(http_compress=True)

    df_iter = df.iterrows()
    for index, document in df_iter:
        json_object = json.dumps(document.to_dict())

        try:
            es_client.index(index="netflix_show", id=f"{document['show_id']}", doc_type="_doc",body=
            json_object)
        except:
            print('object not inserted to elastic')
            print(json_object)


def inset_to_elastic(df):
    es_client = Elasticsearch(http_compress=True)

    df['country'] = df['country'].apply(safe_value)
    df['cast'] = df['cast'].apply(safe_value)
    df['director'] = df['director'].apply(safe_value)


    doc_generator(df)

    resp = es_client.search(index="netflix_show", body={})
    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print(hit["_source"])
