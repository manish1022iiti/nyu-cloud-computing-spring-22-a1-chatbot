import json
from typing import Dict

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

def get_raw_json_items():
    rfile1 = "yelp/parsed_data/resto_data_manhattan_part3.json"
    # rfile2 = "yelp/parsed_data/resto_data_manhattan_part2.json"
    with open(rfile1, "r") as fp:
        data1: Dict = json.load(fp)
    # with open(rfile2, "r") as fp:
    #     data2: Dict = json.load(fp)
    # data1.update(data2)
    return data1


def get_documents():
    data = get_raw_json_items()
    documents = dict()
    for cuisine, cuisine_data in data.items():
        for rid, _ in cuisine_data.items():
            document = dict()
            document["cuisine"] = cuisine
            document["id"] = rid
            documents[rid] = document
    return documents


def main():
    # getting everything OPenSearch
    host = "search-domain2-pogtt2zz253ds6potcveo4gi2a.us-east-1.es.amazonaws.com"
    region = "us-east-1"
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    search = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    index_name = "restaurants"

    # getting data we want to push
    docs = get_documents()

    count = 0
    for id, document in docs.items():
        print(f'Adding document: {count + 1}/{len(docs)}')
        search.index(
            index = index_name,
            body = document,
            id = id,
            refresh = True
        )
        count += 1
        # print(response)

main()
