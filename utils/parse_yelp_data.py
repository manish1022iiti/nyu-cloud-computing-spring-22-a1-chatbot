# -*- coding: utf-8 -*-
"""
Yelp Fusion API code sample.
This program demonstrates the capability of the Yelp Fusion API
by using the Search API to query for businesses by a search term and location,
and the Business API to query additional information about the top result
from the search query.
Please refer to http://www.yelp.com/developers/v3/documentation for the API
documentation.
This program requires the Python requests library, which you can install via:
`pip install -r requirements.txt`.
Sample usage of the program:
`python sample.py --term="bars" --location="San Francisco, CA"`
"""
from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib


# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
API_KEY= "TEJmktpoPL2HMazAEQIPzE7eIuDj5pTJONOG91nPi1SSUPpmj-WTDISHx-QrCguH-gqRGn0WDmv47ZTzPk77i0nqpEZk8gdMZ2uIlCrNJ-myF6HWzhaw_6-iwKoWYnYx"


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 3


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    # print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location, search_limit, offset):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': search_limit,
        "offset": offset
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):
    """Query the Business API by a business ID.
    Args:
        business_id (str): The ID of the business to query.
    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(term, location, search_limit, offset):
    """Queries the API by the input values from the user.
    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    data = list()

    response = search(API_KEY, term, location, search_limit, offset)
    businesses = response.get('businesses')

    if businesses:
        for i, business in enumerate(businesses):
            print(f"Cuisine: {term}, Location: {location}, business no.: {i + 1}/{len(businesses)}")
            business_id = business['id']

            # print(u'{0} businesses found, querying business info ' \
            #     'for the top result "{1}" ...'.format(
            #         len(businesses), business_id))
            try:
                response = get_business(API_KEY, business_id)
                data.append(response)
            except:
                continue

    # print(u'Result for business "{0}" found:'.format(business_id))
    # pprint.pprint(response, indent=2)
    return data


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
                        type=str, help='Search term (default: %(default)s)')
    parser.add_argument('-l', '--location', dest='location',
                        default=DEFAULT_LOCATION, type=str,
                        help='Search location (default: %(default)s)')

    input_values = parser.parse_args()

    try:
        query_api(input_values.term, input_values.location)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )

def test():
    file_name = "parsed_data/resto_data_manhattan_part3.json"
    # file_name = "../temp/temp.json"

    cuisine_types = ["indian", "japanese", "chinese", "thai", "italian", "mexican", "lebanese"]
    # cuisine_types = ["italian", "mexican", "lebanese"]
    # cuisine_types = ["indian"]
    locations = ["Brooklyn"]
    # search_limit = 1100
    search_limit = 160
    # search_limit = 100
    # search_limit = min(505, 1005 // len(locations))
    # search_limit = min(20, 1000 // len(locations))

    for cuisine_type in cuisine_types:
        for location in locations:
            hits = 0
            while hits < search_limit:
                # getting data
                data = query_api(term=cuisine_type, location=location,
                                 search_limit=min(50, search_limit), offset=hits + 1)

                if not data:
                    print(f"Cuisine: {cuisine_type}, Location: {location}, NO businesses found!")
                else:
                    new_data = dict()
                    for j, resto in enumerate(data):
                        # some restaurants' data does NOT seem to have "id" attribute; skipping such names
                        try:
                            new_data[resto["id"]] = resto
                        except:
                            print(f"Cuisine: {cuisine_type}, Location: {location}, NO id found for"
                                  f" {j}/{len(data)} data point!")
                            continue

                    # reading current data and updating it with new data
                    with open(file_name, "r") as fp:
                        curr_data = json.load(fp)
                    if cuisine_type not in curr_data:
                        curr_data[cuisine_type] = dict()
                    for resto_id, resto_data in new_data.items():
                        curr_data[cuisine_type][resto_id] = resto_data

                    # putting updated data back into json
                    with open(file_name, "w") as fp:
                        json.dump(curr_data, fp)

                hits = hits + min(50, search_limit)

if __name__ == '__main__':
    test()
