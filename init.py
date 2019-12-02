try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

from base64 import urlsafe_b64encode
import hashlib
import requests
import json
import pandas as pd
from flatten_json import flatten
from pandas.io.json import json_normalize #package for flattening json in pandas df

data_dict = {}

def webshrinker_categories_v3(access_key, secret_key, url=b"", params={}):
    params['key'] = access_key

    request = "categories/v3/{}?{}".format(urlsafe_b64encode(url).decode('utf-8'), urlencode(params, True))
    request_to_sign = "{}:{}".format(secret_key, request).encode('utf-8')
    signed_request = hashlib.md5(request_to_sign).hexdigest()

    return "https://api.webshrinker.com/{}&hash={}".format(request, signed_request)

access_key = "ENTER ACCESS KEY"
secret_key = "ENTER SECRET KEY"

with open('url_list.txt', 'rb') as url_file:
    for line in url_file:

        api_url = webshrinker_categories_v3(access_key, secret_key, line)
        response = requests.get(api_url)

        status_code = response.status_code

        data = response.json()
        data_dict = data['data']
        dn = json_normalize(data_dict)

        df1 = pd.DataFrame.from_dict(dn['categories'][0])
        df1.drop(df1.tail(1).index, inplace=True)
        df2 = pd.DataFrame.from_dict(dn['url'])
        df3 = df1.join(df2, lsuffix='_caller', rsuffix='_other')

        if status_code == 200:
            print(df3)
            df3.to_csv('export.csv', mode='a', header=False)
        elif status_code == 202:
            # The website is being visited and the categories will be updated shortly
            print("202 Next")
            df3.to_csv('export.csv', mode='a', header=False)
        elif status_code == 400:
            # Bad or malformed HTTP request
            print("Bad or malformed HTTP request")
            print(json.dumps(data, indent=4, sort_keys=True))
        elif status_code == 401:
            # Unauthorized
            print("Unauthorized - check your access and secret key permissions")
            print(json.dumps(data, indent=4, sort_keys=True))
        elif status_code == 402:
            # Request limit reached
            print("Account request limit reached")
            print(json.dumps(data, indent=4, sort_keys=True))
        else:
            # General error occurred
            print("A general error occurred, try the request again")
