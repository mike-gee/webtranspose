import os
from urllib.parse import urljoin

import requests

from .consts import WEBTRANSPOSE_API_URL


def run_webt_api(params, api_path, api_key=None):
    headers = {"X-API-Key": os.environ["WEBTRANSPOSE_API_KEY"] if api_key is None else api_key}
    api_endpoint = urljoin(WEBTRANSPOSE_API_URL, api_path)
    response = requests.post(api_endpoint, headers=headers, json=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("API request failed with status code: {}".format(response.status_code))
