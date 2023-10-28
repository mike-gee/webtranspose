import os
from urllib.parse import urljoin

import requests

from .consts import WEBTRANSPOSE_API_URL


def run_webt_api(params, api_path, api_key=None):
    """
    Run a WebTranspose API request.

    Args:
        params (dict): The parameters for the API request.
        api_path (str): The API path.
        api_key (str, optional): The API key. Defaults to None.

    Returns:
        dict: The JSON response from the API.

    Raises:
        Exception: If the API request fails with a non-200 status code.
    """
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")
    headers = {"X-API-Key": api_key}
    api_endpoint = urljoin(WEBTRANSPOSE_API_URL, api_path)
    response = requests.post(api_endpoint, headers=headers, json=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("API request failed with status code: {}".format(response.status_code))
