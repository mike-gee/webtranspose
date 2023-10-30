import os
from urllib.parse import urljoin

import requests


def run_webt_api(params: dict, api_path: str, api_key: str = None) -> dict:
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
    WEBTRANSPOSE_API_URL = "https://api.webtranspose.com/"
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")
    headers = {"X-API-Key": api_key}
    api_endpoint = urljoin(WEBTRANSPOSE_API_URL, api_path)
    response = requests.post(api_endpoint, headers=headers, json=params, timeout=180)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("API request failed with status code: {}".format(response.status_code))
