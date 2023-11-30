import os
import requests

from .webt_api import run_webt_api


def search(query):
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

    if api_key is not None:
        out_json = run_webt_api(
            {
                "query": query,
            },
            "/v1/search",
            api_key,
        )
        return out_json

    raise ValueError("Must provide api_key or set WEBTRANSPOSE_API_KEY in environment variables.")


def search_filter(query):
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

    if api_key is not None:
        out_json = run_webt_api(
            {
                "query": query,
            },
            "/v1/search/filter",
            api_key,
        )
        return out_json

    raise ValueError("Must provide api_key or set WEBTRANSPOSE_API_KEY in environment variables.")