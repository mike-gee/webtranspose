import json
import logging
import os
import re
import uuid

import requests
from bs4 import BeautifulSoup

from .openai import OpenAIScraper
from .webt_api import run_webt_api


class Scraper:
    def __init__(
        self,
        schema: dict,
        scraper_id: str = None,
        name: str = None,
        render_js: bool = False,
        verbose: bool = False,
        scraper: OpenAIScraper = None,
        api_key: str = None,
        _created: bool = False,
    ):
        """
        Initialize the Scraper object.

        Args:
            schema (dict): The schema for scraping.
            scraper_id (str, optional): The ID of the scraper. Defaults to None.
            name (str, optional): The name of the scraper. Defaults to None.
            render_js (bool, optional): Whether to render JavaScript. Defaults to False.
            verbose (bool, optional): Whether to print verbose output. Defaults to False.
            scraper (OpenAIScraper, optional): The scraper object. Defaults to None.
            api_key (str, optional): The API key. Defaults to None.
            _created (bool, optional): Whether the scraper has been created. Defaults to False.
        """
        self.api_key = api_key
        if self.api_key is None:
            self.api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

        self.name = name
        if self.name is None:
            self.name = "New Scraper"
        self.schema = schema
        self.verbose = verbose
        self.scraper = scraper
        self.render_js = render_js
        self.scraper_id = scraper_id
        if self.scraper is None:
            self.scraper = OpenAIScraper()
        if self.scraper_id is None:
            self.scraper_id = str(uuid.uuid4())
        self.created = _created

        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")
        if api_key is None and self.api_key is None:
            logging.warning(
                "No Web Transpose API provided. Lite version in use...\n\nTo run the actual WebT AI Web Scraper the Web Transpose API, set the WEBTRANSPOSE_API_KEY from https://webtranspose.com. Run cheaper with logging and advanced analytics."
            )

    def __str__(self) -> str:
        """
        Get a string representation of the Scraper object.

        Returns:
            str: The string representation of the Scraper object.
        """
        status = self.status()
        schema = json.dumps(status["schema"], indent=4)
        return (
            f"WebTransposeScraper(\n"
            f"  Status ID: {status['scraper_id']}\n"
            f"  Name: {status['name']}\n"
            f"  Render JS: {status['render_js']}\n"
            f"  Schema: {schema}\n"
            f")"
        )

    def __repr__(self) -> str:
        """
        Get a string representation of the Scraper object.

        Returns:
            str: The string representation of the Scraper object.
        """
        status = self.status()
        schema = json.dumps(status["schema"], indent=4)
        return (
            f"WebTransposeScraper(\n"
            f"  Status ID: {status['scraper_id']}\n"
            f"  Name: {status['name']}\n"
            f"  Render JS: {status['render_js']}\n"
            f"  Schema: {schema}\n"
            f")"
        )

    def create_scraper_api(self):
        """
        Creates a Scraper on https://webtranspose.com
        """
        if self.verbose:
            logging.info(f"Creating AI Web Scraper on Web Transpose...")

        create_json = {
            "name": self.name,
            "schema": self.schema,
            "render_js": self.render_js,
        }
        out_json = run_webt_api(
            create_json,
            "/v1/scraper/create",
            self.api_key,
        )
        self.scraper_id = out_json["scraper_id"]
        self.created = True

    def scrape(self, url=None, html=None, timeout=30):
        """
        Scrape the data from a given URL or HTML.

        Args:
            url (str, optional): The URL to scrape. Defaults to None.
            html (str, optional): The HTML to scrape. Defaults to None.
            timeout (int, optional): The timeout for the request. Defaults to 30.

        Returns:
            dict: The scraped data.

        Raises:
            ValueError: If neither URL nor HTML is provided.
        """
        if self.verbose:
            logging.info(f"Running Scraper({self.name}) on {url}...")

        if self.api_key is None:
            if url is not None:
                response = requests.get(url, timeout=timeout)
                soup = BeautifulSoup(response.content, "html.parser")
                body = soup.body
                html = re.sub("\s+", " ", str(body)).strip()

            if html is None:
                raise ValueError("Must provide either a url or html.")

            return self.scraper.scrape(
                html,
                self.schema,
            )
        else:
            if not self.created:
                self.create_scraper_api()

            scrape_json = {
                "scraper_id": self.scraper_id,
                "url": url,
                "html": html,
            }
            out_json = run_webt_api(
                scrape_json,
                "/v1/scraper/scrape",
                self.api_key,
            )
            return out_json

    def status(self):
        """
        Get the status of the Scraper.

        Returns:
            dict: The status of the Scraper.
        """
        if self.api_key is None or not self.created:
            return {
                "scraper_id": self.scraper_id,
                "name": self.name,
                "verbose": self.verbose,
                "render_js": self.render_js,
                "schema": self.schema,
            }
        else:
            get_json = {
                "scraper_id": self.scraper_id,
            }
            out_api = run_webt_api(
                get_json,
                "/v1/scraper/get",
                self.api_key,
            )
            scraper = out_api["scraper"]
            return {
                "scraper_id": scraper["id"],
                "name": scraper["name"],
                "verbose": self.verbose,
                "render_js": scraper["render_js"],
                "schema": scraper["schema"],
            }


def get_scraper(scraper_id, api_key: str = None):
    """
    Get a Scraper object based on the scraper ID.

    Args:
        scraper_id (str): The ID of the scraper.
        api_key (str, optional): The API key. Defaults to None.

    Returns:
        Scraper: The Scraper object.

    Raises:
        ValueError: If api_key is not provided.
    """
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

    if api_key is not None:
        get_json = {
            "scraper_id": scraper_id,
        }
        out_json = run_webt_api(
            get_json,
            "/v1/scraper/get",
            api_key,
        )
        scraper = out_json["scraper"]
        return Scraper(
            scraper_id=scraper["id"],
            name=scraper["name"],
            schema=scraper["schema"],
            render_js=scraper["render_js"],
            api_key=api_key,
            _created=True,
        )

    raise ValueError("Must provide api_key or set WEBTRANSPOSE_API_KEY in environment variables.")


def list_scrapers(api_key: str = None):
    """
    List all available scrapers.

    Args:
        api_key (str, optional): The API key. Defaults to None.

    Returns:
        list: A list of Scrapers.

    Raises:
        ValueError: If api_key is not provided.
    """
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

    if api_key is not None:
        out_json = run_webt_api(
            {},
            "/v1/scraper/list",
            api_key,
        )
        return out_json["scrapers"]

    raise ValueError("Must provide api_key or set WEBTRANSPOSE_API_KEY in environment variables.")
