import asyncio
import json
import logging
import os
import shutil
import tempfile
import urllib.parse
import uuid
import zipfile
from datetime import datetime
from fnmatch import fnmatch
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse
from time import sleep

import httpx
from bs4 import BeautifulSoup

from .webt_api import run_webt_api


class Chatbot:
    def __init__(
        self,
        url_list: List[str] = [],
        name: str = None,
        max_pages: int = 100,
        api_key: str = None,
        verbose: bool = False,
        chatbot_id: str = None,
        _created: bool = False,
    ) -> None:
        self.api_key = api_key
        if self.api_key is None:
            self.api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

        if self.api_key is None:
            raise ValueError(
                "No Web Transpose API provided. \n\nTo Crawl on the Web Transpose API, set the WEBTRANSPOSE_API_KEY from https://webtranspose.com. Run cheaper with logging and advanced analytics."
            )

        self.url_list = url_list
        self.name = name
        self.max_pages = max_pages
        self.verbose = verbose
        self.chatbot_id = chatbot_id
        self.created = _created

        if not self.chatbot_id:
            self.create()

    def create(self):
        if not self.chatbot_id:
            self._create_chat()
            status = self.status()
            while status['status'] != 'complete':
                if self.verbose:
                    logging.info("Waiting for chat to be created...")
                sleep(5)
                status = self.status()

        else:
            logging.info("Chat already created.")

    def queue_create(self):
        if not self.chatbot_id:
            self._create_chat()
        else:
            logging.info("Chat already created.")

    def _create_chat(self):
        if self.verbose:
            logging.info("Creating chat...")

        if self.chatbot_id is None:
            create_json = {
                "name": self.name,
                "max_pages": self.max_pages,
                "url_list": self.url_list,
            }
            out_json = run_webt_api(
                create_json, 
                "v1/chat/create", 
                self.api_key
            )
            self.chatbot_id = out_json['chatbot_id']
    
    def query_database(self, query, num_records=3):
        if self.verbose:
            logging.info("Querying database...")

        if not self.chatbot_id:
            self.create()

        query_json = {
            "chatbot_id": self.chatbot_id,
            "query": query,
            "num_records": num_records,
        }
        out = run_webt_api(
            query_json,
            "v1/chat/database/query",
            self.api_key
        )
        return out['results']

    def status(self):
        if self.verbose:
            logging.info("Getting chat...")

        if not self.chatbot_id:
            self.create()

        get_json = {
            "chatbot_id": self.chatbot_id,
        }
        out = run_webt_api(
            get_json,
            "v1/chat/get",
            self.api_key
        )
        return out['chatbot']

    def add_urls(self, url_list):
        if self.verbose:
            logging.info("Querying database...")

        if not self.chatbot_id:
            self.create()

        query_json = {
            "chatbot_id": self.chatbot_id,
            "max_pages": self.max_pages,
            "url_list": url_list,
        }
        run_webt_api(
            query_json,
            "v1/chat/urls/add",
            self.api_key
        )

    def delete_crawls(self, crawl_id_list):
        if self.verbose:
            logging.info("Querying database...")

        if not self.chatbot_id:
            self.create()

        query_json = {
            "chatbot_id": self.chatbot_id,
            "crawl_id_list": crawl_id_list,
        }
        run_webt_api(
            query_json,
            "v1/chat/crawls/delete",
            self.api_key
        )



