import logging
import os
from time import sleep
from typing import List

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
        """
        Initialize a Chatbot instance.

        :param url_list: A list of URLs to crawl.
        :param name: The name of the chatbot.
        :param max_pages: The maximum number of pages to crawl.
        :param api_key: The API key for accessing the Web Transpose API.
        :param verbose: Whether to enable verbose logging.
        :param chatbot_id: The ID of an existing chatbot.
        :param _created: Whether the chatbot has already been created.
        """
        self.api_key = api_key
        if self.api_key is None:
            self.api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

        if self.api_key is None:
            raise ValueError(
                "No Web Transpose API provided. \n\nTo use Chatbots, set the WEBTRANSPOSE_API_KEY from https://webtranspose.com."
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
        """
        Create a chatbot.
        """
        if not self.chatbot_id:
            self._create_chat()
            status = self.status()
            while status["status"] != "complete":
                if self.verbose:
                    logging.info("Waiting for chat to be created...")
                sleep(5)
                status = self.status()

        else:
            logging.info("Chat already created.")

    def queue_create(self):
        """
        Queue the creation of a chatbot.
        """
        if not self.chatbot_id:
            self._create_chat()
        else:
            logging.info("Chat already created.")

    def _create_chat(self):
        """
        Create a chat.
        """
        if self.verbose:
            logging.info("Creating chat...")

        if self.chatbot_id is None:
            create_json = {
                "name": self.name,
                "max_pages": self.max_pages,
                "url_list": self.url_list,
            }
            out_json = run_webt_api(create_json, "v1/chat/create", self.api_key)
            self.chatbot_id = out_json["chatbot_id"]

    def query_database(self, query: str, num_records: int = 3) -> list:
        """
        Query the database of the chatbot.

        :param query: The query string.
        :param num_records: The number of records to return.
        :return: The query results.
        """
        if self.verbose:
            logging.info("Querying database...")

        if not self.chatbot_id:
            self.create()

        query_json = {
            "chatbot_id": self.chatbot_id,
            "query": query,
            "num_records": num_records,
        }
        out = run_webt_api(query_json, "v1/chat/database/query", self.api_key)
        return out["results"]

    def status(self):
        """
        Get the status of the chatbot.

        :return: The chatbot status.
        """
        if self.verbose:
            logging.info("Getting chat...")

        if not self.chatbot_id:
            self.create()

        get_json = {
            "chatbot_id": self.chatbot_id,
        }
        out = run_webt_api(get_json, "v1/chat/get", self.api_key)
        return out["chatbot"]

    def add_urls(self, url_list: list):
        """
        Add URLs to the chatbot.

        :param url_list: A list of URLs to add.
        """
        if self.verbose:
            logging.info("Querying database...")

        if not self.chatbot_id:
            self.create()

        query_json = {
            "chatbot_id": self.chatbot_id,
            "max_pages": self.max_pages,
            "url_list": url_list,
        }
        run_webt_api(query_json, "v1/chat/urls/add", self.api_key)

    def delete_crawls(self, crawl_id_list: list):
        """
        Delete crawls from the chatbot.

        :param crawl_id_list: A list of crawl IDs to delete.
        """
        if self.verbose:
            logging.info("Querying database...")

        if not self.chatbot_id:
            self.create()

        query_json = {
            "chatbot_id": self.chatbot_id,
            "crawl_id_list": crawl_id_list,
        }
        run_webt_api(query_json, "v1/chat/crawls/delete", self.api_key)


def get_chatbot(chatbot_id: str, api_key = None) -> Chatbot:
    """
    Get a chatbot.

    :param chatbot_id: The ID of the chatbot.
    :return: The chatbot.
    """
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")
        if api_key is None:
            raise ValueError(
                "No Web Transpose API provided. \n\nTo use Chatbots, set the WEBTRANSPOSE_API_KEY from https://webtranspose.com."
            )
    get_json = {
        "chatbot_id": chatbot_id,
    }
    chat_json = run_webt_api(get_json, "v1/chat/get", api_key)
    chatbot_data = chat_json.get('chatbot', {})
    chatbot = Chatbot(
        chatbot_id=chatbot_data.get('id'),
        name=chatbot_data.get('name'),
        max_pages=chatbot_data.get('num_run', 100),
        verbose=False,
        _created=True
    )
    return chatbot