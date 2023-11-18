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
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup

from .webt_api import run_webt_api


class Crawl:
    def __init__(
        self,
        url: str,
        allowed_urls: List[str] = [],
        banned_urls: List[str] = [],
        n_workers: int = 1,
        max_pages: int = 15,
        render_js: bool = False,
        output_dir: str = "webtranspose-out",
        verbose: bool = False,
        api_key: Optional[str] = None,
        _created: bool = False,
    ) -> None:
        """
        Initialize the Crawl object.

        :param url: The base URL to start crawling from.
        :param allowed_urls: A list of allowed URLs to crawl.
        :param banned_urls: A list of banned URLs to exclude from crawling.
        :param n_workers: The number of worker tasks to use for crawling.
        :param max_pages: The maximum number of pages to crawl.
        :param render_js: Whether to render JavaScript on crawled pages.
        :param output_dir: The directory to store the crawled data.
        :param verbose: Whether to print verbose logging messages.
        :param api_key: The API key to use for webt_api calls.
        """
        self.api_key = api_key
        if self.api_key is None:
            self.api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

        self.base_url = url
        self.allowed_urls = allowed_urls
        self.banned_urls = banned_urls
        self.max_pages = max_pages
        self.queue = asyncio.Queue()
        self.queue.put_nowait(
            {
                "url": self.base_url,
                "parent_urls": [],
            }
        )
        self.output_dir = output_dir
        self.visited_urls = {}
        self.failed_urls = set()
        self.ignored_urls = set()
        self.n_workers = n_workers
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.created = _created
        self.render_js = render_js
        self.crawl_id = None
        if self.api_key is None:
            self.crawl_id = str(uuid.uuid4())
        self.verbose = verbose

        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")
        if api_key is None and self.api_key is None:
            logging.warning(
                "No Web Transpose API provided. Lite version in use...\n\nTo run your Web Crawl on the Web Transpose API, set the WEBTRANSPOSE_API_KEY from https://webtranspose.com. Run cheaper with logging and advanced analytics."
            )

    @staticmethod
    async def crawl_worker(
        name: str,
        queue: asyncio.Queue,
        crawl_id: str,
        visited_urls: Dict[str, str],
        allowed_urls: List[str],
        failed_urls: Set[str],
        banned_urls: List[str],
        output_dir: str,
        base_url: str,
        max_pages: int,
        leftover_queue: asyncio.Queue,
        ignored_queue: asyncio.Queue,
        verbose: bool,
    ) -> None:
        """
        Worker function for crawling URLs.

        :param name: The name of the worker.
        :param queue: The queue of URLs to crawl.
        :param crawl_id: The ID of the crawl.
        :param visited_urls: A dictionary of visited URLs and their file paths.
        :param allowed_urls: A list of allowed URLs to crawl.
        :param banned_urls: A list of banned URLs to exclude from crawling.
        :param output_dir: The directory to store the crawled data.
        :param base_url: The base URL of the crawl.
        :param max_pages: The maximum number of pages to crawl.
        :param leftover_queue: The queue for leftover URLs.
        :param ignored_queue: The queue for ignored URLs.
        :param verbose: Whether to print verbose logging messages.
        """

        def _lint_url(url: str) -> str:
            """
            Lint the given URL by removing the fragment component.

            :param url: The URL to lint.
            :return: The linted URL.
            """
            parsed_url = urlparse(url)
            cleaned_url = parsed_url._replace(fragment="")
            return urlunparse(cleaned_url)

        if verbose:
            logging.info(f"{name}: Starting crawl of {base_url}")
        while max_pages is None or len(visited_urls) < max_pages or not queue.empty():
            curr_url_data = await queue.get()
            curr_url = curr_url_data["url"]
            parent_urls = curr_url_data["parent_urls"]
            base_url_netloc = urlparse(base_url).netloc
            if (
                (
                    (
                        urlparse(curr_url).netloc == base_url_netloc
                        and not any(fnmatch(curr_url, banned) for banned in banned_urls)
                    )
                    or any(fnmatch(curr_url, allowed) for allowed in allowed_urls)
                )
                and curr_url not in visited_urls
                and len(visited_urls) < max_pages
            ):
                base_dir = os.path.join(output_dir, base_url_netloc)
                if not os.path.exists(base_dir):
                    os.makedirs(base_dir)
                filename = urllib.parse.quote_plus(curr_url).replace("/", "_")
                filepath = os.path.join(base_dir, filename) + ".json"
                async with httpx.AsyncClient() as client:
                    try:
                        page = await client.get(curr_url)
                    except:
                        failed_urls.add(curr_url)
                        queue.task_done()
                        continue

                    page_title = None
                    page_html = None
                    page_text = None
                    try:
                        page_type = "html"
                        soup = BeautifulSoup(page.content, "lxml")
                        page_title = soup.title.string if soup.title else ""
                        page_html = page.content.decode("utf-8")
                        page_text = soup.get_text()
                        child_urls = list(
                            set(
                                [
                                    _lint_url(urljoin(base_url, link.get("href")))
                                    for link in soup.find_all(href=True)
                                ]
                            )
                        )
                        for url in child_urls:
                            if url.startswith("http"):
                                queue.put_nowait(
                                    {
                                        "url": url,
                                        "parent_urls": parent_urls + [curr_url],
                                    }
                                )
                    except:
                        child_urls = []
                        page_type = "other"

                    visited_urls[curr_url] = filepath
                    data = {
                        "crawl_id": crawl_id,
                        "url": curr_url,
                        "type": page_type,
                        "title": page_title,
                        "date": datetime.now().isoformat(),
                        "parent_urls": parent_urls,
                        "child_urls": child_urls,
                        "html": page_html,
                        "text": page_text,
                    }
                    with open(filepath, "w") as f:
                        json.dump(data, f)

            elif curr_url not in visited_urls and (
                urlparse(curr_url).netloc == urlparse(base_url).netloc
                or any(fnmatch(curr_url, allowed) for allowed in allowed_urls)
            ):
                leftover_queue.put_nowait(
                    {
                        "url": curr_url,
                        "parent_urls": parent_urls,
                    }
                )

            else:
                ignored_queue.put_nowait(curr_url)

            queue.task_done()

    def create_crawl_api(self):
        """
        Creates a Crawl on https://webtranspose.com
        """
        if self.verbose:
            logging.info(f"Creating crawl of {self.base_url} on Web Transpose...")
        create_json = {
            "url": self.base_url,
            "render_js": self.render_js,
            "max_pages": self.max_pages,
            "allowed_urls": self.allowed_urls,
            "banned_urls": self.banned_urls,
        }
        out_json = run_webt_api(
            create_json,
            "v1/crawl/create",
            self.api_key,
        )
        self.crawl_id = out_json["crawl_id"]
        self.created = True

    def queue_crawl(self):
        """
        Resume crawling of Crawl object. Don't wait for it to finish crawling.
        """
        if self.verbose:
            logging.info(f"Starting crawl of {self.base_url} on Web Transpose...")

        if self.api_key is None:
            logging.error("Cannot queue a local crawl. Please use the crawl() method.")

        else:
            if not self.created:
                self.create_crawl_api()
            queue_json = {
                "crawl_id": self.crawl_id,
            }
            out = run_webt_api(
                queue_json,
                "v1/crawl/resume",
                self.api_key,
            )

    async def crawl(self):
        """
        Resume crawling of Crawl object.
        """
        if self.verbose:
            logging.info(f"Starting crawl of {self.base_url}...")
        if self.api_key is None:
            leftover_queue = asyncio.Queue()
            ignored_queue = asyncio.Queue()
            tasks = []
            for i in range(self.n_workers):
                task = asyncio.create_task(
                    self.crawl_worker(
                        f"worker-{i}",
                        self.queue,
                        self.crawl_id,
                        self.visited_urls,
                        self.allowed_urls,
                        self.failed_urls,
                        self.banned_urls,
                        self.output_dir,
                        self.base_url,
                        self.max_pages,
                        leftover_queue,
                        ignored_queue,
                        self.verbose,
                    )
                )
                tasks.append(task)

            await self.queue.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.queue = leftover_queue
            self.ignored_urls = list(ignored_queue._queue)
            self.to_metadata()
        else:
            self.queue_crawl()
            status = self.status()
            while status["num_queued"] + status["num_visited"] + status["num_ignored"] == 0:
                await asyncio.sleep(5)
                status = self.status()

            if (status["num_failed"] > 0) and (
                status["num_queued"] + status["num_visited"] + status["num_ignored"] == 0
            ):
                raise Exception("The first page crawled failed")

            while status["num_queued"] > 0 and status["num_visited"] < status["max_pages"]:
                await asyncio.sleep(5)
                status = self.status()
        return self

    def get_queued(self, max_pages: int = 30) -> list:
        """
        Get a list of URLs from the queue.

        Args:
            max_pages (int): The number of URLs to retrieve from the queue. Defaults to 30.

        Returns:
            list: A list of URLs from the queue.
        """
        if self.api_key is None:
            urls = []
            for _ in range(max_pages):
                try:
                    url = self.queue.get_nowait()
                    urls.append(url)
                except asyncio.QueueEmpty:
                    break
            for url in urls:
                self.queue.put_nowait(url)
            return urls
        else:
            if not self.created:
                return [self.base_url]
            queue_json = {
                "crawl_id": self.crawl_id,
                "max_pages": max_pages,
            }
            out_json = run_webt_api(
                queue_json,
                "v1/crawl/get-queue",
                self.api_key,
            )
            return out_json["urls"]

    def set_allowed_urls(self, allowed_urls: list) -> "Crawl":
        """
        Set the allowed URLs for the crawl.

        Args:
            allowed_urls (list): A list of allowed URLs.

        Returns:
            self: The Crawl object.
        """
        self.allowed_urls = allowed_urls
        if not self.created:
            self.to_metadata()
        else:
            update_json = {
                "crawl_id": self.crawl_id,
                "allowed_urls": allowed_urls,
            }
            run_webt_api(
                update_json,
                "v1/crawl/set-allowed",
                self.api_key,
            )
        return self

    def set_banned_urls(self, banned_urls: list) -> "Crawl":
        """
        Set the banned URLs for the crawl.

        Args:
            banned_urls (list): A list of banned URLs.

        Returns:
            self: The Crawl object.
        """
        self.banned_urls = banned_urls
        if not self.created:
            self.to_metadata()
        else:
            update_json = {
                "crawl_id": self.crawl_id,
                "banned_urls": banned_urls,
            }
            run_webt_api(
                update_json,
                "v1/crawl/set-banned",
                self.api_key,
            )
        return self

    def get_filename(self, url: str) -> str:
        """
        Get the filename associated with a visited URL.

        Args:
            url (str): The visited URL.

        Returns:
            str: The filename associated with the visited URL.

        Raises:
            ValueError: If the URL is not found in the visited URLs.
        """
        try:
            return self.visited_urls[url]
        except KeyError:
            raise ValueError(f"URL {url} not found in visited URLs")

    def set_max_pages(self, max_pages: int) -> "Crawl":
        """
        Set the maximum number of pages to crawl.

        Args:
            max_pages (int): The maximum number of pages to crawl.

        Returns:
            self: The Crawl object.
        """
        if not self.created:
            self.max_pages = max_pages
            self.to_metadata()
        else:
            max_pages_json = {
                "crawl_id": self.crawl_id,
                "max_pages": max_pages,
            }
            run_webt_api(
                max_pages_json,
                "v1/crawl/set-max-pages",
                self.api_key,
            )
        return self

    def status(self) -> dict:
        """
        Get the status of the Crawl object.

        Returns:
            dict: The status of the Crawl object.
        """
        if not self.created:
            status_json = {
                "crawl_id": self.crawl_id,
                "loc": "local" if self.api_key is None else "cloud",
                "base_url": self.base_url,
                "max_pages": self.max_pages,
                "num_visited": len(self.visited_urls),
                "num_ignored": len(self.ignored_urls),
                "num_failed": len(self.failed_urls),
                "num_queued": self.queue.qsize(),
                "banned_urls": self.banned_urls,
                "allowed_urls": self.allowed_urls,
            }
            status_json["n_workers"] = self.n_workers
            return status_json

        status_json = {
            "crawl_id": self.crawl_id,
        }
        crawl_status = run_webt_api(
            status_json,
            "v1/crawl/get",
            self.api_key,
        )
        crawl_status["loc"] = "cloud"
        if self.verbose:
            logging.info(f"Status of crawl {self.crawl_id}: {crawl_status}")
        return crawl_status

    def get_ignored(self) -> list:
        """
        Get a list of ignored URLs.

        Returns:
            list: A list of ignored URLs.
        """
        if not self.created:
            return list(self.ignored_urls)

        ignored_json = {
            "crawl_id": self.crawl_id,
        }
        out_json = run_webt_api(
            ignored_json,
            "v1/crawl/get/ignored",
            self.api_key,
        )
        return out_json["pages"]

    def get_failed(self) -> list:
        """
        Get a list of failed URLs.

        Returns:
            list: A list of failed URLs.
        """
        if not self.created:
            return list(self.failed_urls)

        visited_json = {
            "crawl_id": self.crawl_id,
        }
        out_json = run_webt_api(
            visited_json,
            "v1/crawl/get/failed",
            self.api_key,
        )
        return out_json["pages"]

    def get_visited(self) -> list:
        """
        Get a list of visited URLs.

        Returns:
            list: A list of visited URLs.
        """
        if not self.created:
            return list(self.visited_urls)

        visited_json = {
            "crawl_id": self.crawl_id,
        }
        out_json = run_webt_api(
            visited_json,
            "v1/crawl/get/visited",
            self.api_key,
        )
        return out_json["pages"]

    def get_banned(self) -> list:
        """
        Get a list of banned URLs.

        Returns:
            list: A list of banned URLs.
        """
        if not self.created:
            return list(self.banned_urls)

        banned_json = {
            "crawl_id": self.crawl_id,
        }
        out_json = run_webt_api(
            banned_json,
            "v1/crawl/get/banned",
            self.api_key,
        )
        return out_json["pages"]

    def download(self):
        """
        Download the output of the crawl.
        """
        if self.verbose:
            logging.info(f"Downloading crawl of {self.base_url}...")

        if self.created:
            download_json = {
                "crawl_id": self.crawl_id,
            }
            out_json = run_webt_api(
                download_json,
                "v1/crawl/download",
                self.api_key,
            )
            presigned_url = out_json["url"]
            with tempfile.TemporaryDirectory() as tmpdir:
                zip_file_path = os.path.join(tmpdir, "temp.zip")
                with open(zip_file_path, "wb") as f:
                    response = httpx.get(presigned_url)
                    f.write(response.content)

                with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdir)

                for root, _, files in os.walk(tmpdir):
                    for file in files:
                        if file.endswith(".json"):
                            json_file = os.path.join(root, file)
                            with open(json_file, "r") as f:
                                data = json.load(f)
                            url = data["url"]
                            base_url_netloc = urlparse(self.base_url).netloc
                            base_dir = os.path.join(self.output_dir, base_url_netloc)
                            if not os.path.exists(base_dir):
                                os.makedirs(base_dir)
                            filename = urllib.parse.quote_plus(url).replace("/", "_")
                            filepath = os.path.join(base_dir, filename) + ".json"
                            shutil.move(json_file, filepath)

        logging.info(f"The output of the crawl can be found at: {self.output_dir}")

    def to_metadata(self) -> None:
        """
        Save the metadata of the Crawl object to a file.
        """
        if not self.created:
            filename = os.path.join(self.output_dir, f"{self.crawl_id}.json")
            metadata = {
                "crawl_id": self.crawl_id,
                "n_workers": self.n_workers,
                "base_url": self.base_url,
                "max_pages": self.max_pages,
                "visited_urls": self.visited_urls,
                "ignored_urls": list(self.ignored_urls),
                "render_js": self.render_js,
                "queue": list(self.queue._queue),
                "banned_urls": self.banned_urls,
                "allowed_urls": self.allowed_urls,
                "output_dir": self.output_dir,
            }
            with open(filename, "w") as file:
                json.dump(metadata, file)

    @staticmethod
    def from_metadata(crawl_id: str, output_dir: str = "webtranspose-out") -> "Crawl":
        """
        Create a Crawl object from metadata stored in a file.

        Args:
            crawl_id (str): The ID of the crawl.
            output_dir (str, optional): The directory to store the crawled data. Defaults to "webtranspose-out".

        Returns:
            Crawl: The Crawl object.
        """
        filename = os.path.join(output_dir, f"{crawl_id}.json")
        with open(filename, "r") as file:
            metadata = json.load(file)
        crawl = Crawl(
            metadata["base_url"],
            metadata["allowed_urls"],
            metadata["banned_urls"],
            metadata["n_workers"],
            metadata["max_pages"],
            render_js=metadata["render_js"],
            output_dir=metadata["output_dir"],
        )
        crawl.crawl_id = metadata["crawl_id"]
        crawl.visited_urls = metadata["visited_urls"]
        crawl.ignored_urls = set(metadata["ignored_urls"])
        crawl.queue = asyncio.Queue()
        for url in metadata["queue"]:
            crawl.queue.put_nowait(url)
        return crawl

    @staticmethod
    def from_cloud(crawl_id: str, api_key: Optional[str] = None) -> "Crawl":
        """
        Create a Crawl object from metadata stored in the cloud.

        Args:
            crawl_id (str): The ID of the crawl.
            api_key (str, optional): The API key for accessing the cloud. Defaults to None.

        Returns:
            Crawl: The Crawl object.
        """
        if api_key is None:
            api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

        if api_key is not None:
            get_json = {
                "crawl_id": crawl_id,
            }
            out_json = run_webt_api(get_json, "v1/crawl/get", api_key)
            crawl = Crawl(
                out_json["base_url"],
                out_json["allowed_urls"],
                out_json["banned_urls"],
                max_pages=out_json["max_pages"],
                render_js=out_json["render_js"],
                api_key=api_key,
                _created=True,
            )
            crawl.crawl_id = out_json["crawl_id"]
            return crawl

        raise ValueError(
            "API key not found. Please set WEBTRANSPOSE_API_KEY environment variable or pass api_key argument."
        )

    def __str__(self) -> str:
        """
        Get a string representation of the Crawl object.

        Returns:
            str: The string representation of the Crawl object.
        """
        status = self.status()
        return (
            f"WebTransposeCrawl(\n"
            f"  Crawl ID: {status['crawl_id']}\n"
            f"  Number of Workers: {status['n_workers'] if 'n_workers' in status else 'cloud'}\n"
            f"  Base URL: {status['base_url']}\n"
            f"  Max Pages: {status['max_pages']}\n"
            f"  Number of Visited URLs: {status['num_visited']}\n"
            f"  Number of Ignored URLs: {status['num_ignored']}\n"
            f"  Number of Queued URLs: {status['num_queued']}\n"
            f"  Number of Failed URLs: {status['num_failed']}\n"
            f"  Banned URLs: {status['banned_urls']}\n"
            f"  Allowed URLs: {status['allowed_urls']}"
            f")"
        )

    def __repr__(self) -> str:
        """
        Get a string representation of the Crawl object.

        Returns:
            str: The string representation of the Crawl object.
        """
        status = self.status()
        return (
            f"WebTransposeCrawl(\n"
            f"  Crawl ID: {status['crawl_id']}\n"
            f"  Number of Workers: {status['n_workers'] if 'n_workers' in status else 'cloud'}\n"
            f"  Base URL: {status['base_url']}\n"
            f"  Max Pages: {status['max_pages']}\n"
            f"  Number of Visited URLs: {status['num_visited']}\n"
            f"  Number of Ignored URLs: {status['num_ignored']}\n"
            f"  Number of Queued URLs: {status['num_queued']}\n"
            f"  Number of Failed URLs: {status['num_failed']}\n"
            f"  Banned URLs: {status['banned_urls']}\n"
            f"  Allowed URLs: {status['allowed_urls']}"
            f")"
        )

    def get_page(self, url: str) -> dict:
        """
        Get the page data for a given URL.

        Args:
            url (str): The URL of the page.

        Returns:
            dict: The page data.
        """
        if not self.created:
            fn = self.visited_urls[url]
            try:
                with open(fn, "r") as f:
                    data = json.load(f)
                    return data
            except:
                logging.error(f"Could not find HTML for URL {url}")
        else:
            get_json = {
                "crawl_id": self.crawl_id,
                "url": url,
            }
            out_json = run_webt_api(
                get_json,
                "v1/crawl/get-page",
                self.api_key,
            )
            return out_json

    def get_child_urls(self, url: str) -> list:
        """
        Get the child URLs for a given URL.

        Args:
            url (str): The URL.

        Returns:
            list: A list of child URLs.
        """
        if not self.created:
            try:
                fn = self.visited_urls[url]
            except:
                logging.error(f"Could not find child URLs for URL {url}")
                return None
            try:
                with open(fn, "r") as f:
                    data = json.load(f)
                    return data["child_urls"]
            except:
                logging.error(f"Could not find child URLs for URL {url}")
        else:
            get_json = {
                "crawl_id": self.crawl_id,
                "url": url,
            }
            out_json = run_webt_api(
                get_json,
                "v1/crawl/get-child-urls",
                self.api_key,
            )
            return out_json

    def retry_failed_urls(self) -> None:
        """
        Queue failed URLs from a crawl.
        """
        if not self.created:
            logging.error("Cannot retry failed URLs for un-created crawl.")
        elif self.api_key is not None:
            queue_json = {
                "crawl_id": self.crawl_id,
            }
            run_webt_api(
                queue_json,
                "v1/crawl/retry-failed",
                self.api_key,
            )


def get_crawl(crawl_id: str, api_key: Optional[str] = None) -> Crawl:
    """
    Get a Crawl object based on the crawl ID.

    Args:
        crawl_id (str): The ID of the crawl.
        api_key (str, optional): The API key. Defaults to None.

    Returns:
        Crawl: The Crawl object.
    """
    try:
        return Crawl.from_metadata(crawl_id)
    except FileNotFoundError:
        return Crawl.from_cloud(crawl_id, api_key=api_key)


def list_crawls(loc: str = "cloud", api_key: Optional[str] = None) -> list:
    """
    List all available crawls.

    Args:
        loc (str, optional): The location of the crawls. Defaults to 'cloud'.
        api_key (str, optional): The API key. Defaults to None.

    Returns:
        list: A list of Crawl objects.
    """
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

    if api_key is not None and loc == "cloud":
        crawl_list_data = run_webt_api(
            {},
            "v1/crawl/list",
            api_key,
        )
        return crawl_list_data["crawls"]

    elif loc == "local" or api_key is None:
        crawls = []
        for filename in os.listdir("."):
            if filename.endswith(".json"):
                crawls.append(Crawl.from_metadata(filename[:-5]))
        return crawls


def retry_failed(crawl_id: str, api_key: Optional[str] = None) -> None:
    """
    Queue failed URLs from a crawl.

    Args:
        crawl_id (str): The ID of the crawl.
        api_key (str, optional): The API key. Defaults to None.
    """
    if api_key is None:
        api_key = os.environ.get("WEBTRANSPOSE_API_KEY")

    if api_key is not None:
        queue_json = {
            "crawl_id": crawl_id,
        }
        run_webt_api(
            queue_json,
            "v1/crawl/retry-failed",
            api_key,
        )
