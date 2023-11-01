import json
import os

import openai
import tiktoken


class OpenAIScraper:
    def __init__(
        self,
        chunk_size: int = 2500,
        overlap_size: int = 100,
    ):
        """
        Initialize the OpenAIScraper.

        Args:
            chunk_size (int, optional): The size of each chunk of text to process. Defaults to 2500.
            overlap_size (int, optional): The size of the overlap between chunks. Defaults to 100.
        """
        self.api_key = os.environ["OPENAI_API_KEY"]
        self.encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
        self.chunk_size = chunk_size
        self.overlap_size = overlap_size

    @staticmethod
    def process_html(
        text: str, chunk_size: int, overlap_size: int, encoding: tiktoken.Encoding
    ) -> list:
        """
        Process the HTML text into chunks.

        Args:
            text (str): The HTML text to process.
            chunk_size (int): The size of each chunk of text.
            overlap_size (int): The size of the overlap between chunks.
            encoding (tiktoken.Encoding): The encoding object.

        Returns:
            list: A list of decoded chunks.
        """
        encoded = encoding.encode(text)
        if overlap_size >= chunk_size:
            raise ValueError("Overlap size should be less than chunk size.")
        chunks = []
        idx = 0
        while idx < len(encoded):
            end_idx = idx + chunk_size
            chunks.append(encoded[idx:end_idx])
            idx = end_idx - overlap_size
        decoded_chunks = [encoding.decode(chunk) for chunk in chunks]
        return decoded_chunks

    def scrape(self, html: str, schema: dict) -> dict:
        """
        Scrape the HTML text using the provided schema.

        Args:
            html (str): The HTML text to scrape.
            schema (dict): The schema to use for scraping.

        Returns:
            dict: The scraped data.
        """
        processed_schema = self.transform_schema(schema)
        schema_keys = ", ".join(processed_schema.keys())
        out_data = {}

        for sub_html in self.process_html(html, self.chunk_size, self.overlap_size, self.encoding):
            model = "gpt-3.5-turbo-0613"
            if len(self.encoding.encode(sub_html)) > 2500:
                model = "gpt-3.5-turbo-16k"

            response = openai.ChatCompletion.create(
                model=model,
                temperature=0,
                messages=[{"role": "user", "content": sub_html}],
                functions=[
                    {
                        "name": "extract_info",
                        "description": f"Extract the {schema_keys} from the website text if any exist. Empty if not found.",
                        "parameters": {
                            "type": "object",
                            "properties": processed_schema,
                            "required": list(processed_schema.keys()),
                        },
                    },
                ],
            )
            out = response["choices"][0]["message"]

            if "function_call" in out:
                args = json.loads(out["function_call"]["arguments"])

                for k in args.keys():
                    if k in processed_schema:
                        if processed_schema[k]["type"] == "array":
                            if k not in out_data:
                                out_data[k] = []
                            out_data[k] += args[k]
                        else:
                            out_data[k] = args[k]
                            del processed_schema[k]
                    elif k not in out_data:
                        out_data[k] = None

        return out_data

    def transform_schema(self, schema: dict) -> dict:
        """
        Transform the schema into the format required by OpenAI.

        Args:
            schema (dict): The schema to transform.

        Returns:
            dict: The transformed schema.
        """
        openai_type_map = {
            "str": "string",
            "int": "number",
            "bool": "boolean",
        }

        properties = {}
        for key, value in schema.items():
            if isinstance(value, dict):
                if "type" in value and value["type"] == "array":
                    properties[key] = {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": self.transform_schema(value["items"]),
                        },
                        "required": list(value["items"].keys()),
                    }
                elif "type" in value:
                    properties[key] = value
                else:
                    properties[key] = self.transform_schema(value)
            elif isinstance(value, list):
                try:
                    properties[key] = {
                        "type": openai_type_map[type(value[0]).__name__],
                        "enum": value,
                        "description": key,
                    }
                except IndexError:
                    raise Exception(f"Empty list for key {key}")
            else:
                properties[key] = {
                    "type": value,
                    "description": key,
                }

        return properties
