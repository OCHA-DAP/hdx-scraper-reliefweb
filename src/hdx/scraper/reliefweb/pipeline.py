#!/usr/bin/python
"""Reliefweb scraper"""

import logging
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._APP_NAME = "vocabulary"
        self._DATE_FIELD = "date-event"
        self._FILENAME = "reliefweb-disasters-list.csv"
        self._LIMIT = 1000
        self._LOCATION = "world"
        self._PRESET = "external"

    def scrape_data(self, max_items: Optional[int] = None) -> list:
        """
        Query the API and store the results in a list.

        The result for a single row will have the following format:
        {
            "id": "52108",
            "score": 1,
            "fields": {
                "id": 52108,
                "name": "Chad: Floods - Aug 2024",
                "status": "ongoing",
                "glide": "FL-2024-000139-TCD"
            },
            "href": "https://api.reliefweb.int/v1/disasters/52108"
        }
        """
        logger.info("Scraping data")
        data_url = f"{self._configuration['base_url']}?appname={self._APP_NAME}&preset={self._PRESET}&limit={self._LIMIT}"
        data = self._retriever.download_json(data_url)
        disaster_list = data["data"]

        # Use for testing
        if max_items is not None:
            disaster_list = disaster_list[:max_items]

        disasters_list = []
        for disaster in disaster_list:
            disaster_url = disaster["href"]

            disaster_data = None
            try:
                disaster_data = self._retriever.download_json(disaster_url)
            except Exception as e:
                if "404" in str(e):
                    logger.info(f"404 error for {disaster['fields']['name']}: {e}")
                else:
                    logger.error(
                        f"Error downloading data for {disaster['fields']['name']}: {e}"
                    )
                continue

            if not disaster_data:
                logger.info(f"No data for {disaster['fields']['name']}")
                continue

            flat_data = _format_data(disaster_data)
            disaster_fields = flat_data["data"][0]["fields"]

            # Remove items from the dictionary that are not necessary to the dataset
            remove_keys = [
                "uuid",
                "type-primary",
                "country-primary",
                "profile-overview",
                "profile-overview-html",
            ]
            for key in remove_keys:
                disaster_fields.pop(key, None)

            disasters_list.append(disaster_fields)

        return disasters_list

    def generate_dataset(self, disaster_list: list) -> Optional[Dataset]:
        """
        Generate the dataset
        """
        # Setup the dataset information
        title = "ReliefWeb Disasters List"
        slugified_name = slugify("ReliefWeb Disasters List")

        logger.info(f"Creating dataset: {title}")

        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )

        dataset.add_other_location(self._LOCATION)
        dataset.add_tags(self._configuration["tags"])

        resource_data = {
            "name": self._FILENAME,
            "description": "CSV with HXL tags of ongoing and past disasters covered by ReliefWeb",
        }

        dataset.generate_resource_from_iterable(
            list(disaster_list[0].keys()),
            disaster_list,
            {},
            self._tempdir,
            self._FILENAME,
            resource_data,
            self._DATE_FIELD,
            quickcharts=None,
        )

        return dataset


def _format_data(data: dict) -> dict:
    """
    Takes a dictionary and flattens any nested dictionaries or lists,
    stringing together the keys.
    """
    for d in data["data"]:
        d["fields"] = _flatten_data(d.pop("fields"))
    return data


def _flatten_data(data, sep: str = "-") -> dict:
    """
    The data contains fields with nested dictionaries and lists, where a
    sample field could contain a format like this:
    {
        "primary_type": {
            "id": 4611,
            "name": "Flood",
            "code": "FL"
        },
        "type": [
            {
                "id": 4624,
                "name": "Flash Flood",
                "code": "FF"
            },
            {
                "id": 4611,
                "name": "Flood",
                "code": "FL",
                "primary": true
            }
        ]
    }
    This function flattens the nested dictionaries and lists, so it would look like:
    {
        "primary_type-id": 4611,
        "primary_type-name": "Flood",
        "primary_type-code": "FL",
        "type-id": "4624, 4611",
        "type-name": "Flash Flood, Flood",
        "type-code": "FF, FL"
    }
    """
    flat_dict = {}

    def _flatten_inner(item, parent_key=""):
        if isinstance(item, dict):
            # Flatten nested dictionary
            for key, value in item.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                _flatten_inner(value, new_key)
        elif isinstance(item, list):
            # Flatten nested list that may also contain nested dictionaries
            list_items = {}
            for value in item:
                if isinstance(value, dict):
                    flattened_dict = {}
                    _flatten_inner(value, parent_key)
                    for k, v in flat_dict.items():
                        key_root = f"{parent_key}{sep}"
                        if k.startswith(key_root):
                            flattened_dict[k[len(key_root) :]] = v
                    for k, v in flattened_dict.items():
                        if k not in list_items:
                            list_items[k] = []
                        list_items[k].append(v)
                else:
                    if parent_key not in list_items:
                        list_items[parent_key] = []
                    list_items[parent_key].append(str(value))

            # Flatten collected list items into comma-separated strings
            for k, v in list_items.items():
                flat_dict[f"{parent_key}{sep}{k}"] = ", ".join(map(str, v))
        else:
            flat_dict[parent_key] = item

    _flatten_inner(data)
    return flat_dict
