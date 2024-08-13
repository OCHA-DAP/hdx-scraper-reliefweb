import logging
from typing import List, Optional

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class ReliefWeb:
    _APP_NAME = "vocabulary"
    _DATE_FIELD = "date-event"
    _FILENAME = "reliefweb-disasters-list.csv"
    _LIMIT = 1000
    _LOCATION = "world"
    _PRESET = "external"

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._tags = self._create_tags()
        self._hxl_tags = self._create_hxl_tags()

    def scrape_data(self) -> list:
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
        data_url = f"{self._configuration["base_url"]}?appname={self._APP_NAME}&preset={self._PRESET}&limit={self._LIMIT}"
        data = self._retriever.download_json(data_url)
        disaster_list = data["data"]

        disasters_list = []
        for disaster in disaster_list:
            disaster_url = disaster["href"]

            try:
                disaster_data = self._retriever.download_json(disaster_url)
            except Exception as e:
                if "404" in str(e):
                    logger.info(
                        f"404 error for {disaster['fields']['name']}: {e}"
                    )
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
        dataset.add_tags(self._tags)

        resource_data = {
            "name": self._FILENAME,
            "description": "CSV with HXL tags of ongoing and past disasters covered by ReliefWeb",
        }

        dataset.generate_resource_from_iterable(
            list(disaster_list[0].keys()),
            disaster_list,
            self._hxl_tags,
            self._temp_dir,
            self._FILENAME,
            resource_data,
            self._DATE_FIELD,
            quickcharts=None,
        )

        return dataset

    def _create_tags(self) -> List[str]:
        logger.info("Generating tags")
        tags = self._configuration["fixed_tags"]
        return tags

    def _create_hxl_tags(self) -> List[str]:
        logger.info("Generating hxl tags")
        hxl_tags = self._configuration["hxl_tags"]
        return hxl_tags


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
