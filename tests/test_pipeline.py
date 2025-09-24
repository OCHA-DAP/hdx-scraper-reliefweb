from os import getenv
from os.path import join

from dotenv import load_dotenv
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.reliefweb.pipeline import Pipeline

# Load local .env file if not running in GitHub Actions
if getenv("GITHUB_ACTIONS") is None:
    load_dotenv()


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        APP_NAME = getenv("APP_NAME")

        with temp_dir(
            "TestReliefweb",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir, APP_NAME)
                # Use the first 3 records for testing
                disaster_list = pipeline.scrape_data(3)

                assert list(disaster_list[0].keys()) == [
                    "id",
                    "name",
                    "description",
                    "status",
                    "glide",
                    "primary_country-href",
                    "primary_country-id",
                    "primary_country-name",
                    "primary_country-shortname",
                    "primary_country-iso3",
                    "primary_country-location-lat",
                    "primary_country-location-lon",
                    "primary_type-id",
                    "primary_type-name",
                    "primary_type-code",
                    "country-href",
                    "country-id",
                    "country-name",
                    "country-shortname",
                    "country-iso3",
                    "country-location-lat",
                    "country-location-lon",
                    "type-id",
                    "type-name",
                    "type-code",
                    "url",
                    "url_alias",
                    "date-changed",
                    "date-created",
                    "date-event",
                    "current",
                ]

                dataset = pipeline.generate_dataset(disaster_list=disaster_list[:3])
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "data_update_frequency": 7,
                    "dataset_date": "[2025-07-13T00:00:00 TO 2025-09-04T23:59:59]",
                    "dataset_source": "Multiple sources",
                    "groups": [{"name": "world"}],
                    "license_id": "cc-by",
                    "maintainer": "ab54dbbf-b25c-4c31-8bda-778ad2f39328",
                    "methodology": "Registry",
                    "name": "reliefweb-disasters-list",
                    "notes": "ReliefWeb is a humanitarian information service provided by the United Nations Office for the Coordination of Humanitarian Affairs (OCHA). ReliefWeb's editorial team monitors and collects information from more than 4,000 key sources, including humanitarian agencies at the international and local levels, governments, think-tanks and research institutions, and the media.\n\n[ReliefWeb disaster dataset](https://reliefweb.int/disasters) provides an overview of the situation and situation reports, news and press releases, assessments, evaluations, infographics and maps of natural disasters with humanitarian impact from 1981 until today.\n",
                    "owner_org": "de410fc7-6116-4283-9c26-67287aaa2634",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": False,
                    "tags": [
                        {
                            "name": "climate hazards",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "drought",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "earthquake-tsunami",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "natural disasters",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "ReliefWeb Disasters List",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "CSV containing data on ongoing and past disasters covered by ReliefWeb",
                        "format": "csv",
                        "name": "reliefweb-disasters-list.csv",
                    }
                ]
                for resource in resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(fixtures_dir, filename)
                    assert_files_same(actual, expected)
