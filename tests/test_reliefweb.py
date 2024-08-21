import logging
from os.path import join

import pytest
from freezegun import freeze_time

from hdx.api.configuration import Configuration
from hdx.scraper.reliefweb.reliefweb import ReliefWeb
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def expected_json() -> dict:
    return {
        "id": 52092,
        "uuid": "9b380090-74a6-4086-9864-3497a185e0d6",
        "name": "Sudan: Floods - Jul 2024",
        "description": "Initial reports indicate that an estimated 10,180 people, most of whom are newly arrived IDPs from Sennar State, have been affected by recent heavy rains and flooding in Kassala State. [...] Heavy rains and flooding have also affected an unspecified number of people and homes in Aroma, Shamal Al Delta, Reifi Kassala, and Gharb Kassala localities. [...] Floodwater reportedly submerged tents and water and sanitation (WASH) facilities, as well as roads. The majority of the affected IDPs have been forced to live in the open on the roadsides and they do not have access to food, clean drinking water, or safe sanitation facilities amid heightened concerns of a possible spike in water-borne diseases. ([OCHA, 28 Jul 2024](https://reliefweb.int/node/4081549))\n\nMore than 17,000 people have been affected by heavy rains and flooding in parts of western and eastern Sudan since the onset of the rainy season. This includes an estimated 10,700 flood-affected people in Kassala State, most of whom fled recent hostilities in Sennar State; about 5,600 people in North Darfur; an estimated 500 people in East Darfur and another 210 people in Kulbus, West Darfur. Five people died, including two minors while swimming in the Gash River, while five people were injured in Kassala State. The actual number of affected people is yet to be determined, as authorities and humanitarian partners continue to assess the impact of heavy rains and flooding among host communities, refugees, and IDPs who settled in Kassala after the war broke out in April 2023. ([OCHA, 2 Aug 2024](https://reliefweb.int/node/4083398))",
        "status": "ongoing",
        "glide": "FL-2024-000128-SDN",
    }


@pytest.fixture(scope="module")
def expected_dataset():
    return {
        "data_update_frequency": 7,
        "dataset_date": "[2015-08-03T00:00:00 TO 2015-08-07T23:59:59]",
        "dataset_source": "ReliefWeb",
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
        ],
        "title": "ReliefWeb Disasters List",
    }


@pytest.fixture(scope="module")
def expected_resources():
    return [
        {
            "description": "CSV with HXL tags of ongoing and past disasters covered by ReliefWeb",
            "format": "csv",
            "name": "reliefweb-disasters-list.csv",
            "resource_type": "file.upload",
            "url_type": "upload",
        }
    ]


@pytest.fixture
def mock_get_mapped_tags(mocker):
    return mocker.patch(
        "hdx.data.vocabulary.Vocabulary.get_mapped_tags",
        return_value=(["climate hazards", "climate-weather", "drought"], []),
    )


class TestReliefWeb:
    @pytest.fixture(scope="class")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="dev",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "reliefweb", "config")

    @freeze_time("2024-07-30")
    def test_reliefweb(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
        expected_json,
        expected_dataset,
        expected_resources,
        mock_get_mapped_tags,
    ):
        with temp_dir(
            "TestReliefWeb",
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

                reliefweb = ReliefWeb(
                    configuration=configuration,
                    retriever=retriever,
                    temp_dir=tempdir,
                )

                disaster_list = reliefweb.scrape_data()

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
                    "description-html",
                ]

                dataset = reliefweb.generate_dataset(
                    disaster_list=disaster_list
                )
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == expected_dataset
                resources = dataset.get_resources()
                assert resources == expected_resources

                filename_list = [
                    "reliefweb-disasters-list.csv",
                ]
                for filename in filename_list:
                    assert_files_same(
                        join("tests", "fixtures", filename),
                        join(tempdir, filename),
                    )
