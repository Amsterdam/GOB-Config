from unittest import TestCase
from unittest.mock import patch

from gobconfig.datastore.config import (
    GOBConfigException,
    get_datastore_config,
    TYPE_POSTGRES,
    TYPE_OBJECTSTORE,
    TYPE_ORACLE,
    TYPE_SFTP,
    TYPE_SQL_SERVER,
    TYPE_BAG_EXTRACT,
    DATASTORE_CONFIGS
)


class TestConfig(TestCase):

    mock_config = {
        'DatastoreA': {
            'some': 'config',
        }
    }

    @patch("gobconfig.datastore.config.DATASTORE_CONFIGS", mock_config)
    def test_datastore_config(self):

        self.assertEqual({
            'some': 'config',
            'name': 'DatastoreA',
        }, get_datastore_config('DatastoreA'))

        with self.assertRaises(GOBConfigException):
            get_datastore_config('NonExistent')

    def test_valid_types(self):
        """Tests that all configs have a valid type set

        :return:
        """
        valid_types = [
            TYPE_ORACLE,
            TYPE_OBJECTSTORE,
            TYPE_POSTGRES,
            TYPE_SFTP,
            TYPE_SQL_SERVER,
            TYPE_BAG_EXTRACT,
        ]

        for name, config in DATASTORE_CONFIGS.items():
            self.assertTrue(config.get('type') in valid_types)
