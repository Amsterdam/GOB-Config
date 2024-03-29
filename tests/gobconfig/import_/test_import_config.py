import json

from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobconfig.exception import GOBConfigException
from gobconfig.import_.import_config import (
    get_dataset_file_location,
    _build_dataset_locations_mapping,
    get_import_definition,
    get_import_definition_by_filename,
    get_absolute_filepath,
    get_mapping,
    get_query
)

from collections import defaultdict


class TestImportConfig(TestCase):
    mock_locations_mapping = {
        "catalogue_a": {
            "collection_a": {
                "application_a": "cat_a_col_a_app_a.json",
            },
            "collection_b": {
                "application_a": "cat_a_col_b_app_a.json",
            },
        },
        "catalogue_b": {
            "collection_b": {
                "application_a": "cat_b_col_b_app_a.json",
            },
        },
        "catalogue_c": {
            "collection_a": {
                "application_a": "cat_c_col_a_app_a.json",
                "application_b": "cat_c_col_a_app_b.json",
            }
        },
        "catalogue_d": {
            "collection_a": {
                "application_a": "cat_c_col_a_app_a.json",
                "application_b": "cat_c_col_a_app_b.json",
                "_default": "the_default.json",
            }
        }

    }

    @patch("gobconfig.import_.import_config.dataset_locations_mapping", mock_locations_mapping)
    def test_get_dataset_file_location_explicit_application(self):
        self.assertEqual(self.mock_locations_mapping["catalogue_b"]["collection_b"]["application_a"],
                         get_dataset_file_location("catalogue_b", "collection_b", "application_a"))

        with self.assertRaisesRegex(GOBConfigException, "No dataset found"):
            get_dataset_file_location("catalogue_b", "collection_c_non_existent", "application_a")

    @patch("gobconfig.import_.import_config.dataset_locations_mapping", mock_locations_mapping)
    def test_get_dataset_file_location_implicit_application(self):
        self.assertEqual(self.mock_locations_mapping["catalogue_b"]["collection_b"]["application_a"],
                         get_dataset_file_location("catalogue_b", "collection_b"))

    @patch("gobconfig.import_.import_config.dataset_locations_mapping", mock_locations_mapping)
    def test_get_dataset_file_location_implicit_ambiguous_applications(self):
        with self.assertRaisesRegex(GOBConfigException, "Multiple applications"):
            get_dataset_file_location("catalogue_c", "collection_a")

    @patch("gobconfig.import_.import_config.dataset_locations_mapping", mock_locations_mapping)
    def test_get_dataset_file_location_multiple_applications_default(self):
        self.assertEqual("the_default.json", get_dataset_file_location("catalogue_d", "collection_a"))

    def defaultdict_to_dict(self, o):
        if isinstance(o, defaultdict):
            o = {k: self.defaultdict_to_dict(v) for k, v in o.items()}
        return o

    @patch("gobconfig.import_.import_config.get_mapping")
    @patch("gobconfig.import_.import_config.os")
    @patch("gobconfig.import_.import_config.DATASET_DIR", "")
    def test_build_dataset_locations_mapping_double_application(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file1.json', 'file2.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.side_effect = lambda x: {
            'file1.json': {
                'catalogue': 'somecatalogue',
                'entity': 'someentity',
                'source': {
                    'application': 'some_application'
                }
            },
            'file2.json': {
                'catalogue': 'somecatalogue',
                'entity': 'someentity',
                'source': {
                    'application': 'some_application'
                }
            }
        }[x]

        with self.assertRaisesRegex(GOBConfigException, "Have multiple import definitions"):
            _build_dataset_locations_mapping()

    @patch("gobconfig.import_.import_config.get_mapping")
    @patch("gobconfig.import_.import_config.os")
    @patch("gobconfig.import_.import_config.DATASET_DIR", "")
    def test_build_dataset_locations_mapping_double_default(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file1.json', 'file2.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.side_effect = lambda x: {
            'file1.json': {
                'catalogue': 'somecatalogue',
                'entity': 'someentity',
                'default': True,
                'source': {
                    'application': 'some_application'
                }
            },
            'file2.json': {
                'catalogue': 'somecatalogue',
                'entity': 'someentity',
                'default': True,
                'source': {
                    'application': 'some_application2'
                }
            }
        }[x]

        with self.assertRaisesRegex(GOBConfigException, "Have multiple defaults for"):
            _build_dataset_locations_mapping()

    @patch("gobconfig.import_.import_config.get_mapping")
    @patch("gobconfig.import_.import_config.os")
    @patch("gobconfig.import_.import_config.DATASET_DIR", "mocked/data/dir/")
    def test_build_dataset_locations_mapping(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.return_value = {
            'catalogue': 'somecatalogue',
            'entity': 'someentity',
            'default': True,
            'source': {
                'application': 'some_application'
            }
        }

        expected_result = {
            'somecatalogue': {
                'someentity': {
                    'some_application': 'mocked/data/dir/file.json',
                    '_default': 'mocked/data/dir/file.json',
                }
            }
        }

        result = _build_dataset_locations_mapping()
        self.assertEqual(expected_result, self.defaultdict_to_dict(result))

    @patch("gobconfig.import_.import_config.get_mapping")
    @patch("gobconfig.import_.import_config.os")
    @patch("gobconfig.import_.import_config.DATASET_DIR", "mocked/data/dir/")
    def test_build_dataset_locations_mapping_invalid_dict(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.return_value = {
            'catalogue': 'somecatalogue',
            'source': {
                'application': 'some_application'
            }
        }

        with self.assertRaisesRegex(GOBConfigException, "Dataset file mocked/data/dir/file.json invalid"):
            _build_dataset_locations_mapping()

    @patch("gobconfig.import_.import_config.get_mapping")
    @patch("gobconfig.import_.import_config.os")
    @patch("gobconfig.import_.import_config.DATASET_DIR", "mocked/data/dir/")
    def test_build_dataset_locations_mapping_invalid_json(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.side_effect = json.decoder.JSONDecodeError("", MagicMock(), 0)

        with self.assertRaisesRegex(GOBConfigException, "Dataset file mocked/data/dir/file.json invalid"):
            _build_dataset_locations_mapping()

    @patch("gobconfig.import_.import_config.get_query")
    @patch("builtins.open")
    @patch("gobconfig.import_.import_config.json.load")
    @patch("gobconfig.import_.import_config.get_absolute_filepath", lambda x: '/path/to/' + x)
    def test_get_mapping(self, mock_load, mock_open, mock_get_query):
        mock_load.return_value = {
            'source': {
                'application_config': {
                    'filename': 'the_filename',
                }
            }
        }
        mock_file = mock_open.return_value.__enter__.return_value

        result = get_mapping('filename')

        self.assertEqual('/path/to/the_filename', result['source']['application_config']['filepath'])
        mock_load.assert_called_with(mock_file)

        mock_load.return_value = {
            'source': {
                'application_config': {
                    'filename': 'the_filename',
                },
                'query': []
            }
        }
        result = get_mapping('filename')
        self.assertEqual(result['source']['query'], [])

        mock_load.return_value = {
            'source': {
                'application_config': {
                    'filename': 'the_filename',
                },
                'query': 'some filename'
            }
        }
        mock_get_query.return_value = 'query'
        result = get_mapping('filename')
        self.assertEqual(result['source']['query'], 'query')

    @patch("builtins.open")
    def test_get_query(self, mock_open):
        mock_file = MagicMock()
        mock_file.readlines.return_value = 'any sql contents'
        mock_open.return_value.__enter__.return_value = mock_file
        result = get_query('any filename')
        self.assertEqual(result, 'any sql contents')

        # Fail silently, return None
        mock_open.return_value.__enter__.side_effect = FileNotFoundError
        result = get_query('any filename')
        self.assertEqual(result, [])

    @patch("gobconfig.import_.import_config.DATASET_DIR", "mocked/data/dir/")
    def test_get_absolute_filepath(self):
        self.assertEqual('mocked/data/dir/the_file.json', get_absolute_filepath('the_file.json'))

    @patch("gobconfig.import_.import_config.get_mapping")
    @patch("gobconfig.import_.import_config.get_absolute_filepath", lambda x: 'absolute(' + x + ')')
    def test_get_import_definition_by_filename(self, mock_get_mapping):
        self.assertEqual(mock_get_mapping.return_value, get_import_definition_by_filename('thefilename.json'))
        mock_get_mapping.assert_called_with('absolute(thefilename.json)')

    @patch("gobconfig.import_.import_config.get_mapping")
    @patch("gobconfig.import_.import_config.get_dataset_file_location")
    def test_get_import_definition(self, mock_file_location, mock_get_mapping):
        self.assertEqual(mock_get_mapping.return_value, get_import_definition('cat', 'col', 'appl'))
        mock_get_mapping.assert_called_with(mock_file_location.return_value)
        mock_file_location.assert_called_with('cat', 'col', 'appl')
