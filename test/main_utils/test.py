import json, unittest, warnings, os
from main import main as run_test
from es_utils import EsUtils
from test_utils import TestUtils
from elasticsearch import Elasticsearch
from main_utils import MainUtils

"""
    Tests whether the input parameter "delete_old" is functioning as intended
"""
class Test(unittest.TestCase):
    def setUp(self):
        self.es_utils = EsUtils()
        self.test_utils = TestUtils()
        self.main_utils = MainUtils()
        self.directory = os.path.dirname(os.path.abspath(__file__))
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    # get_latest_index
    def test1_main_utils(self):
        with open(f'{self.directory}/test1_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)
        latest_index = self.main_utils.get_latest_index('customer-1_0_0', 'account')
        self.assertTrue(latest_index == "resolver-customer-1_0_0-2021-08-11_10-25-57-089-account")
        self.test_utils.cleanup(test_config)

    # get_latest_index (exception case)
    def test2_main_utils(self):
        with self.assertRaisesRegex(Exception, "No indices returned from query"):
            self.main_utils.get_latest_index('non_existent_index', 'account')

    # get_latest_run_id_index (exception case)
    def test3_main_utils(self):
        indices = ["search-customer-1_0_0-2019-12-10_10-25-57-1",
                   "search-customer-1_0_0-2022-01-10_10-25-57-400",
                   "search-customer-1_0_0-2021-09-10_10-25-57-12",
                   "search-customer-1_0_0-2022-01-07_10-25-57-14"]

        latest_index = self.main_utils.get_latest_run_id_index(indices)
        self.assertTrue(latest_index == "search-customer-1_0_0-2022-01-10_10-25-57-400")

    # get_run_id
    def test4_main_utils(self):
        index = "resolver-customer-1_0_0-2018-10-06_10-22-57-12-business"
        run_id = self.main_utils.get_run_id(index)
        self.assertTrue(run_id == "2018-10-06_10-22-57-12")

    # get_run_id (exception case)
    def test5_main_utils(self):
        index = "resolver-customer-1_0_0-new-index-business"

        with self.assertRaisesRegex(Exception, "No runId found for index"):
            self.main_utils.get_run_id(index)

    # run_id_to_timestamp
    def test6_main_utils(self):
        run_ids = ["2018-10-07_10-22-57-100", "2018-10-06_10-22-57-100"]
        timestamps = [self.main_utils.run_id_to_timestamp(run_id) for run_id in run_ids]
        print(timestamps)
        self.assertTrue(timestamps[0] > timestamps[1])

    # run_id_to_timestamp
    def test7_main_utils(self):
        run_ids = ["2020-10-06_10-12-57-7", "2020-10-06_10-22-57-74"]
        timestamps = [self.main_utils.run_id_to_timestamp(run_id) for run_id in run_ids]
        print(timestamps)
        self.assertTrue(timestamps[0] < timestamps[1])

    # validate_alias_mappings (exception case)
    def test8_main_utils(self):
        with open(f'{self.directory}/test8_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)

        with self.assertRaisesRegex(Exception, "Latest indices already associated with alias"):
            run_test(json.dumps(test_config['input_config']))

        self.test_utils.cleanup(test_config)

    # validate_run_ids (exception case)
    def test9_main_utils(self):
        with open(f'{self.directory}/test9_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)

        with self.assertRaisesRegex(Exception, "A single runId does not exist for the input indices"):
            run_test(json.dumps(test_config['input_config']))

        self.test_utils.cleanup(test_config)

    # latest index used when more than 1 runId exists for indices
    def test10_main_utils(self):
        with open(f'{self.directory}/test10_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.cleanup(test_config)
        self.test_utils.setup(test_config)

        old_indices = test_config['create_old_indices']
        new_alias_indices = test_config['new_alias_indices']
        run_test(json.dumps(test_config['input_config']))

        old_indices_deleted = [len(self.es_utils.get_indices(index)) == 0 for index in old_indices]
        new_indices_aliased = [self.es_utils.get_alias_indices(alias)[0] == new_alias_indices[alias] for
                               alias in new_alias_indices.keys()]

        self.assertTrue(all(old_indices_deleted) and all(new_indices_aliased))
        self.test_utils.cleanup(test_config)


if __name__ == "__main__":
    unittest.main()
