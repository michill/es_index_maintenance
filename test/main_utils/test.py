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
        self.es = Elasticsearch()
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
        run_ids = ["2018-10-06_10-22-57-120000", "2018-10-06_10-22-57-100000"]
        timestamps = [self.main_utils.run_id_to_timestamp(run_id) for run_id in run_ids]
        print(timestamps)
        self.assertTrue(timestamps[0] > timestamps[1])

    # run_id_to_timestamp
    def test7_main_utils(self):
        index = "resolver-customer-1_0_0-2018-10-06_10-22-57-12-business"
        run_id = self.main_utils.get_run_id(index)
        self.assertTrue(run_id == "2018-10-06_10-22-57-12")


if __name__ == "__main__":
    unittest.main()
