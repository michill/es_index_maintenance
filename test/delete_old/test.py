import json, unittest, warnings, os
from main import main as run_test
from es_utils import EsUtils
from test_utils import TestUtils

"""
    Tests whether the input parameter "delete_old" is functioning as intended
"""
class Test(unittest.TestCase):
    def setUp(self):
        self.es_utils = EsUtils()
        self.test_utils = TestUtils()
        self.directory = os.path.dirname(os.path.abspath(__file__))
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    # delete_old = True
    def test1_delete_old(self):
        with open(f'{self.directory}/test1_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)
        run_test(json.dumps(test_config['input_config']))
        index_query_results = [len(self.es_utils.get_indices(index)) == 0 for index in test_config['create_old_indices']]
        self.assertTrue(all(index_query_results))
        self.test_utils.cleanup(test_config)

    # delete_old = False
    def test2_delete_old(self):
        with open(f'{self.directory}/test2_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)
        run_test(json.dumps(test_config['input_config']))
        index_query_results = [len(self.es_utils.get_indices(index)) == 1 for index in test_config['create_old_indices']]
        self.assertTrue(all(index_query_results))
        self.test_utils.cleanup(test_config)


if __name__ == "__main__":
    unittest.main()
