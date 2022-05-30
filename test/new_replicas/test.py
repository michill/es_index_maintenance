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

    # new_replicas = 0
    def test1_new_replicas(self):
        with open(f'{self.directory}/test1_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)
        run_test(json.dumps(test_config['input_config']))

        zero_replica_indices = [self.es_utils.es.indices.get_settings(index)[index]['settings']['index']['number_of_replicas'] == "0"
                                for index in test_config['create_new_indices']]

        self.assertTrue(all(zero_replica_indices))
        self.test_utils.cleanup(test_config)

    # new_replicas = 1
    def test2_new_replicas(self):
        with open(f'{self.directory}/test2_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)
        run_test(json.dumps(test_config['input_config']))

        one_replica_indices = [self.es_utils.es.indices.get_settings(index)[index]['settings']['index']['number_of_replicas'] == "1"
                                for index in test_config['create_new_indices']]

        self.assertTrue(all(one_replica_indices))
        self.test_utils.cleanup(test_config)

    # new_replicas = 2
    def test3_new_replicas(self):
        with open(f'{self.directory}/test3_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)
        run_test(json.dumps(test_config['input_config']))

        two_replica_indices = [self.es_utils.es.indices.get_settings(index)[index]['settings']['index']['number_of_replicas'] == "2"
                                for index in test_config['create_new_indices']]

        self.assertTrue(all(two_replica_indices))
        self.test_utils.cleanup(test_config)


if __name__ == "__main__":
    unittest.main()
