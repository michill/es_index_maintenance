import json, unittest, warnings, os, socket, time
from main import main as run_test
from es_utils import EsUtils
from test_utils import TestUtils
from urllib3.exceptions import ReadTimeoutError
from opensearchpy.exceptions import ConnectionTimeout

"""
    Tests whether the input parameter "forcemerge_new" is functioning as intended
"""
class Test(unittest.TestCase):
    def setUp(self):
        self.es_utils = EsUtils()
        self.test_utils = TestUtils()
        self.directory = os.path.dirname(os.path.abspath(__file__))
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    # forcemerge_new = True
    def test1_forcemerge_new(self):
        with open(f'{self.directory}/test1_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)

        # Adds data to each new index to increase the segment counts
        for index in test_config['create_new_indices']:
            self.test_utils.insert_bulk_data(index, 25000)

        run_test(json.dumps(test_config['input_config']))
        results = []
        time.sleep(10)

        # Appends 'True' to results list if all shards for an index have 1 segment
        for index in test_config['create_new_indices']:
            shard_segment_counts = self.test_utils.get_segment_counts(index).values()
            shard_segment_counts_equal_one = [int(count) == 1 for count in shard_segment_counts]
            print(f'index: {index}, '
                  f'shard_segment_counts: {shard_segment_counts}, '
                  f'shard_segment_counts_eq_one: {shard_segment_counts_equal_one}')
            results.append(all(shard_segment_counts_equal_one))

        self.assertTrue(all(results))
        self.test_utils.cleanup(test_config)

    # forcemerge_new = False
    def test2_forcemerge_new(self):
        with open(f'{self.directory}/test2_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.setup(test_config)

        # Adds data to each new index to increase the segment counts
        for index in test_config['create_new_indices']:
            self.test_utils.insert_bulk_data(index, 25000)

        run_test(json.dumps(test_config['input_config']))
        results = []

        # Appends 'True' to results list if any shards for an index have > 1 segment
        for index in test_config['create_new_indices']:
            shard_segment_counts = self.test_utils.get_segment_counts(index).values()
            shard_segment_counts_gt_one = [int(count) > 1 for count in shard_segment_counts]
            print(f'index: {index}, '
                  f'shard_segment_counts: {shard_segment_counts}, '
                  f'shard_segment_counts_gt_one: {shard_segment_counts_gt_one}')
            results.append(any(shard_segment_counts_gt_one))

        self.assertTrue(any(results))
        self.test_utils.cleanup(test_config)

    # Exceed timeout threshold (default of 10 seconds) when applying forcemerge to an index
    # with 2,500,000 documents with the exception handling logic in place
    def test3_forcemerge_new(self):
        self.es_utils.delete_index("test-index")
        self.es_utils.create_index("test-index")
        self.es_utils.set_index_replicas(index="test-index", replicas=0)
        self.test_utils.insert_bulk_data("test-index", 2500000)
        print(self.test_utils.get_segment_counts("test-index"))
        self.es_utils.forcemerge_index(index="test-index", max_num_segments=1)
        print(self.test_utils.get_segment_counts("test-index"))
        self.es_utils.set_index_replicas(index="test-index", replicas=1)
        print(self.test_utils.get_segment_counts("test-index"))
        print(self.test_utils.get_segment_counts("test-index"))
        assert(True)
        # self.es_utils.delete_index("test-index")

    # Exceed timeout threshold (default of 10 seconds) when applying forcemerge to an index
    # with 2,500,000 documents without exception handling logic in place
    def test4_forcemerge_new(self):
        self.es_utils.create_index("test-index")
        self.test_utils.insert_bulk_data("test-index", 2500000)

        with self.assertRaises((socket.timeout, ReadTimeoutError, ConnectionTimeout)):
            self.es_utils.es.indices.forcemerge(index="test-index", max_num_segments=1)

        self.es_utils.delete_index("test-index")


if __name__ == "__main__":
    unittest.main()
