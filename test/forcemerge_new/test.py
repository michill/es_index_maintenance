import json, unittest, warnings, os
from main import main as run_test
from es_utils import EsUtils
from test_utils import TestUtils
from elasticsearch import Elasticsearch

"""
    Tests whether the input parameter "forcemerge_new" is functioning as intended
"""
class Test(unittest.TestCase):
    def setUp(self):
        self.es = Elasticsearch()
        self.es_utils = EsUtils()
        self.test_utils = TestUtils()
        self.directory = os.path.dirname(os.path.abspath(__file__))
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    # forcemerge_new = True
    def test1_forcemerge_new(self):
        with open(f'{self.directory}/test1_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.cleanup(test_config)
        self.test_utils.setup(test_config)

        # Adds data to each new index to increase the segment counts
        for index in test_config['create_new_indices']:
            bulk_request = ""
            for i in range(500):
                bulk_request += f'{{ "index" : {{ "_index" : "{index}" }} }}\n'
                bulk_request += f'{{ "{i}" : "{i}" }}\n'

            self.es.bulk(index=index, doc_type='test', body=bulk_request)

        run_test(json.dumps(test_config['input_config']))
        results = []

        # Appends 'True' to results list if all shards for an index have 1 segment
        for index in test_config['create_new_indices']:
            shard_segment_counts = self.test_utils.get_segment_counts(index).values()
            shard_segment_counts_equal_one = [int(count) == 1 for count in shard_segment_counts]
            print(f'index: {index}, '
                  f'shard_segment_counts: {shard_segment_counts}, '
                  f'shard_segment_counts_gt_one: {shard_segment_counts_equal_one}')
            results.append(all(shard_segment_counts_equal_one))

        self.assertTrue(all(results))
        self.test_utils.cleanup(test_config)

    # forcemerge_new = False
    def test2_forcemerge_new(self):
        with open(f'{self.directory}/test2_config.json') as config_file:
            test_config = json.load(config_file)

        self.test_utils.cleanup(test_config)
        self.test_utils.setup(test_config)

        # Adds data to each new index to increase the segment counts
        for index in test_config['create_new_indices']:
            bulk_request = ""
            for i in range(500):
                bulk_request += f'{{ "index" : {{ "_index" : "{index}" }} }}\n'
                bulk_request += f'{{ "{i}" : "{i}" }}\n'

            self.es.bulk(index=index, doc_type='test', body=bulk_request)

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

        self.assertTrue(all(results))
        self.test_utils.cleanup(test_config)


if __name__ == "__main__":
    unittest.main()
