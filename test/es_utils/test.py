import unittest, warnings
import time
from es_utils import EsUtils
from test_utils import TestUtils

"""
    Tests the functions defined within es_utils
"""
class Test(unittest.TestCase):
    def setUp(self):
        self.es_utils = EsUtils()
        self.test_utils = TestUtils()
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    # create_index
    def test1_es_utils(self):
        self.es_utils.create_index("test")
        self.assertTrue(len(self.es_utils.get_indices("test")) == 1)

    # create_alias
    def test2_es_utils(self):
        self.es_utils.create_alias('test', 'test-alias')
        self.assertTrue(self.es_utils.es.indices.exists_alias('test-alias'))

    # delete_alias
    def test3_es_utils(self):
        self.es_utils.delete_alias("test", "test-alias")
        self.assertFalse(self.es_utils.es.indices.exists_alias('test-alias'))

    # forcemerge_index (ensure segment count > 1 prior to running forcemerge)
    def test4_es_utils(self):
        self.test_utils.insert_bulk_data("test", 100000)
        shard_segment_counts = self.test_utils.get_segment_counts("test").values()
        shard_segment_counts_gt_one = [int(count) > 1 for count in shard_segment_counts]
        print(f"shard_segment_counts: {shard_segment_counts}, "
              f"shard_segment_counts_gt_one: {shard_segment_counts_gt_one}")
        self.assertTrue(any(shard_segment_counts_gt_one))

    # forcemerge_index
    def test5_es_utils(self):
        self.es_utils.forcemerge_index("test", 1)
        time.sleep(5)
        shard_segment_counts = self.test_utils.get_segment_counts("test").values()
        shard_segment_counts_equal_one = [int(count) == 1 for count in shard_segment_counts]
        print(f"shard_segment_counts: {shard_segment_counts}, "
              f"shard_segment_counts_equal_one: {shard_segment_counts_equal_one}")
        self.assertTrue(all(shard_segment_counts_equal_one))

    # set_index_replicas
    def test6_es_utils(self):
        self.es_utils.set_index_replicas('test', 2)
        replicas = self.es_utils.es.indices.get_settings('test')['test']['settings']['index']['number_of_replicas']
        self.assertTrue(replicas == "2")

    # set_index_replicas
    def test7_es_utils(self):
        self.es_utils.set_index_replicas('test', 3)
        replicas = self.es_utils.es.indices.get_settings('test')['test']['settings']['index']['number_of_replicas']
        self.assertTrue(replicas == "3")

    # delete_index
    def test8_es_utils(self):
        self.es_utils.delete_index("test")
        self.assertTrue(len(self.es_utils.get_indices("test")) == 0)


if __name__ == "__main__":
    unittest.main()
