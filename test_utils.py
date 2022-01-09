from es_utils import EsUtils
from elasticsearch import Elasticsearch

"""
    Contains helper functions for use in tests
"""
class TestUtils(object):
    def __init__(self):
        self.es_utils = EsUtils()
        self.es = Elasticsearch()

    def setup(self, test_config):
        for index in test_config['create_old_indices']:
            self.es_utils.create_index(index)

        for (alias, index) in test_config['create_old_aliases'].items():
            self.es_utils.create_alias(index, alias)

        for index in test_config['create_new_indices']:
            self.es_utils.create_index(index)

    def cleanup(self, test_config):
        aliases_to_delete = f"*{test_config['input_config']['alias_name']}*"
        indices_to_delete = test_config['create_new_indices'] + test_config['create_old_indices']

        self.es_utils.delete_alias("*", aliases_to_delete)

        for index in indices_to_delete:
            self.es_utils.delete_index(index)

    def get_segment_counts(self, index):
        shards = self.es.indices.segments(index)['indices'][index]['shards'].keys()
        counts = {shard: len(self.es.indices.segments(index)['indices'][index]['shards'][shard][0]['segments'])
                  for shard in shards}
        return counts
