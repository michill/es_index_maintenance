from es_utils import EsUtils
from elasticsearch import helpers
import string, random

"""
    Contains helper functions for use in tests
"""
class TestUtils(object):
    def __init__(self):
        self.es_utils = EsUtils()

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
        shards = self.es_utils.es.indices.segments(index)['indices'][index]['shards'].keys()
        counts = {shard: len(self.es_utils.es.indices.segments(index)['indices'][index]['shards'][shard][0]['segments'])
                  for shard in shards}
        return counts

    def insert_bulk_data(self, index, num_docs):
        documents = [
            {
                "_index": index,
                "_type": "doc",
                "_id": i,
                "_source": {
                    "text": ''.join(random.choice(string.ascii_lowercase) for _ in range(100))
                }
            }
            for i in range(num_docs)
        ]
        helpers.bulk(self.es_utils.es, documents)
