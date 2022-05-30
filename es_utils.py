import json, os, socket
from opensearchpy import OpenSearch
from urllib3.exceptions import ReadTimeoutError
from opensearchpy.exceptions import ConnectionTimeout, TransportError

"""
    Contains helper functions used to interface with an Opensearch cluster
"""
class EsUtils(object):
    def __init__(self, env='dev'):
        self.env = env
        directory = os.path.dirname(os.path.abspath(__file__))

        with open(f'{directory}/config.json') as config_file:
            file_config = json.load(config_file)

        ca_certs = file_config['ca_certs']

        hosts_parameter = f'es_urls_{self.env}'
        hosts = file_config[hosts_parameter]

        if ca_certs:
            self.es = OpenSearch(hosts=hosts, timeout=1, ca_certs=ca_certs)
        else:
            self.es = OpenSearch(hosts=hosts, timeout=1)

    # Creates an index with the specified name, ignoring errors if it already exists
    def create_index(self, index):
        print(f"Creating index: {index}")
        self.es.indices.create(index=index,
                               ignore=[400, 404])

    # Deletes indices matching the specified input. Ignores errors when the input
    # index does not exist and does not delete based on wildcard notation
    def delete_index(self, index):
        print(f"Deleting index: {index}")
        self.es.indices.delete(index=index,
                               ignore=[400, 404],
                               expand_wildcards='none')

    # Returns a list of indices matching the input wildcard index name
    def get_indices(self, index_query):
        if self.es.indices.exists(index_query):
            return list(self.es.indices.get(index_query))
        else:
            return []

    # Creates an alias for the input index, ignoring errors if it already exists
    def create_alias(self, index, alias):
        print(f"Creating alias: {alias} for index: {index}")
        self.es.indices.put_alias(index=index,
                                  name=alias,
                                  ignore=[400, 404])

    # Deletes an alias for the input index. Ignores errors when the input
    # index / alias does not exist
    def delete_alias(self, index, alias):
        print(f"Deleting alias: {alias} for index: {index}")
        self.es.indices.delete_alias(index=index,
                                     name=alias,
                                     ignore=[400, 404])

    # Returns a list of indices associated with the input alias
    def get_alias_indices(self, alias):
        if self.es.indices.exists_alias(alias):
            return list(self.es.indices.get(alias).keys())
        else:
            return []

    # Applies the "forcemerge" operation to the input index. Ignores exceptions related to
    # hitting a 10 second timeout from this synchronous operation
    def forcemerge_index(self, index, max_num_segments):
        print(f"Applying forcemerge to index: {index}")
        try:
            self.es.indices.forcemerge(index=index, max_num_segments=max_num_segments)
        except (socket.timeout, ReadTimeoutError, ConnectionTimeout, TransportError) as e:
            print(f"Exception: {e} hit for index: {index}")
            pass

    # Modifies the number of replicas for the input index
    def set_index_replicas(self, index, replicas):
        print(f"Setting replicas to {replicas} for index: {index}")
        self.es.indices.put_settings(index=index,
                                     body={"number_of_replicas": replicas})
