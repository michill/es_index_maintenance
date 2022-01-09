import re, time, datetime
from es_utils import EsUtils

"""
    Contains helper functions used to run the index maintenance commands
"""
class MainUtils(object):
    def __init__(self):
        self.es_utils = EsUtils()

    # Returns the index with the latest runId value of all indices with a specified base. Raises
    # an exception if no indices are returned from the constructed query
    def get_latest_index(self, index_base, entity):
        if entity == 'search':
            index_query = f'search-{index_base}-*'
        else:
            index_query = f'resolver-{index_base}-*-{entity}'

        indices = self.es_utils.get_indices(index_query)

        if indices:
            return self.get_latest_run_id_index(indices)
        else:
            print(f"No indices returned for index_query: {index_query}")
            raise Exception("No indices returned from query")

    # Returns the index with the latest run_id from a list of indices
    def get_latest_run_id_index(self, indices):
        index_run_ids = {index: self.get_run_id(index) for index in indices}
        index_timestamps = {index: self.run_id_to_timestamp(run_id) for (index, run_id) in index_run_ids.items()}
        return max(index_timestamps, key=index_timestamps.get)

    # Returns the runId portion of the name from the input index. Raises an exception if the
    # input index has no matches for the runId pattern
    @staticmethod
    def get_run_id(index):
        run_id_pattern = '(\d){4}-(\d){2}-(\d){2}_(\d){2}-(\d){2}-(\d){2}-(\d){0,3}'
        match = re.search(run_id_pattern, index)

        if match:
            return match.group(0)
        else:
            print(f"No runId found for index: {index}")
            raise Exception("No runId found for index")

    # Converts from a runId format to a Timestamp
    @staticmethod
    def run_id_to_timestamp(run_id):
        run_id_format = "%Y-%m-%d_%H-%M-%S-%f"
        return time.mktime(datetime.datetime.strptime(run_id, run_id_format).timetuple())

    # Performs validation on the set of indices determined as being the latest based on their
    # runId. Throws an exception if there are different runId values among these indices
    def validate_run_ids(self, indices):
        if len(set(self.get_run_id(index) for index in indices)) != 1:
            print(f"A single runId does not exist for the following indices: {indices}")
            raise Exception(f"A single runId does not exist for the input indices")

    # Performs validation checks on input config. Raises an exception if any of the required
    # input fields are not present or have values considered invalid
    @staticmethod
    def validate_input(input_config, file_config):
        if sorted(input_config.keys()) != sorted(file_config['input_config_fields']):
            print(f"Input fields: {sorted(input_config.keys())}. "
                  f"Required fields: {sorted(file_config['input_config_fields'])}")
            raise Exception("Invalid set of input fields provided")

        if input_config['data_source'] not in file_config['data_sources']:
            print(f"Input data_source value: '{input_config['data_source']}'. "
                  f"Accepted values: {file_config['data_sources']}")
            raise Exception("Invalid data_source value")

        if input_config['abort_run'] not in ['True', 'False']:
            print(f"Input abort_run value: '{input_config['abort_run']}'. "
                  f"Accepted values: [\'True\', \'False\']")
            raise Exception("Invalid abort_run value")

        if input_config['delete_old'] not in ['True', 'False']:
            print(f"Input delete_old value: '{input_config['delete_old']}'. "
                  f"Accepted values: [\'True\', \'False\']")
            raise Exception("Invalid delete_old value")

        if input_config['forcemerge_new'] not in ['True', 'False']:
            print(f"Input forcemerge_new value: '{input_config['forcemerge_new']}'. "
                  f"Accepted values: [\'True\', \'False\']")
            raise Exception("Invalid forcemerge_new value")

        if not re.match("^\d+$", input_config['new_replicas']):
            print(f"Input new_replicas value: '{input_config['new_replicas']}'. "
                  f"Accepted values: {{'0', '1', '2', ...}}")
            raise Exception("Invalid new_replicas value")
