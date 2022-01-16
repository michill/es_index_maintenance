import os, sys, json
from es_utils import EsUtils
from main_utils import MainUtils

def main(args):
    directory = os.path.dirname(os.path.abspath(__file__))

    with open(f'{directory}/config.json') as config_file:
        file_config = json.load(config_file)

    input_config = json.loads(args)
    MainUtils.validate_input(input_config, file_config)

    es_utils = EsUtils(input_config['env'])
    main_utils = MainUtils(input_config['env'])

    if eval(input_config['abort_run']):
        print("Aborting run as abort_run parameter was set to True")
        raise Exception("Run aborted")

    # Obtains current and to-be mappings from alias -> index
    entities = file_config[f"{input_config['data_source']}_entities"]
    search_index_alias = {"search": f"search-{input_config['alias_name']}"}
    resolver_index_aliases = {entity: f"resolver-{input_config['alias_name']}-{entity}" for entity in entities}
    all_index_aliases = {**search_index_alias, **resolver_index_aliases}
    old_alias_mappings = {alias: es_utils.get_alias_indices(alias) for alias in all_index_aliases.values()}
    new_alias_mappings = {alias: main_utils.get_latest_index(input_config['index_base'], entity)
                          for (entity, alias) in all_index_aliases.items()}

    main_utils.validate_alias_mappings(old_alias_mappings, new_alias_mappings)
    main_utils.validate_run_ids(new_alias_mappings.values())

    # Adds new indices to aliases
    for (alias, index) in new_alias_mappings.items():
        es_utils.create_alias(index, alias)

    # Removes old indices from aliases
    for (alias, indices) in old_alias_mappings.items():
        for index in indices:
            es_utils.delete_alias(index, alias)

    # Deletes old indices
    if eval(input_config['delete_old']):
        for indices in old_alias_mappings.values():
            for index in indices:
                es_utils.delete_index(index)

    # Performs a "forcemerge" on newly loaded indices
    if eval(input_config['forcemerge_new']):
        for index in new_alias_mappings.values():
            es_utils.forcemerge_index(index, 1)

    # Sets the replicas for the newly loaded indices
    for index in new_alias_mappings.values():
        es_utils.set_index_replicas(index, input_config['new_replicas'])


if __name__ == "__main__":
    main(sys.argv[1])
