# Aliasing Design

input config (formatted)

```json
{
    "abort_run":"False",
    "alias_name":"customer-prod",
    "data_source":"customer",
    "delete_old":"True",
    "forcemerge_new":"True",
    "index_base":"customer-2_0_0",
    "new_replicas":"1"
}
```



Tests Remaining:

- main_utils (functions)
  - run_id_to_timestamp (validate order of timestamps)
  - validate_run_ids
- main_utils (scenarios)
  - if there is more than 1 runId for an index, is the latest one used
  - Handling of SMR index format
  - If the latest runId doesn't exist for all entities is an error returned
- exception for old = new









index	shards	size	segments	gb/shard	segments/shard	segments/gb	command ()



- command = (gb/shard) / (desired segments/gb)









- Abstract the urls from config?
  - Probably not necessary as already in airflow repo
- What if the runId ends on 000 ms
- Finish off remaining tests
- file to run all tests
- Modify loadElastic for all 4 data sources

- smr and smr-employee
- Closed sockets issue?
  - /home/michael/Desktop/aliasing/venv/lib/python3.8/site-packages/elasticsearch/transport.py:108: ResourceWarning: unclosed <socket.socket fd=6, family=AddressFamily.AF_INET, type=SocketKind.SOCK_STREAM, proto=6, laddr=('127.0.0.1', 59852), raddr=('127.0.0.1', 9200)>
      self.deserializer = Deserializer(_serializers, default_mimetype)
    ResourceWarning: Enable tracemalloc to get the object allocation traceback
- Readme.md
- Deploy

- failure at different points of operation?
- what if old indices = new indices and delted old is true
- whether to use 1 as the number of segments?



python -m unittest -vb





plan of attack:

- CR or Inc for prod?
  - Apply manual maintenance commands to all indices
  - reindex any with crazy shard counts
- CR for Wednesday release into ppte lz198 
- CR for Thursday release into prod lz198





Steps to implement:

- Complete tests
- Post question on slack
  - Update forcemerge functionality (update config.json to include max_segment_size_bytes parameter)
- Modify indices to include runId
  - Can smr be changed to smr-employee-blah rather than smr-blah-employee? (ask if any impacts to Alex)
- Deploy BVD
- Update airflow
  - smr and smr-employee added to smr
- Run in dev, ppte, prod



Slack Q:

- How to determine optimal max_num_segments value for forcemerge with each index?
  - transaction indices are still being written to. All others will be written to only once
  - 2GB 