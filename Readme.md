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



Tests:

- abort_run will exit if True
- delete_old set to True vs False
- forcemerge_new set to True vs False
- new_replicas set to 0, 1, 2
- input config validation
  - invalid input (missing config or too many fields)
  - data_source not in list of data_sources from config
- If there is more than 1 runId for an index, is the latest one used
- If the latest runId doesn't exist for all entities is an error returned



test1: 

- test_config:
  - create_indices ([resolver-customer-2_0_0-2021-01-10_10-25-57-089, ... , search-customer-2_0_0-2021-01-10_10-25-57-089)
  - create_aliases (resolver-customer-prod -> resolver-customer-2_0_0-2021-01-10_10-25-57-089)
  - input_config

- runner



```json
{
    "create_indices":[
        "resolver-customer-test-2021-01-10_10-25-57-089-account",
        "resolver-customer-test-2021-01-10_10-25-57-089-address",
        "resolver-customer-test-2021-01-10_10-25-57-089-business",
        "resolver-customer-test-2021-01-10_10-25-57-089-doc2rec",
        "resolver-customer-test-2021-01-10_10-25-57-089-email",
        "resolver-customer-test-2021-01-10_10-25-57-089-individual",
        "resolver-customer-test-2021-01-10_10-25-57-089-telephone",
        "search-customer-test-2021-01-10_10-25-57-089"
    ],
    "create_aliases":[
        "resolver-customer-test-account":"resolver-customer-test-2021-01-10_10-25-57-089-account",
        "resolver-customer-test-address":"resolver-customer-test-2021-01-10_10-25-57-089-address",
        "resolver-customer-test-business":"resolver-customer-test-2021-01-10_10-25-57-089-business",
        "resolver-customer-test-doc2rec":"resolver-customer-test-2021-01-10_10-25-57-089-doc2rec",
        "resolver-customer-test-email":"resolver-customer-test-2021-01-10_10-25-57-089-email",
        "resolver-customer-test-individual":"resolver-customer-test-2021-01-10_10-25-57-089-individual",
        "resolver-customer-test-telephone":"resolver-customer-test-2021-01-10_10-25-57-089-telephone",
        "search-customer-test":"search-customer-test-2021-01-10_10-25-57-089"
    ],
    "input_config":[
        "abort_run":"False",
        "alias_name":"customer-prod",
        "data_source":"customer",
        "delete_old":"True",
        "forcemerge_new":"True",
        "index_base":"customer-2_0_0",
        "new_replicas":"1"
    ]
}
```



input config (compact)

```json
{"data_source":"customer","alias_name":"customer-prod","index_base":"customer-2_0_0","delete_old":"True","forcemerge_new":"True","new_replicas":"1","abort_run":"False"}
```





for entity in entities:

- Get current index associated with alias
  - alias: customer-prod-account
  - index: customer-2_0_0-2021_12_21_12_38_59-account
- Add new index to alias
  - alias: customer-prod-account
  - index: customer-2_0_0-2021_12_22_11_24_18-account
- Remove old index from alias
- delete old index
- forcemerge new index
- replicate new index







```json
{
    "es_url_dev":"https://abc.com",
    "es_url_ppte":"https://def.com",
    "es_url_prod":"https://ghi.com",
    "bvd_entities":[
        "address",
        "business",
        "email",
        "individual",
        "telephone"
    ],
    "customer_entities":[
        "account",
        "address",
        "business",
        "email",
        "individual",
        "telephone"
    ],
    "smr_entities":[
        "account",
        "address",
        "business",
        "email",
        "individual",
        "telephone"
    ],
    "worldcheck_entities":[
        "address",
        "business",
        "individual"
    ]
}
```









