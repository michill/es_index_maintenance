# Aliasing Design

Static config (config.json)

```json
{
    "ca_certs":"",
    "es_urls_dev":["http://localhost:9200"],
    "es_urls_ppte":["http://localhost:9200"],
    "es_urls_prod":["http://invalid:9200"],
    "data_sources":[
        "bvd",
        "customer",
        "smr",
        "smr-employee",
        "worldcheck"
    ],
    "input_config_fields":[
        "abort_run",
        "alias_name",
        "data_source",
        "delete_old",
        "env",
        "forcemerge_new",
        "index_base",
        "new_replicas"
    ],
    "bvd_entities":[
        "address",
        "business",
	    "doc2rec",
	    "email",
        "individual",
        "telephone"
    ],
    "customer_entities":[
        "account",
        "address",
        "business",
	    "doc2rec",
        "email",
        "individual",
        "telephone"
    ],
    "smr_entities":[
        "account",
        "address",
        "business",
        "doc2rec",
        "email",
        "individual",
        "telephone"
    ],
    "smr-employee_entities":[
        "account",
        "address",
        "business",
        "doc2rec",
        "email",
        "individual",
        "telephone"
    ],
    "worldcheck_entities":[
        "address",
        "business",
        "doc2rec",
        "individual"
    ]
}
```



Dynamic config example:

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



Accepted config values:

```json
"abort_run": ["True", "False"]
"alias_name": "*"
"data_source": ["bvd", "customer", "smr", "smr-employee", "worldcheck"] (data_sources from config.json)
"delete_old": ["True", "False"]
"forcemerge_new": ["True", "False"]
"index_base": "*"
"new_replicas": ["0", "1", "2", ...]
```



Run script example:

```shell
python3 main.py '{"data_source":"customer","alias_name":"customer-prod","index_base":"customer-2_0_0","delete_old":"True","forcemerge_new":"True","new_replicas":"1","abort_run":"False"}'
```



Run Tests:

```python
venv/bin/python -m unittest -vb
```

