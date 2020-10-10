## What does table_loader do?

table_loader helps you effortlessly maintain static dimension data warehouse tables by managing 3 data states:
+ The "preferred" state (or what's in the local directory)
+ The "last known applied" state (or what's in the bucket)
+ The "current" state (what is in the dataset)

```python
table_loader --bucket_prefix='gs://your-bucket-name'
```

## Directory Structure
```
projects
|-- some_dataset_name
|   |-- table_schema.json
|   |-- table_data.jsonl
|   |-- some_other_table.json
|   |-- some_other_data.jsonl
|-- some_other_dataset
|   |-- ...schema.json
|   |-- ...etc
```

## Run tests
```python
pytest --log-cli-level DEBUG
```

## Requirements

### Functional
 * If the user does something to the tables or other files, the client should be resilient
 * We need to support the following actions:
   * Create a table
   * Delete a table
   * Update a table in any way
 * We should allow to have unmanaged tables next to managed tables without influencing them


### User facing

 * The user needs to provide a way to save data across executions (last known applied state)
 * The user needs to provide a billing project to run against
