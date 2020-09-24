## table_loader

table_loader helps you maintain static dimension data warehouse tables.

## Schema example
```json
[
  {
    "description": "ISO Code",
    "mode": "REQUIRED",
    "name": "COUNTRY_ISO_CODE",
    "type": "STRING"
  },
  {
    "description": "Country Name",
    "mode": "REQUIRED",
    "name": "COUNTRY_NAME",
    "type": "STRING"
  }
]
```

## Data Example
```json
{"COUNTRY_ISO":"GR", "COUNTRY_NAME": "Greece"}
{"COUNTRY_ISO":"AF", "COUNTRY_NAME": "Afganistan"}
{"COUNTRY_ISO":"ES", "COUNTRY_NAME": "Spain"}
```

### Dev Links
+ [Schema Components](https://cloud.google.com/bigquery/docs/schemas#schema_components)
+ [Loading json](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json)
+ [Poetry](https://python-poetry.org/docs/)


