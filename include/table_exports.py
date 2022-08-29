"Configuration of tables to export in cratedb_table_export DAG"
TABLES = [
    {
        "table": "telegraf.metrics",
        "timestamp_column": "timestamp",
        "target_bucket": "crate-astro-tutorial/metrics",
    }
]
