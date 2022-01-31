CREATE SNAPSHOT {repository_name}."{table_schema}.{table_name}-{partition_value}" TABLE {table_fqn} PARTITION ({partition_column} = {partition_value}) WITH ("wait_for_completion" = true);
