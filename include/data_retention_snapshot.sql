CREATE SNAPSHOT {target_repository_name}."{schema}.{table}-{value}" TABLE {table_fqn} PARTITION ({column} = {value}) WITH ("wait_for_completion" = true);
