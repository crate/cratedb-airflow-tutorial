SELECT QUOTE_IDENT(p.table_schema),
       QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(r.partition_column),
       TRY_CAST(p.values[r.partition_column] AS BIGINT),
       r.strategy,
       reallocation_attribute_name,
       reallocation_attribute_value
FROM information_schema.table_partitions p
JOIN doc.retention_policies r ON p.table_schema = r.table_schema
  AND p.table_name = r.table_name
  AND p.values[r.partition_column] < '{date}'::TIMESTAMP - r.retention_period
LEFT JOIN doc.retention_policy_tracking t ON t.table_schema = p.table_schema
  AND t.table_name = p.table_name
WHERE r.strategy = 'delete'
  OR (
    r.strategy = 'reallocate'
      AND (
        t.last_partition_value IS NULL
        OR p.values[r.partition_column] > t.last_partition_value
      )
  )
ORDER BY 5 ASC;
