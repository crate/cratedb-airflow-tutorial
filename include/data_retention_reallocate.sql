ALTER TABLE {table_fqn} PARTITION ({column} = {value}) SET ("routing.allocation.require.{attribute_name}" = '{attribute_value}');
