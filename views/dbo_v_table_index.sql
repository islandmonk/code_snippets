CREATE OR ALTER VIEW dbo.v_table_index 
AS
	SELECT 
		  x.schema_name + '.' + x.table_name as two_part_table_name
		, x.object_id
		, x.index_name
		, x.schema_name + '.' 
		+ x.table_name + ' {' 
		+ LEFT(x.key_columns, LEN(x.key_columns) - 1) + '}'
		+ CASE 
			WHEN x.include_columns IS NULL 
			THEN '' 
			ELSE ' include {' + LEFT(x.include_columns, LEN(x.include_columns) - 1) + '}' END as index_standardized_name
		, x.is_primary_key
		, x.is_clustered
		, x.is_unique
		, x.is_unique_constraint
		, LEFT(x.key_columns, LEN(x.key_columns) - 1) as key_columns
		, LEFT(x.include_columns, LEN(x.include_columns) - 1) as include_columns
		, x.row_count
		, x.index_depth
		, x.index_level
		, CAST(x.avg_fragmentation_in_percent as decimal(18,3)) as avg_fragmentation_in_percent
		, x.fragment_count
		, CAST(x.avg_fragment_size_in_pages as decimal(18,3)) as avg_fragment_size_in_pages
		, x.page_count
		, x.user_seeks
		, x.last_user_seek
		, x.user_scans
		, x.last_user_scan
		, x.user_lookups
		, x.last_user_lookup
		, x.user_updates
		, x.last_user_update
		, x.system_seeks
		, x.last_system_seek
		, x.system_scans
		, x.last_system_scan
		, x.system_lookups
		, x.last_system_lookup
		, x.system_updates
		, x.last_system_update
		, x.avg_page_space_used_in_percent
		, x.record_count
		, x.index_id
		, x.schema_name
		, x.schema_id
		, x.table_name
	FROM (
		SELECT 
			  s.name as schema_name
			, s.schema_id
			, t.name as table_name
			, t.object_id 
			, ISNULL(i.name, i.type_desc) as index_name
			, i.index_id
			, i.is_primary_key
			, i.is_unique_constraint
			, i.is_unique
			, CASE i.type_desc WHEN 'CLUSTERED' THEN 1 ELSE 0 END as is_clustered
			, (
				SELECT c.name + CASE ic.is_descending_key WHEN 1 THEN ' DESC' ELSE '' END + ','
				FROM sys.index_columns as ic
				INNER JOIN sys.columns as c
					ON ic.object_id = c.object_id
					AND ic.column_id = c.column_id
				WHERE t.object_id = ic.object_id
				AND i.index_id = ic.index_id
				AND ic.is_included_column = 0
				ORDER BY ic.key_ordinal
				FOR XML PATH('')
			) as key_columns
			, (
				SELECT c.name + ','
				FROM sys.index_columns as ic
				INNER JOIN sys.columns as c
					ON ic.object_id = c.object_id
					AND ic.column_id = c.column_id
				WHERE t.object_id = ic.object_id
				AND i.index_id = ic.index_id
				AND ic.is_included_column = 1
				ORDER BY c.name
				FOR XML PATH('')
			) as include_columns
			, ps.row_count
			, ips.index_depth
			, ips.index_level
			, CAST(ips.avg_fragmentation_in_percent as decimal(18,3)) as avg_fragmentation_in_percent
			, ips.fragment_count
			, CAST(ips.avg_fragment_size_in_pages as decimal(18,3)) as avg_fragment_size_in_pages
			, ips.page_count
			, ius.user_seeks
			, ius.last_user_seek
			, ius.user_scans
			, ius.last_user_scan
			, ius.user_lookups
			, ius.last_user_lookup
			, ius.user_updates
			, ius.last_user_update
			, ius.system_seeks
			, ius.last_system_seek
			, ius.system_scans
			, ius.last_system_scan
			, ius.system_lookups
			, ius.last_system_lookup
			, ius.system_updates
			, ius.last_system_update
			, ips.avg_page_space_used_in_percent
			, ips.record_count
		FROM sys.schemas as s
		INNER JOIN sys.tables as t
			ON s.schema_id = t.schema_id
		INNER JOIN sys.indexes as i
			ON t.object_id = i.object_id
		INNER JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) as ips
			ON i.[object_id] = ips.[object_id]
			AND i.[index_id] = ips.[index_id]
			AND ips.alloc_unit_type_desc = 'IN_ROW_DATA'
		INNER JOIN sys.dm_db_partition_stats as ps
			ON i.index_id = ps.index_id
			AND i.[object_id] = ps.[object_id]
		LEFT OUTER JOIN sys.dm_db_index_usage_stats as ius
			ON i.[object_id] = ius.[object_id]
			AND i.index_id = ius.index_id
	) as x

-- SELECT * FROM dbo.v_table_index ORDER BY index_standardized_name

