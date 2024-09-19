;with t as (
	SELECT
		t.TABLE_NAME, OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) as objid, TABLE_TYPE
	FROM INFORMATION_SCHEMA.TABLES as t
	WHERE t.TABLE_TYPE = 'BASE TABLE'
)
, r as (
	SELECT 
		t.TABLE_NAME
		, CASE
			WHEN EXISTS (
				SELECT TOP 1 1
				FROM sys.indexes as i
				WHERE t.objid = i.object_id
			)
			THEN 1
			ELSE 0
		  END as has_index
		, CASE
			WHEN EXISTS (
				SELECT TOP 1 1
				FROM sys.indexes as i
				WHERE t.objid = i.object_id
				AND i.is_unique = 1
			)
			THEN 1
			ELSE 0
		  END as has_unique_index
		, CASE
			WHEN EXISTS (
				SELECT TOP 1 1
				FROM sys.indexes as i
				WHERE t.objid = i.object_id
				AND i.is_primary_key = 1
			)
			THEN 1
			ELSE 0
		  END as has_PK
	FROM t
)
SELECT
	  LEFT(r.TABLE_NAME, 50) as TABLE_NAME
	, has_index
	, has_unique_index
	, has_PK
FROM r
where has_index = 0
OR has_unique_index = 0
or has_PK = 0


ALTER TABLE [dbo].ADAPTIVAPACKAGES add [hello] varchar(50)