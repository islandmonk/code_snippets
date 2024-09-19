-- to turn on xp_cmdshell:
/*
EXEC sp_configure 'show advanced options', 1
go

RECONFIGURE
GO

EXEC sp_configure 'xp_cmdshell', 1
GO

RECONFIGURE
GO
*/
SET NOCOUNT ON
SET ROWCOUNT 0
GO
/*
	DROP TABLE [dbo].[extended_events_file]
	DROP TABLE [dbo].[extended_event]
	DROP TABLE [dbo].[extended_event_values]
*/

IF OBJECT_ID('dbo.extended_events_file') IS NULL
BEGIN
	CREATE TABLE [dbo].[extended_events_file] (
		  [extended_events_file_id] int IDENTITY(1,1) PRIMARY KEY
		, [extended_events_file_name] varchar(1000) NOT NULL
		, [file_path_and_name] varchar(1000) NOT NULL
	)
END



IF OBJECT_ID('dbo.extended_event') IS NULL
BEGIN
	CREATE TABLE [dbo].[extended_event] (    -- TODO: rename to [event] 
		  [event_id]						int identity(1,1) primary key    -- TODO: rename to [event_id]
		, [extended_events_file_id]			int NOT NULL
		, [extended_event]					xml NOT NULL
	)
	WITH (DATA_COMPRESSION = Page);
END

-- each dataprovider file has many datasets
IF OBJECT_ID('dbo.extended_event_values') IS NULL
BEGIN
	CREATE TABLE [dbo].[extended_event_values] (
		  [event_id]						int primary key    -- TODO: rename to [event_id]
		, [event_type]						varchar(250)
		, [timestamp]						datetime
		, [database_id]						bigint
		, [source_database_id]				bigint
		, [database_name]					varchar(250)
		, [duration_micro_s]				bigint
		, [object_id]						bigint
		, [object_name]						varchar(max)
		, [object_type]						varchar(250)
		, [row_count]						bigint
		, [last_row_count]					bigint
		, [cpu_time]						bigint
		, [page_server_reads]				bigint
		, [physical_reads]					bigint
		, [logical_reads]					bigint
		, [writes]							bigint
		, [spills]							bigint
		, [nest_level]						bigint
		, [line_number]						bigint
		, [offset]							bigint
		, [offset_end]						bigint
		, [client_app_name]					varchar(250)
		, [batch_text]						varchar(max)
		, [batch_text_checksum]				as CASE WHEN [batch_text] IS NULL THEN NULL ELSE checksum([batch_text]) END PERSISTED
		, [sql_text]						varchar(max)
		, [sql_text_checksum]				as CASE WHEN [sql_text] IS NULL THEN NULL ELSE checksum([sql_text]) END PERSISTED
		, [statement]						varchar(max)
		, [statement_text_checksum]			as CASE WHEN [statement] IS NULL THEN NULL ELSE checksum([statement]) END PERSISTED
		, [overall_checksum]				AS checksum(COALESCE([sql_text], [statement], [batch_text])) PERSISTED
		, [query_hash]						bigint
		, [activity_guid]					uniqueidentifier
		, [activity_sequence]				bigint
	)
	WITH (DATA_COMPRESSION = Page);

	CREATE INDEX idx_extended_event_values__activity_guid__activity_sequence
	ON [dbo].[extended_event_values] ([activity_guid], [activity_sequence])
	WITH (DATA_COMPRESSION = Page)

	CREATE INDEX idx_extended_event_values__overall_checksum
	ON [dbo].[extended_event_values] ([overall_checksum])
	WITH (DATA_COMPRESSION = Page)

	CREATE NONCLUSTERED INDEX idx_extended_event_values__duration_micro_s
	ON [dbo].[extended_event_values] ([duration_micro_s])
	INCLUDE ([activity_guid])
	WITH (DATA_COMPRESSION = Page)
END

--TRUNCATE TABLE [dbo].[extended_events_file]
--TRUNCATE TABLE [dbo].[extended_event]

DECLARE @files TABLE ([file_id] int identity(1,1) PRIMARY KEY, [file_name] varchar(250))

DECLARE 
	  @path		varchar(1000) = N'D:\SQL Extended Events'
	, @cmd		varchar(1000)

SELECT @cmd = 'DIR "' + @path + '\*.xel" /b'

PRINT @cmd

INSERT @files ([file_name])
EXEC master..xp_cmdshell @cmd

INSERT [dbo].[extended_events_file] ([file_path_and_name], [extended_events_file_name])
SELECT DISTINCT @path + '\' + [file_name], [file_name] 
FROM @files as f
WHERE LEN([file_name]) > 0
AND NOT EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[extended_events_file] as x
	WHERE x.[extended_events_file_name] = f.[file_name]
)


GO
----------------------------------------------------------------------------------------
-- open the files


DECLARE 
	  @content								xml
	, @fetchStatus							int = 0
	, @extended_events_file_path_and_name	varchar(1000)
	, @extended_events_file_id				int


DECLARE f CURSOR 
FOR SELECT [extended_events_file_id], [file_path_and_name] 
FROM [dbo].[extended_events_file]
--WHERE [extended_events_file_id] IN (6,7,9)
OPEN f

WHILE @fetchStatus = 0
BEGIN
	FETCH NEXT FROM f INTO @extended_events_file_id, @extended_events_file_path_and_name

	SELECT @fetchStatus = @@FETCH_STATUS

	IF @fetchStatus = 0
	BEGIN
		--PRINT @obex_file_path_and_name
		;WITH ee as (
			SELECT 
				  @extended_events_file_id as extended_events_file_id
				, CAST(x.event_data as xml) as event_data
			FROM sys.fn_xe_file_target_read_file(@extended_events_file_path_and_name, NULL, NULL, NULL) as x
		)
		INSERT [dbo].[extended_event] ( 
			  [extended_events_file_id]
			, [extended_event]			
		)
		SELECT 
			  ee.[extended_events_file_id]
			, ee.event_data as [extended_event]			
		FROM ee
	END
END

CLOSE f
DEALLOCATE f

GO
TRUNCATE TABLE [dbo].[extended_event_values];
go

DECLARE 
	  @start int = 0  
	, @chunk_size int = 100000
	, @end int

WHILE EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[extended_event]
	WHERE event_id > @start
)
BEGIN
	SELECT @end = @start + @chunk_size - 1

	INSERT [dbo].[extended_event_values] (
		  [event_id] -- TODO: rename to [event_id]
		, [event_type]			
		, [timestamp]			
		, [database_id]			
		, [source_database_id]	
		, [database_name]		
		, [duration_micro_s]	
		, [object_id]			
		, [object_name]			
		, [object_type]			
		, [row_count]			
		, [last_row_count]		
		, [cpu_time]			
		, [page_server_reads]	
		, [physical_reads]		
		, [logical_reads]		
		, [writes]				
		, [spills]				
		, [nest_level]			
		, [line_number]			
		, [offset]				
		, [offset_end]			
		, [client_app_name]		
		, [batch_text]			
		, [sql_text]			
		, [statement]			
		, [activity_guid]		
		, [activity_sequence]	
	)
	SELECT
		  ee.[event_id] -- TODO: rename to [event_id]
		, [event_type]				= e.[event].value('@name', 'varchar(250)') 
		, [timestamp]				= e.[event].value('@timestamp', 'datetime') 
		, [database_id]				= e.[event].value('./data[@name="database_id"][1]/value[1]', 'int') 
		, [source_database_id]		= e.[event].value('./data[@name="source_database_id"][1]/value[1]', 'int') 
		, [database_name]			= e.[event].value('./action[@name="database_name"][1]/value[1]', 'varchar(250)') 
		, [duration_micro_s]		= e.[event].value('./data[@name="duration"][1]/value[1]', 'bigint') 	
		, [object_id]				= e.[event].value('./data[@name="object_id"][1]/value[1]', 'int') 
		, [object_name]				= e.[event].value('./data[@name="object_name"][1]/value[1]', 'varchar(250)') 
		, [object_type]				= e.[event].value('./data[@name="object_id"][1]/text[1]', 'varchar(250)') 
		, [row_count]				= e.[event].value('./data[@name="row_count"][1]/value[1]', 'bigint') 
		, [last_row_count]			= e.[event].value('./data[@name="last_row_count"][1]/value[1]', 'bigint') 
		, [cpu_time]				= e.[event].value('./data[@name="cpu_time"][1]/value[1]', 'bigint') 
		, [page_server_reads]		= e.[event].value('./data[@name="page_server_reads"][1]/value[1]', 'bigint') 
		, [physical_reads]			= e.[event].value('./data[@name="physical_reads"][1]/value[1]', 'bigint') 
		, [logical_reads]			= e.[event].value('./data[@name="logical_reads"][1]/value[1]', 'bigint') 
		, [writes]					= e.[event].value('./data[@name="writes"][1]/value[1]', 'bigint') 
		, [spills]					= e.[event].value('./data[@name="spills"][1]/value[1]', 'bigint') 
		, [nest_level]				= e.[event].value('./data[@name="nest_level"][1]/value[1]', 'bigint') 
		, [line_number]				= e.[event].value('./data[@name="line_number"][1]/value[1]', 'bigint') 
		, [offset]					= e.[event].value('./data[@name="offset"][1]/value[1]', 'bigint') 
		, [offset_end]				= e.[event].value('./data[@name="offset_end"][1]/value[1]', 'bigint') 
		, [client_app_name]			= e.[event].value('./action[@name="client_app_name"][1]/value[1]', 'varchar(250)')
		, [batch_text]				= e.[event].value('./data[@name="batch_text"][1]/value[1]', 'varchar(max)') 
		, [sql_text]				= e.[event].value('./action[@name="sql_text"][1]/value[1]', 'varchar(max)') 
		, [statement]				= e.[event].value('./data[@name="statement"][1]/value[1]', 'varchar(max)') 
		, [activity_guid]			= LEFT(e.[event].value('./action[@name="attach_activity_id"][1]/value[1]', 'varchar(250)'), 36) 
		, [activity_sequence]		= SUBSTRING(e.[event].value('./action[@name="attach_activity_id"][1]/value[1]', 'varchar(250)'), 38, 32) 
	FROM [dbo].[extended_event] as ee
	CROSS APPLY ee.extended_event.nodes('./event[1]') as e([event])
	WHERE ee.event_id BETWEEN @start AND @end

	-- select top 100 * from dbo.ex

	-- SELECT @start as [start], @@ROWCOUNT as [count_rows]

	SELECT @start = @end + 1
END
select * from dbo.extended_event_values where database_name = 'adaptiva'
select top 1 * from dbo.extended_event_values order by event_id desc
GO
/*
UPDATE ee
SET database_id = did.database_id
FROM dbo.extended_event_values as ee
INNER JOIN (
	SELECT DISTINCT activity_guid, database_id
	FROM dbo.extended_event_values as w
	WHERE database_id IS NOT NULL
) as did
	ON ee.activity_guid = did.activity_guid
WHERE ee.database_id IS NULL


SELECT 
	  overall_checksum
	, count(*) as instances
	, SUM(duration_micro_s) / 1000000 as duration_s
	, database_name
FROM [dbo].[extended_event_values] as v
GROUP BY overall_checksum, database_name
ORDER BY 3 desc



SELECT 
	  overall_checksum
	, count(*) as instances
	, SUM(duration_micro_s) / 1000000 as duration_s
	, database_name
FROM [dbo].[extended_event_values] as v
GROUP BY overall_checksum, database_name
ORDER BY 3 desc



select count(*) from [dbo].[extended_event_values] -- 8,360,774

select extended_event from extended_event


select top 10000 * from [dbo].[extended_event]

select distinct database_id, database_name
from [dbo].[extended_event_values] WITH (NOLOCK)

SELECT MIN(timestamp), MAX(timestamp) FROM [dbo].[extended_event_values]


SELECT  
	  ee.database_name
	, ee.activity_sequence
	, ee.event_type
	, ee.timestamp
	, ee.duration_micro_s 
	, ee.duration_micro_s / 1000000 as duration_s
	, ee.row_count
	, ee.cpu_time
	, ee.logical_reads
	, ee.physical_reads
	, ee.writes
	, ee.spills
	, ee.nest_level
	, ee.line_number
	--, ee.offset
	--, ee.offset_end
	, ee.batch_text
	, ee.sql_text
	, ee.statement
FROM [dbo].[extended_event_values] as ee WITH (NOLOCK)
ORDER BY ee.timestamp

-- just to see some XML
-- select top 20 * from [dbo].[extended_event]

-- most expensive sequences
SELECT  
	  ee.database_name
	--, ee.[source_database_id]
	, ag.sequence_no
	, ee.activity_sequence
	, ag.duration_s as sequence_duration_s
	--, ee.extended_event
	, ee.event_type
	, ee.timestamp
	, ee.duration_micro_s 
	, ee.duration_micro_s / 1000000 as duration_s
	, ee.row_count
	--, ee.last_row_count
	, ee.cpu_time
	, ee.logical_reads
	, ee.physical_reads
	, ee.writes
	, ee.spills
	, ee.nest_level
	, ee.line_number
	--, ee.offset
	--, ee.offset_end
	, ee.batch_text
	, ee.sql_text
	, ee.statement
FROM [dbo].[extended_event_values] as ee WITH (NOLOCK)
INNER JOIN (
	SELECT 
		  activity_guid
		, duration_micro_s / 1000000 as duration_s
		, ROW_NUMBER() OVER (ORDER BY duration_micro_s DESC) as sequence_no
	FROM (
		SELECT 
			  activity_guid
			, max(duration_micro_s) as duration_micro_s
		FROM (
			SELECT top 10000 activity_guid, duration_micro_s
			FROM [dbo].[extended_event_values] WITH (NOLOCK)  -- 300,012,510
			--WHERE overall_checksum = -62411432
			--AND activity_sequence = 1
			ORDER BY duration_micro_s DESC
		) as x
		GROUP BY x.activity_guid
	) as y
) as ag
	ON ee.activity_guid = ag.activity_guid
ORDER BY ag.sequence_no, ee.activity_sequence

select * 
from dbo.extended_event_values WITH (NOLOCK)
--where event_type = 'sql_statement_completed'
--WHERE client_app_name = 'jtds'
where sql_text like '%policystatus%' or batch_text like '%policystatus%' or statement like '%policystatus%'
ORDER BY timestamp desc

select distinct event_type 
from dbo.extended_event_values 
WITH (NOLOCK)

update dbo.extended_event_values 
set database_name = 'adaptiva' 
where client_app_name = 'jtds'

*/
/*
Select client_id as AdaptivaClientId 
from client_ip_info_table as a
INNER JOIN client_machine_names as b 
	on a.client_id = b.client_id
order by b.MachineName


-- VERY IMPORTANT!!!!
-- change this:
Select AdaptivaClientId from a_AdaptivaClientData order by MachineName

--to this:
Select a.client_id as AdaptivaClientId 
from client_ip_info_table as a
INNER JOIN client_machine_names as b 
	on a.client_id = b.client_id
order by b.MACHINE_NAME

*/




