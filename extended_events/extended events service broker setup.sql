IF OBJECT_ID('dbo.extended_events_file') IS NULL
BEGIN
	CREATE TABLE [dbo].[extended_events_file] (
		  [extended_events_file_id] int IDENTITY(1,1) PRIMARY KEY
		, [extended_events_file_name] varchar(1000) NOT NULL
		, [file_path_and_name] varchar(1000) NOT NULL
	)
END

IF OBJECT_ID('dbo.extended_event_values') IS NULL
BEGIN
	CREATE TABLE [dbo].[extended_event] (
		  [event_id]						int identity(1,1) primary key    
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
		, [activity_id]						varchar(250)
		, [activity_guid]					as TRY_CAST(LEFT([activity_id], 36) as uniqueidentifier) PERSISTED
		, [activity_sequence]				as TRY_CAST(REPLACE(REPLACE(SUBSTRING([activity_id], 38, 32), '[', ''), ']', '') as int) PERSISTED
	)
	WITH (DATA_COMPRESSION = Page);
	
	CREATE INDEX idx_extended_event__activity_guid__activity_sequence
	ON [dbo].[extended_event] ([activity_guid], [activity_sequence])
	WITH (DATA_COMPRESSION = Page)

	CREATE INDEX idx_extended_event__overall_checksum
	ON [dbo].[extended_event] ([overall_checksum])
	WITH (DATA_COMPRESSION = Page)

	CREATE NONCLUSTERED INDEX idx_extended_event__duration_micro_s
	ON [dbo].[extended_event] ([duration_micro_s])
	INCLUDE ([activity_guid])
	WITH (DATA_COMPRESSION = Page)
END

	/*
		-- notes on SQL Server's Service Broker:
		-- decent setup tutorial
		-- https://www.youtube.com/watch?v=kbqcaDDCRbc


		-- to enable Service Broker on your database
		ALTER DATABASE [extended_events] SET SINGLE_USER -- WITH ROLLBACK IMMEDIATE
		GO
		ALTER DATABASE [extended_events] set ENABLE_BROKER
		GO
		ALTER DATABASE [extended_events] SET MULTI_USER  


		-- to DISable Service Broker on your database
		ALTER DATABASE [extended_events] SET SINGLE_USER -- WITH ROLLBACK IMMEDIATE
		GO
		ALTER DATABASE [extended_events] set disABLE_BROKER
		GO
		ALTER DATABASE [extended_events] SET MULTI_USER  


		-- is Service Broker enabled:
		SELECT name, service_broker_guid, is_broker_enabled FROM sys.databases where [name] like 'extended%'


		ALTER DATABASE [extended_events] SET TRUSTWORTHY ON;

		-- first define a message type(s)
		CREATE MESSAGE TYPE [//ExtendedEventMessageTypeInitiator] VALIDATION = NONE;
		CREATE MESSAGE TYPE [//ExtendedEventMessageTypeTarget] VALIDATION = NONE;

		-- create a contract
		CREATE CONTRACT [//ExtendedEventsContract] (
			  [//ExtendedEventMessageTypeInitiator]		SENT BY INITIATOR
			, [//ExtendedEventMessageTypeTarget]		SENT BY TARGET
		)

		CREATE QUEUE dbo.initiatorQueue WITH STATUS = ON;
		CREATE SERVICE initiatorService ON QUEUE dbo.initiatorQueue ([//ExtendedEventsContract]) ;
		GO
		CREATE QUEUE dbo.targetQueue WITH STATUS = ON;
		CREATE SERVICE targetService ON QUEUE dbo.targetQueue ([//ExtendedEventsContract]) ;
		GO

		-- above are all the necessary objects
		------------------------------------

		messages landing in sys.transmission_queue and not going to their intended queue?
		Check for error messages in the sys.transmission_queue.
		One I found showed me that I needed to change the owner of the database to sa

		select * from sys.transmission_queue WITH (NOLOCK) ORDER BY enqueue_time DESC

*/
select count(*) from extended_event WITH (NOLOCK)
GO



CREATE OR ALTER PROCEDURE dbo.prc_extract_extended_event_from_queue
AS
BEGIN
	/*
	select count(*) from dbo.commandCurrentCommandHeartbeatQueue with (nolock) -- 21,020,204

	-- disable activation on queue 
	ALTER QUEUE dbo.targetQueue
	WITH STATUS = ON , RETENTION = OFF, ACTIVATION (STATUS = OFF)

	-- disable activation on queue 
	ALTER QUEUE dbo.targetQueue
	WITH STATUS = OFF , RETENTION = OFF


	-- enable activation on queue to start processing messages
	ALTER QUEUE dbo.targetQueue
	WITH STATUS = ON , RETENTION = OFF ,
	ACTIVATION ( 
		  STATUS = ON
		, execute as owner
		, PROCEDURE_NAME = dbo.prc_extract_extended_event_from_queue 
		, MAX_QUEUE_READERS = 10 )

	*/
	DECLARE 
		  @conversation_handle	uniqueidentifier
		, @message_type_name	varchar(1000)
		, @package				nvarchar(max)
		, @command				varchar(1000)
		, @job_execution_id		int
		, @procedure			varchar(250)
		, @target_client_id		int
		, @source_client_id		int
		, @row_id				int
		, @cmd					varchar(1000);

	;RECEIVE 
		  @conversation_handle	= [conversation_handle]
		, @message_type_name	= [message_type_name]
		, @package				= [message_body]  
	FROM dbo.targetQueue;	

	--INSERT note (note) VALUES (CAST(@package as varchar(max)));

	INSERT [dbo].[extended_event] (
		  [event_type]			
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
		, [activity_id]		
	)
	SELECT 
		  [event_type]				= JSON_VALUE(e.[event], '$.Name')  
		, [timestamp]				= TRY_CAST(LEFT(REPLACE(JSON_VALUE(e.[event], '$.Timestamp'), 'T', ' '), 23) as datetime) 
		, [database_id]				= TRY_CAST(JSON_VALUE(e.[event], '$.database_id') as int)  
		, [source_database_id]		= TRY_CAST(JSON_VALUE(e.[event], '$.source_database_id') as int)  
		, [database_name]			= JSON_VALUE(e.[event], '$.database_name')  
		, [duration_micro_s]		= TRY_CAST(JSON_VALUE(e.[event], '$.duration') as bigint)  
		, [object_id]				= TRY_CAST(JSON_VALUE(e.[event], '$.object_id') as int) 
		, [object_name]				= JSON_VALUE(e.[event], '$.object_name') 
		, [object_type]				= JSON_VALUE(e.[event], '$.object_type') 
		, [row_count]				= TRY_CAST(JSON_VALUE(e.[event], '$.row_count') as bigint)
		, [last_row_count]			= TRY_CAST(JSON_VALUE(e.[event], '$.last_row_count') as bigint)
		, [cpu_time]				= TRY_CAST(JSON_VALUE(e.[event], '$.cpu_time') as bigint)
		, [page_server_reads]		= TRY_CAST(JSON_VALUE(e.[event], '$.page_server_reads') as bigint) 
		, [physical_reads]			= TRY_CAST(JSON_VALUE(e.[event], '$.physical_reads') as bigint) 
		, [logical_reads]			= TRY_CAST(JSON_VALUE(e.[event], '$.logical_reads') as bigint) 
		, [writes]					= TRY_CAST(JSON_VALUE(e.[event], '$.writes') as bigint) 
		, [spills]					= TRY_CAST(JSON_VALUE(e.[event], '$.spills') as bigint) 
		, [nest_level]				= TRY_CAST(JSON_VALUE(e.[event], '$.nest_level') as bigint) 
		, [line_number]				= TRY_CAST(JSON_VALUE(e.[event], '$.line_number') as bigint) 
		, [offset]					= TRY_CAST(JSON_VALUE(e.[event], '$.offset') as bigint) 
		, [offset_end]				= TRY_CAST(JSON_VALUE(e.[event], '$.offset_end') as bigint) 
		, [client_app_name]			= JSON_VALUE(e.[event], '$.client_app_name')
		, [batch_text]				= JSON_VALUE(e.[event], '$.batch_text')
		, [sql_text]				= JSON_VALUE(e.[event], '$.sql_text')
		, [statement]				= JSON_VALUE(e.[event], '$.statement')
		, [activity_id]				= JSON_VALUE(e.[event], '$.attach_activity_id')
	FROM (
		SELECT p.[value] as [event]
		FROM OPENJSON(@package) as p
		--WHERE p.[type] = 5
	) as e
END
GO

--SELECT COUNT(*) FROM dbo.initiatorQueue WITH (NOLOCK)
select top 1000 * 
, TRY_CAST(REPLACE(REPLACE(SUBSTRING([activity_id], 38, 32), '[', ''), ']', '') as int)
from extended_event
SELECT COUNT(*) FROM dbo.targetQueue WITH (NOLOCK)

SELECT COUNT(*) FROM [dbo].[extended_event] WITH (NOLOCK)  -- 17,700,906, -- 30,966,375, 129,153,094



select name as [Queue], x.[service], is_published, is_schema_published, activation_procedure, execute_as_principal_id, is_activation_enabled, is_receive_enabled, is_retention_enabled, is_poison_message_handling_enabled 
from sys.service_queues as q
CROSS APPLY (
	SELECT TOP 1 s.name as [service]
	FROM sys.services as s
	WHERE q.object_id = s.service_queue_id
) as x
WHERE is_ms_shipped = 0

select * from sys.transmission_queue WITH (NOLOCK) ORDER BY enqueue_time DESC

select * from extended_event_values WITH (NOLOCK)
--select * from note

--truncate table note
--truncate table  extended_event_values


EXEC xp_readerrorlog

-- reset broker and clear out transmission queue
-- ALTER DATABASE extended_events SET NEW_BROKER WITH ROLLBACK IMMEDIATE
GO
CREATE OR ALTER PROCEDURE dbo.prc_enqueue_event_batch 
	@event_data nvarchar(max)
AS
BEGIN
	DECLARE 
		  @dialog_handle						uniqueidentifier

	BEGIN DIALOG @dialog_handle
	FROM SERVICE initiatorService
	TO SERVICE 'targetService' 
	ON CONTRACT [//ExtendedEventsContract]
	WITH ENCRYPTION = OFF;

	SEND ON CONVERSATION @dialog_handle
	MESSAGE TYPE [//ExtendedEventMessageTypeInitiator] (@event_data);

	SELECT @dialog_handle as dialog_handle
END
GO


CREATE OR ALTER VIEW dbo.v_sequences_in_order
AS
	SELECT TOP 100 PERCENT 
		  ee.database_name
		--, ee.[source_database_id]
		, ag.sequence_no
		, ee.activity_sequence
		, ee.duration_micro_s / 1000 as step_duration_ms
		, ag.duration_s as sequence_duration_s
		--, ee.extended_event
		, ee.event_type
		, ee.timestamp
		, ee.duration_micro_s 
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
	FROM [dbo].[extended_event] as ee WITH (NOLOCK)
	INNER JOIN (
		SELECT 
			  activity_guid
			, duration_micro_s / 1000000 as duration_s
			, ROW_NUMBER() OVER (ORDER BY startTime) as sequence_no
		FROM (
			SELECT 
				  activity_guid
				, max(duration_micro_s) as duration_micro_s
				, min([timestamp]) as startTime
			FROM [dbo].[extended_event] WITH (NOLOCK)  -- 300,012,510
			GROUP BY activity_guid
		) as y
	) as ag
		ON ee.activity_guid = ag.activity_guid
	ORDER BY ag.sequence_no, ee.activity_sequence
GO


CREATE OR ALTER VIEW dbo.v_most_expensive_sequences
AS
	SELECT TOP 100 PERCENT   
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
	FROM [dbo].[extended_event] as ee WITH (NOLOCK)
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
				FROM [dbo].[extended_event] WITH (NOLOCK)  -- 300,012,510
				--WHERE overall_checksum = -62411432
				--AND activity_sequence = 1
				ORDER BY duration_micro_s DESC
			) as x
			GROUP BY x.activity_guid
		) as y
	) as ag
		ON ee.activity_guid = ag.activity_guid
	ORDER BY ag.sequence_no, ee.activity_sequence
GO

