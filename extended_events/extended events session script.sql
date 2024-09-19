IF EXISTS (
	SELECT TOP 1 1
	FROM sys.server_event_sessions
	WHERE [name] = 'watch TSQL'
)
BEGIN
	DROP EVENT SESSION [watch TSQL] ON SERVER 
END
GO

-- VERY IMPORTANT: check the [filename] parameter below
-- it is decorated with an important comment

CREATE EVENT SESSION [watch TSQL] ON SERVER 
ADD EVENT sqlserver.blocked_process_report(
    ACTION(
		 package0.collect_system_time
		,package0.event_sequence
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.cursor_close(
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.cursor_open(
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.cursor_prepare(
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.cursor_recompile(
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.cursor_unprepare(
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.error_reported(
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.lock_deadlock(
	SET collect_database_name=(1),collect_resource_description=(1)
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.lock_deadlock_chain(
	SET collect_database_name=(1),collect_resource_description=(1)
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.module_end(
	SET collect_statement=(1)
    ACTION(
		package0.collect_system_time
		,package0.event_sequence
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.module_start(
	SET collect_statement=(1)
    ACTION(
		package0.event_sequence
		,sqlos.task_time
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.nt_username
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.sp_statement_completed(
	SET collect_object_name=(1), collect_statement=(1)
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.sql_batch_completed(
	SET collect_batch_text=(1)
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.sql_batch_starting(
	SET collect_batch_text=(1)
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.sql_statement_completed(
	SET collect_statement=(1)
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
    WHERE ([package0].[not_equal_i_unicode_string]([sqlserver].[sql_text],N'EXEC sp_unprepare%'))),
ADD EVENT sqlserver.sql_statement_starting(
	SET collect_statement=(1)
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
),
ADD EVENT sqlserver.sql_transaction(
    ACTION(
		 package0.collect_system_time
		,sqlserver.client_app_name
		,sqlserver.client_hostname
		,sqlserver.context_info
		,sqlserver.database_id
		,sqlserver.database_name
		,sqlserver.query_hash
		,sqlserver.session_id
		,sqlserver.sql_text
		,sqlserver.username
	)
) 
ADD TARGET package0.event_file(
	SET 
		-- VERY IMPORTANT: this path to the xel files needs to already exist
		-- before this script is executed
		-- SQL Server won't create the directory if it doesn't exist
		  filename=N'D:\my Extended Events\watch TSQL.xel'
		, max_file_size=(50000)
		, max_rollover_files=(10)
)
WITH (
	  MAX_MEMORY=4096 KB
	, EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS
	, MAX_DISPATCH_LATENCY=30 SECONDS
	, MAX_EVENT_SIZE=0 KB
	, MEMORY_PARTITION_MODE=NONE
	, TRACK_CAUSALITY=ON
	, STARTUP_STATE=OFF
)
GO


