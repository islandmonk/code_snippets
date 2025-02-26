GO
/****** Object:  StoredProcedure [dbo].[prc_maintain_currently_running_commands]    Script Date: 7/24/2024 3:20:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_maintain_currently_running_commands]
AS
BEGIN
	/*
	The intention here is to have a table that can be read very quickly to give
	the server's currently running commands. This table will be read repeatedly by
	any custodian health page on whatever browsers have it up. The read against this
	table needs to be fast. And the table needs to be near-real-time so I want it 
	refreshed every second.

	This procedure refreshes that table.

	I want this procedure to run every one (1) second. SQL Server Agent won't do that.
	So I created a wrapper procedure: [dbo].[prc_maintain_currently_running_commands_wrapper]
	Agent kicks this off every minute. The wrapper makes multiple calls to 
	this procedure to give the resolution that I want.

	The procedure called by clients to get this command snapshot is:
	[dbo].[prc_get_currently_running_commands]

	*/
	DECLARE 
		  @one_hour_ms INT = 2.16e6
		, @iterations int = 0
		, @max_iterations int = 9

	CREATE TABLE #spids (
		  [SPID]				int
		, [status]				varchar(100)
		, [login]				varchar(100)
		, [host_name]			varchar(100)
		, [blocked_by]			varchar(100)
		, [DBName]				varchar(100)
		, [command]				varchar(max)
		, [cpu_time]			bigint
		, [disk_io]				bigint
		, [last_batch]			varchar(100)
		, [program_name]		varchar(100)
		, [SPID_ii]				int
		, [request_id]			int
		, [class]				varchar(100)
	)

	CREATE TABLE #spid_ii (
		  [SPID]				int			PRIMARY KEY
		, [status]				varchar(100)
		, [login]				varchar(100)
		, [host_name]			varchar(100)
		, [blocked_by]			varchar(100)
		, [DBName]				varchar(100)
		, [command]				varchar(max)
		, [cpu_time]			bigint
		, [disk_io]				bigint
		, [program_name]		varchar(100)
		, [class]				varchar(100)
		, [threads]				int
		, [reads]				int
		, [writes]				int
		, [ip]					varchar(100)
	)


	CREATE TABLE #status (
		  [status] varchar(100) PRIMARY KEY
		, [included] bit NOT NULL
		, [ord] int NOT NULL
	);

	CREATE TABLE #program (
		  [program_name] varchar(256) PRIMARY KEY
		, [program_name_short] varchar(50)
	)

	INSERT #program ([program_name], [program_name_short])
	SELECT [program_name], [program_name_short]
	FROM (
		VALUES 
			  ('Microsoft JDBC Driver for SQL Server          ', 'SQL JDBC')
			, ('Microsoft SQL Server Management Studio - Query', 'SSMS Query')
			, ('Microsoft SQL Server Management Studio        ', 'SSMS')
			, ('SQLAgent - Email Logger                       ', 'Agent Email Logger')
			, ('SQLAgent - Job invocation engine              ', 'Agent Job Invocation')
			, ('SQLAgent - Generic Refresher                  ', 'Agent Generic Refresher')
			, ('SQLAgent - Contained AG                       ', 'Agent Contained AG')
			, ('SQLServerCEIP                                 ', 'CEIP')
	) as p ([program_name], [program_name_short])

	INSERT #status ([status], [included], [ord])
	SELECT [status], [included], [ord]
	FROM (
		VALUES 
			  ('Dormant', 1, 2)
			, ('Running', 1, 1)
			, ('Background', 0, 3)
			, ('Rollback', 1, 4)
			, ('Pending', 1, 5)
			, ('Runnable', 1, 6)
			, ('Spinloop', 1, 7)
			, ('Suspended', 1, 8)
			, ('Sleeping', 0, 9)
	) AS s ([status], [included], [ord]);

	CREATE TABLE #commands (
		  [session_id]			int primary key
		, [text]				varchar(max)
		, [text_short]			varchar(max)
		, [status]				varchar(100)
		, [command]				varchar(max)
		, [cpu_time]			varchar(100)
		, [total_elapsed_time]	varchar(100)
	);


	INSERT #spids (
		  [SPID] 
		, [status] 
		, [login] 
		, [host_name] 
		, [blocked_by] 
		, [DBName] 
		, [command] 
		, [cpu_time] 
		, [disk_io] 
		, [last_batch] 
		, [program_name] 
		, [SPID_ii] 
		, [request_id] 
	)
	EXEC sp_who2;

	DELETE #spids 
	WHERE [SPID] = @@SPID
	OR [DBName] = 'custodian';

	--UPDATE sp
	--SET 
	--	  [status]			= TRIM([status])
	--	, [DBName]			= TRIM(ISNULL([DBName], ''))
	--	, [login]			= TRIM(REPLACE([login], 'ARISTANETWORKS\', ''))
	--	, [host_name]		= TRIM([host_name])
	--	, [command]			= TRIM([command])
	--	, [program_name]	= CASE 
	--							WHEN sp.[program_name] LIKE '%Job 0x%'
	--							THEN COALESCE([dbo].[GetJobNameFromProgramName](sp.[program_name]), sp.[program_name])
	--							WHEN sp.[program_name] LIKE 'SQL Server Profiler%'
	--							THEN 'SQL Server Profiler'
	--							ELSE COALESCE(p.[program_name_short], sp.[program_name])
	--						  END
	--FROM #spids as sp
	--LEFT OUTER JOIN #program as p
	--	ON sp.[program_name] = p.[program_name]

	UPDATE #spids
	SET 
		  [login]	= 'SQL Agent'
	WHERE [login] LIKE '%SQLSERVERAGENT%'

	--UPDATE #spids
	--SET 
	--	  [host_name] = 'Cube - ' + (
	--						SELECT [value] 
	--						FROM dbo.fnt_single_column_csv([host_name], '-') as x 
	--						WHERE ord = 2
	--					)
	--WHERE [host_name] LIKE 'ec-%'

	UPDATE s
	SET [class] = 'blocking'
	FROM #spids as s
	WHERE EXISTS (
		SELECT TOP 1 1
		FROM #spids as x
		WHERE s.[SPID] <> x.[SPID]
		AND s.[SPID] = TRY_CAST(x.[blocked_by] as int)
	);

	UPDATE #spids
	SET [class] = 'blocked'
	WHERE TRY_CAST([blocked_by] as int) <> [SPID]

	--DELETE sp
	--FROM #spids as sp
	--LEFT OUTER JOIN [dbo].[database] as db
	--	ON sp.[DBName] = db.[database_name]
	--LEFT OUTER JOIN #status as st
	--	ON sp.[status] = st.[status]
	--WHERE NOT (
	--	(ISNULL(db.[is_active], 0) = 1 AND ISNULL(sp.[status], '') NOT IN ('background', 'sleeping'))
	--	OR ISNULL(st.[included], 0) = 1
	--	OR ISNULL(sp.[class], '') IN ('blocked', 'blocking')
	--	OR ISNULL(sp.[status], '') NOT IN ('background', 'sleeping')
	--	-- OR ISNULL(sp.[login], '') = 'SQL Agent'
	--);

	INSERT #spid_ii (
		  [SPID]
		, [login]
		, [host_name]
		, [DBName]
		, [program_name]
		, [command]
		, [class]
		, [disk_io]
		, [cpu_time]
		, [threads]
	)
	SELECT 
		[SPID]
		, MAX(s.[login])
		, MAX(s.[host_name])
		, MAX(s.[DBName])
		, MAX(s.[program_name])
		, MAX(s.[command])
		, MAX(s.[class])
		, SUM(s.[disk_io]) as [disk_io]
		, SUM(s.[cpu_time]) as [cpu_time]
		, COUNT(*) as [threads]
	FROM #spids as s
	GROUP BY [SPID]

	UPDATE ii
	SET 
		[status] = s.[status]
		, [blocked_by] = ISNULL(bb.[blocked_by], '-')
	FROM #spid_ii as ii
	OUTER APPLY (
		SELECT STRING_AGG(y.blocked_by, ', ') WITHIN GROUP (ORDER BY TRY_CAST(y.blocked_by as int)) as blocked_by
		FROM (
			SELECT DISTINCT x.blocked_by
			FROM #spids as x
			WHERE ii.[SPID] = x.[SPID]
			AND TRY_CAST(x.blocked_by as int) <> x.SPID
		) as y
	) as bb
	INNER JOIN (
		SELECT 
			x.[SPID]
			, st.[status]
			, ROW_NUMBER() OVER (PARTITION BY x.[SPID] ORDER BY st.[ord]) as rn
		FROM #spids as x
		INNER JOIN #status as st
			ON x.[status] = st.[status]
	) as s
		ON ii.[SPID] = s.[SPID]
		AND s.rn = 1

	--SELECT * FROM #spids

	--SELECT * FROM #spid_ii

	INSERT #commands (
		  [session_id]
		, [text]	
		, [text_short]
		, [status]		
		, [command]		
		, [cpu_time]	
		, [total_elapsed_time]		
	)
	SELECT 
		  [req].[session_id] 
		, TRIM([sqltext].[TEXT]) 
		, TRIM([sqltext].[TEXT])  
		, [req].[status] 
		, [req].[command] 
		, [req].[cpu_time]
		, [req].[total_elapsed_time]
		--[dbo].[fns_comma_delimited_int](CAST([req].[cpu_time] as bigint)) as [cpu_time]
		--, [dbo].[fns_comma_delimited_int](CAST([req].[total_elapsed_time] as bigint)) as [total_elapsed_time] 
	FROM sys.dm_exec_requests as req
	CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext
	WHERE req.session_id <> @@SPID

	UPDATE #commands
	SET [text_short] = LEFT([text], 500)
	--WHERE LEN([text]) > 250

	UPDATE #commands
	SET [text_short] = LEFT([text_short], CHARINDEX('@', [text_short]) - 1)
	WHERE [text_short] LIKE '%CREATE PROCEDURE%[@]%'
	OR [text_short] LIKE '%EXEC %[@]%'

	UPDATE #commands
	SET [text_short] = LEFT([text_short], PATINDEX('% AS %', [text_short]) - 1)
	WHERE [text_short] LIKE 'CREATE PROCEDURE % AS %'

	UPDATE #commands
	SET [text_short] = TRIM(REPLACE([text_short], 'CREATE PROCEDURE ', ''))
	WHERE [text_short] LIKE '%CREATE PROCEDURE %'

	UPDATE #commands
	SET [text_short] = REPLACE([text_short], 'EXEC ', '')
	WHERE [text_short] LIKE 'EXEC %'

		SELECT 
			  [spid].[SPID]
			, [spid].[status]
			, [spid].[DBName]
			, [spid].[login]
			, [spid].[program_name]
			, [spid].[blocked_by]
			, [spid].[host_name]
			, c.[cpu_time] 
			, c.[total_elapsed_time]
			--, [dbo].[fns_comma_delimited_int]([spid].[cpu_time]) as [cpu_time_ms] 
			--, [dbo].[fns_comma_delimited_int]([spid].[disk_io]) as [disk_io] 
			, [spid].[threads]
			, c.[text_short]
			, c.[text]
			, c.[command]
			, [spid].[class]
			, REPLACE(x.client_net_address, '<local machine>', 'WPRD') as [ip]
			--, [dbo].[fns_comma_delimited_int](x.[TotalReads]) as [reads]
			--, [dbo].[fns_comma_delimited_int](x.[TotalWrites]) as [writes]
		FROM #spid_ii as spid
		LEFT OUTER JOIN #commands as c
			ON [spid].[SPID] = [c].[session_id]
		LEFT OUTER JOIN (
			SELECT  
				  ess.session_id
				, ecs.client_net_address
				, ecs.client_tcp_port
				, ess.[program_name]
				, ess.[host_name]
				, ess.login_name
				, SUM(num_reads) TotalReads
				, SUM(num_writes) TotalWrites
				, COUNT(ecs.session_id) AS SessionCount
			FROM sys.dm_exec_sessions AS ess WITH (NOLOCK) 
			INNER JOIN sys.dm_exec_connections AS ecs WITH (NOLOCK) 
				ON ess.session_id = ecs.session_id 
			GROUP BY    
				  ess.session_id
				, ecs.client_net_address
				, ecs.client_tcp_port
				, ess.[program_name]
				, ess.[host_name]
				, ess.login_name
		) as x
			ON spid.[spid] = x.[session_id];

	/*
		-- other useful system view

		SELECT  
			  ess.session_id
			, ecs.client_net_address
			, ecs.client_tcp_port
			, ess.[program_name]
			, ess.[host_name]
			, ess.login_name
			, SUM(num_reads) TotalReads
			, SUM(num_writes) TotalWrites
			, COUNT(ecs.session_id) AS SessionCount
		FROM sys.dm_exec_sessions AS ess WITH (NOLOCK) 
		INNER JOIN sys.dm_exec_connections AS ecs WITH (NOLOCK) 
			ON ess.session_id = ecs.session_id 
		GROUP BY    
			  ess.session_id
			, ecs.client_net_address
			, ecs.client_tcp_port
			, ess.[program_name]
			, ess.[host_name]
			, ess.login_name
		ORDER BY SessionCount DESC;

	*/
END
