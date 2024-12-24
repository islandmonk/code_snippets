sp_who2
GO
CREATE OR ALTER PROCEDURE [dbo].[prc_populate_current_command_queue]
	@package varchar(max) = NULL
	, @job_execution_id int = NULL
AS
BEGIN
	/*
	This procedure allows us to send a message to the Service Broker queue [dbo].[commandQueue]. Once the message
	is sent, the procedure that makes the call to this is free to move on. This facilitates an asynchronous
	call approach in SQL Server. In other words, it allows us to 'fire and forget'

	When the message lands in the queue, the 'activation' procedure for the Service Broker queue [dbo].[commandQueue]
	([dbo].[prc_receive_from_command_queue]) is invoked. [dbo].[prc_receive_from_command_queue] will interpret 
	the message and act appropriately.
	*/

	/*
		-- notes on SQL Server's Service Broker:
		-- decent setup tutorial
		-- https://www.youtube.com/watch?v=kbqcaDDCRbc


		-- to enable Service Broker on your database
		ALTER DATABASE [database_name] SET SINGLE_USER -- WITH ROLLBACK IMMEDIATE
		GO
		ALTER DATABASE [database_name] set ENABLE_BROKER
		GO
		ALTER DATABASE [database_name] SET MULTI_USER  


		-- to DISable Service Broker on your database
		ALTER DATABASE [database_name] SET SINGLE_USER -- WITH ROLLBACK IMMEDIATE
		GO
		ALTER DATABASE [database_name] set disABLE_BROKER
		GO
		ALTER DATABASE [database_name] SET MULTI_USER  


		-- is Service Broker enabled:
		SELECT name, service_broker_guid, is_broker_enabled FROM sys.databases where [name] like 'custodian%'


		ALTER DATABASE [custodian] SET TRUSTWORTHY ON;

		CREATE MESSAGE TYPE [//AristaMessageTypeInitiator] VALIDATION = NONE;

		CREATE MESSAGE TYPE [//AristaMessageTypeCurrentCommandHeartbeat] VALIDATION = NONE;

		CREATE CONTRACT [//AristaContract] ([//AristaMessageTypeInitiator] SENT BY INITIATOR, [//AristaMessageTypeCurrentCommandHeartbeat] SENT BY TARGET)

		CREATE QUEUE dbo.commandQueueInitiator WITH STATUS = ON;
		CREATE QUEUE dbo.commandCurrentCommandHeartbeatQueue WITH STATUS = ON;
		GO
		CREATE SERVICE commandQueueInitiatorService ON QUEUE dbo.commandQueueInitiator ([//AristaContract]) ;
		CREATE SERVICE commandCurrentCommandHeartbeatQueueService ON QUEUE dbo.commandCurrentCommandHeartbeatQueue ([//AristaContract]) ;
		GO

		-- above are all the necessary objects
		------------------------------------


		-- enable activation on queue to start processing messages
		ALTER QUEUE dbo.commandCurrentCommandHeartbeatQueue
		WITH STATUS = ON , RETENTION = OFF ,
		ACTIVATION ( 
			STATUS = ON
			, execute as owner
			, PROCEDURE_NAME = dbo.prc_maintain_currently_running_commands_activation 
			, MAX_QUEUE_READERS = 1 )


		-- stop activation on queue 
		ALTER QUEUE dbo.commandCurrentCommandHeartbeatQueue
		WITH STATUS = ON , RETENTION = OFF ,
		ACTIVATION (drop)


		-- DISable activation on queue to start processing messages
		ALTER QUEUE dbo.commandQueue
		WITH STATUS = ON , RETENTION = OFF ,
		ACTIVATION ( STATUS = OFF )


		DECLARE 
			@dialog_handle uniqueidentifier
			, @message_body varchar(1000) = 'this is a service broker message.'

		BEGIN DIALOG @dialog_handle
		FROM SERVICE commandQueueInitiatorService
		TO SERVICE 'commandCurrentCommandHeartbeatQueueService' 
		ON CONTRACT [//AristaContract]
		WITH ENCRYPTION = OFF;

		SEND ON CONVERSATION @dialog_handle
		MESSAGE TYPE [//AristaMessageTypeInitiator] (@message_body);

		SELECT * FROM dbo.commandCurrentCommandHeartbeatQueue  WITH (NOLOCK)

		select *
		from sys.services as s
		INNER JOIN sys.service_contract_usages as cu
			ON s.service_id = cu.service_id
		INNER JOIN sys.service_contracts as sc
			ON cu.service_contract_id = sc.service_contract_id
		LEFT OUTER JOIN sys.service_queues as q
			ON s.service_queue_id = q.[object_id]
		INNER JOIN sys.objects as o
			ON s.service_queue_id = o.[object_id]

		select * from sys.service_queues

		select * from sys.service_contract_usages

		select * from sys.service_contracts

		select * from dbo.commandCurrentCommandHeartbeatQueue

		select COUNT(*) from dbo.commandCurrentCommandHeartbeatQueue WITH (NOLOCK)


		see [dbo].[prc_receive_from_command_queue] for code regrding
		receiving from a queue

		possible speedbumps:
		An exception occurred while enqueueing a message in the target queue. Error: 33009, State: 2. The database owner SID recorded in the master database differs from the database owner SID recorded in database 'custodian'
		solution: EXEC sp_changedbowner 'aristanetworks\doug.hills'

		An exception occurred while enqueueing a message in the target queue. Error: 33019, State: 1. Cannot create implicit user for the special login 'sa'.
		solution: database doesn't have a legitimate owner. I set the owner to aristanetworks\doug.hills
					I don't know why it doesn't like sa as owner
	*/
 	DECLARE 
		  @count_messages int
		, @message varchar(20) = '{}'

	SELECT @count_messages = COUNT(*) 
	FROM dbo.commandCurrentCommandHeartbeatQueue with (nolock)

	WHILE @count_messages < @max_iterations
	BEGIN
		DECLARE 
			@dialog_handle uniqueidentifier

		BEGIN DIALOG @dialog_handle
		FROM SERVICE commandQueueInitiatorService
		TO SERVICE 'commandCurrentCommandHeartbeatQueueService' 
		ON CONTRACT [//AristaContract]
		WITH ENCRYPTION = OFF;

		SEND ON CONVERSATION @dialog_handle
		MESSAGE TYPE [//AristaMessageTypeInitiator] (@message);

		SELECT @count_messages += 1;
	END
END

GO



CREATE OR ALTER PROCEDURE [dbo].[prc_create_scrambled_lifecycle_activation]
AS
BEGIN
	/*
	The intention here is to have a table that can be read very quickly to give
	the server's currently running commands. This table will be read repeatedly by
	any custodian health page on whatever browsers have it up. The read against this
	table needs to be fast. And the table needs to be near-real-time.

	This procedure calls the procedure that refreshes that table.

	I want the table refreshed every second or so. This procedure is the activation
	procedure for the 

	select count(*) from dbo.commandCurrentCommandHeartbeatQueue with (nolock) -- 21,020,204

	-- disable activation on queue 
	ALTER QUEUE dbo.commandCurrentCommandHeartbeatQueue
	WITH STATUS = OFF , RETENTION = OFF


	-- enable activation on queue to start processing messages
	ALTER QUEUE dbo.commandCurrentCommandHeartbeatQueue
	WITH STATUS = ON , RETENTION = OFF ,
	ACTIVATION ( 
		  STATUS = ON
		, execute as owner
		, PROCEDURE_NAME = dbo.prc_create_scrambled_lifecycle_activation 
		, MAX_QUEUE_READERS = 10 )
	*/
	DECLARE 
		  @conversation_handle	uniqueidentifier
		, @message_type_name	varchar(1000)
		, @package				varchar(max)
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
	FROM dbo.commandCurrentCommandHeartbeatQueue;	

	SELECT 
		  @source_client_id = JSON_VALUE(@package, '$.source_client_id')
		, @target_client_id = JSON_VALUE(@package, '$.target_client_id')

	--SELECT @source_client_id as source_client_id, @target_client_id as target_client_id

	SELECT @cmd = 
		'dbo.prc_create_scrambled_lifecycle '
			+ '@target_client_id = ' + CAST(@target_client_id as varchar(32)) + ' '
			+ ', @source_client_id = ' + CAST(@source_client_id as varchar(32))

	EXEC (@cmd)

	--EXEC dbo.prc_create_scrambled_lifecycle @target_client_id = @target_client_id, @source_client_id = @source_client_id;

	--SELECT COUNT(*) as patch_fact_client_metric
	--FROM patch_fact_client_metric
	--WHERE client_id = @target_client_id

	--SELECT COUNT(*) as patch_fact_patch_client_deployment_result
	--FROM patch_fact_patch_client_deployment_result
	--WHERE client_id = @target_client_id

	--SELECT COUNT(*) as patch_fact_patch_client_status
	--FROM patch_fact_patch_client_status
	--WHERE client_id = @target_client_id

	--SELECT COUNT(*) as patch_fact_product_client_status
	--FROM patch_fact_product_client_status
	--WHERE client_id = @target_client_id

END


GO


SELECT c.CLIENT_ID as target_client_id, c.x_id as source_client_id 
FROM dbo.CLIENT_IP_INFO_TABLE as c
WHERE s_id IS NOT NULL
AND x_id IS NOT NULL
AND EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_client_metric] as x
	WHERE c.CLIENT_ID = x.client_id
)
AND EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_patch_client_deployment_result] as x
	WHERE c.CLIENT_ID = x.client_id
)
AND EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_patch_client_status] as x
	WHERE c.CLIENT_ID = x.client_id
)
AND EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_product_client_status] as x
	WHERE c.CLIENT_ID = x.client_id
)


select * from dbo.patch_fact_patch_client_status
where client_id = 403 order by patch_id, insert_timestamp

select * from dbo.patch_fact_patch_client_status
where client_id = 1456 order by patch_id, insert_timestamp

declare @json varchar(max) = '{"source_client_id": 312, "target_client_id": 10}'

	SELECT JSON_VALUE(@json, '$.source_client_id') as source_client_id
		  source_client_id
		, target_client_id
	FROM OPENJSON(@package)
	WITH (
		  target_client_id int
		, source_client_id  int
	);



SELECT c.CLIENT_ID as target_client_id, c.x_id as source_client_id 
FROM dbo.CLIENT_IP_INFO_TABLE as c WITH (NOLOCK)
WHERE s_id IS NOT NULL
AND x_id IS NOT NULL

AND NOT EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_client_metric] as x WITH (NOLOCK)
	WHERE c.CLIENT_ID = x.client_id
)
AND NOT EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_patch_client_deployment_result] as x WITH (NOLOCK)
	WHERE c.CLIENT_ID = x.client_id
)
AND NOT EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_patch_client_status] as x WITH (NOLOCK)
	WHERE c.CLIENT_ID = x.client_id
)
AND NOT EXISTS (
	SELECT TOP 1 1
	FROM [dbo].[patch_fact_product_client_status] as x WITH (NOLOCK)
	WHERE c.CLIENT_ID = x.client_id
)


select top 5 message_enqueue_time, CAST(message_body as VARCHAR(max)) as message_body, message_body
from commandCurrentCommandHeartbeatQueue as q WITH (NOLOCK)
ORDER BY message_enqueue_time


/*
target_client_id	source_client_id
10					312
11					180
12					267
13					132
14					180
15					277
16					341
*/


select * from dbo.patch_fact_patch_client_status
where client_id = 403 order by insert_timestamp, patch_id

select * from dbo.patch_fact_patch_client_status
where client_id = 1966 order by insert_timestamp, patch_id


dbo.prc_create_scrambled_lifecycle @target_client_id = 1966 , @source_client_id = 403


-- encqueu