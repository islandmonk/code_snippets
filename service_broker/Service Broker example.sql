ALTER PROCEDURE [dbo].[prc_maintain_currently_running_commands_activation]
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

	-- enable activation on queue to start processing messages
	ALTER QUEUE dbo.commandCurrentCommandHeartbeatQueue
	WITH STATUS = ON , RETENTION = OFF ,
	ACTIVATION ( 
		STATUS = ON
		, execute as owner
		, PROCEDURE_NAME = dbo.prc_maintain_currently_running_commands_activation 
		, MAX_QUEUE_READERS = 1 )
	*/
	DECLARE 
		  @conversation_handle	uniqueidentifier
		, @message_type_name	varchar(1000)
		, @package				varchar(max)
		, @command				varchar(1000)
		, @job_execution_id		int
		, @procedure			varchar(250)
		, @row_id				int;

	;RECEIVE 
		  @conversation_handle	= [conversation_handle]
		, @message_type_name	= [message_type_name]
		, @package				= [message_body]  
	FROM dbo.commandCurrentCommandHeartbeatQueue;	

	EXEC [dbo].[prc_maintain_currently_running_commands];

	WAITFOR DELAY '00:00:01';

END


USE [custodian]
GO
/****** Object:  StoredProcedure [dbo].[prc_populate_current_command_queue]    Script Date: 8/13/2024 9:14:31 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[prc_populate_current_command_queue]
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


		see [dbo].[prc_receive_from_command_queue] for code regarding
		receiving from a queue

		possible speedbumps:
		An exception occurred while enqueueing a message in the target queue. Error: 33009, State: 2. The database owner SID recorded in the master database differs from the database owner SID recorded in database 'custodian'
		solution: EXEC sp_changedbowner 'aristanetworks\doug.hills'

		An exception occurred while enqueueing a message in the target queue. Error: 33019, State: 1. Cannot create implicit user for the special login 'sa'.
		solution: database doesn't have a legitimate owner. I set the owner to aristanetworks\doug.hills
					I don't know why it doesn't like sa as owner
	*/
 	DECLARE 
		  @max_iterations INT = 60 * 60 * 21  -- run every second from 2am ~ 11pm
		, @count_messages int
		, @message varchar(20) = '{}'

	SELECT @max_iterations = 100000 -- just make sure we have 100,000

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

