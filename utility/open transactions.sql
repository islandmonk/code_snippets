/*
-- split this code into two query windows by tearing at the dotted line
-- This code will open a transaction and leave it open

---
begin tran

UPDATE [dbo].[ABSTRACTPATCHBOT]
SET BOTWORKFLOW = 0
WHERE OBJECTID = 2693


-- commit tran -- highlighting this command and executing it will commit and close

-- rollback tran  -- rollback transaction and close it

---
-- run the code above in one window. Then run the code below on another.
*/


------------------------------------------------------------------------
-- review open transactions

SELECT 
	  st.[session_id] as locked_spid 
    , dt.database_id 
	, db.[name] as [database_name]
    , dt.database_transaction_log_bytes_used 
    , dt.database_transaction_log_bytes_reserved 
    , datediff(second, s.last_request_end_time, getdate()) as secconds_since_last_request_end
FROM sys.dm_tran_session_transactions as st
INNER JOIN sys.dm_tran_database_transactions as dt
    ON dt.transaction_id = st.transaction_id
INNER JOIN sys.databases as db
	ON dt.database_id = db.database_id
INNER JOIN sys.dm_exec_sessions as s
    ON s.session_id = st.session_id

-- the secconds_since_last_request_end value can be used as a 
-- criterion for deciding if a transaction has been open long.
-- To make an open transaction go away
-- kill the locked_spid that has the open transaction

-- kill 57