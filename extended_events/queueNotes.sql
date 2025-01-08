SELECT COUNT(*) FROM dbo.targetQueue WITH (NOLOCK)

SELECT COUNT(*) FROM [dbo].[extended_event] WITH (NOLOCK)  -- 17,700,906, -- 30,966,375, 129,153,094
--truncate table [dbo].[extended_event]

EXEC sp_who2

-- kill 55
-- package size 10000
--  1,651,916  19 minutes
-- 20,628,749  2 hours 42 min

-- with no indexes at first
--   9,224,649	-- 45m
--  29,297,563	-- 2h
--  76,658,169	-- 5h
-- 129,153,094	-- 9h 42m
select top 10000 * from dbo.extended_event WITH (NOLOCK) where activity_sequence = 1