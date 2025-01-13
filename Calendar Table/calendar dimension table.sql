DROP TABLE IF EXISTS [dbo].[calendar];
GO

CREATE TABLE [dbo].[calendar] ( 
	  [date]                         date                      NOT NULL
	, [date_id]                      int                       NOT NULL
	, [day]                          int                       NOT NULL
	, [month]                        int                       NOT NULL
	, [quarter]                      int                       NOT NULL
	, [year]                         int                       NOT NULL
	, [week_day_name]                nvarchar (50)                 NULL
	, [week_day_abbr]                nchar(3)                      NULL
	, [month_name]                   varchar (50)                  NULL
	, [month_name_abbr]              nchar (3)                     NULL
	, [month_start_date]             date                          NULL
	, [month_end_date]               date                          NULL
	, [week_end_date]                date                          NULL
	, [week_start_date]              date                          NULL
	, [week_of_quarter]              int                           NULL
	, [week_of_year]                 int                           NULL
	, [quarter_start_date]           date                          NULL
	, [quarter_end_date]             date                          NULL
	, [iso_year]                     int                           NULL
	, [iso_week]                     nchar(9)                      NULL
	, [iso_week_number]              int                           NULL
	, [build_week]                   nvarchar(25)                  NULL
	, [iso_week_start_date]          date                          NULL
	, [iso_week_end_date]            date                          NULL
	, [is_first_day_of_month]        bit                       NOT NULL DEFAULT(0)
	, [is_last_day_of_month]         bit                       NOT NULL DEFAULT(0)
	, [is_last_week_of_quarter]      bit                       NOT NULL DEFAULT(0)
	, [year_week]                    AS ((CONVERT([varchar], [year]) + ' - w') + right('00' + CONVERT([varchar], [week_of_year]), (2)))
	, [year_quarter]                 AS ((CONVERT([varchar], [year]) + ' - Q') + CONVERT([varchar],[quarter]))
	, [year_month]                   AS ((CONVERT([varchar], [year]) + ' - M') + right('00' + CONVERT([varchar], [month]), (2)))
)

ALTER TABLE [dbo].[calendar]
ADD CONSTRAINT [PK__dbo_calendar__date_id] PRIMARY KEY NONCLUSTERED
([date_id]) 

CREATE NONCLUSTERED INDEX [idx__dbo_calendar__quarter_end_date] 
ON [dbo].[calendar] ([quarter_end_date]) 

CREATE NONCLUSTERED INDEX [idx_calendar__is_first_day_of_month__month__year] 
ON [dbo].[calendar] ([is_first_day_of_month]) 
INCLUDE ([month], [year]) 

CREATE NONCLUSTERED INDEX [idx_dbo_calendar__date] 
ON [dbo].[calendar] ([date]) 
INCLUDE ([month], [quarter], [year], [quarter_start_date], [month_start_date], [iso_week_start_date]) 

CREATE NONCLUSTERED INDEX [idx_dbo_calendar__day__quarter_start_date] 
ON [dbo].[calendar] ([day], [quarter_start_date]) 

CREATE NONCLUSTERED INDEX [idx_dbo_calendar__iso_week] 
ON [dbo].[calendar] ([iso_week_start_date]) 
INCLUDE ([month], [quarter], [year], [month_end_date], [quarter_end_date]) 

CREATE NONCLUSTERED INDEX [idx_dbo_calendar__month_year__iso_week] 
ON [dbo].[calendar] ([month], [year]) 
INCLUDE ([iso_week]) 

CREATE NONCLUSTERED INDEX [INI_dbo_calendar_quarter_start_date] 
ON [dbo].[calendar] ([quarter_start_date]) 
INCLUDE ([quarter], [year], [year_quarter]) 

CREATE UNIQUE CLUSTERED INDEX [uq_calendar_date] 
ON [dbo].[calendar] ([date]) 

GO




CREATE OR ALTER FUNCTION [dbo].[ISO_year] (@theDate date)
RETURNS int
AS
BEGIN
	/*
		From Wikipedia:
		https://en.wikipedia.org/wiki/ISO_week_date

		ISO Week
		First Week:		
			- the first week of the year with the majority (4 or more) of its days in January
			- Its first day is the monday nearest to Jan 1
			- It has Jan 4 in it

		Last Week:
			- it has the year's last thursday in it
			- it is the last week with the majority of its days (4 or more) in December
			- its middle day, Thursday, falls in the ending year
			- its last day is the Sunday nearest to Dec 31
			- it has Dec 28 in it

	*/
	DECLARE 
		  @date_year					int		= DATEPART(year, @theDate)
		, @date_month					int		= DATEPART(month, @theDate)
		, @iso_week						int		= DATEPART(ISO_WEEK, @theDate)
		, @iso_year						int

	SELECT @iso_year = 
		CASE 
			WHEN @iso_week > 50 and @date_month = 1
			THEN @date_year - 1
			WHEN @iso_week = 1 AND @date_month = 12
			THEN @date_year + 1
			ELSE @date_year
		END

	RETURN @iso_year
END
GO

CREATE OR ALTER FUNCTION [dbo].[ISO_week] (@theDate date)
RETURNS char(9)
AS
BEGIN
	/*
		From Wikipedia:
		https://en.wikipedia.org/wiki/ISO_week_date

		ISO Week
		First Week:		
			- the first week of the year with the majority (4 or more) of its days in January
			- Its first day is the monday nearest to Jan 1
			- It has Jan 4 in it

		Last Week:
			- it has the year's last thursday in it
			- it is the last week with the majority of its days (4 or more) in December
			- its middle day, Thursday, falls in the ending year
			- its last day is the Sunday nearest to Dec 31
			- it has Dec 28 in it

	*/
	DECLARE 
		  @date_year		int		= DATEPART(year, @theDate)
		, @iso_week			int		= DATEPART(ISO_WEEK, @theDate)
		, @iso_year			int		= [dbo].[ISO_year] (@theDate)
		, @return_value		char(9) 
		
	SELECT @return_value = CAST(@iso_year as varchar(10)) + '-w-' + RIGHT('00' + CAST(@iso_week as varchar(10)), 2)


	RETURN @return_value
END
GO



CREATE OR ALTER PROCEDURE [dbo].[prc_populate_calendar]
AS
BEGIN
	TRUNCATE TABLE dbo.calendar 


	DECLARE 
		  @theDate date = '1950-01-01'		-- first desired date of the calendar 
		, @finalDate date = '2040-12-31'	-- last desired date of the calendar 


	WHILE @theDate <= @finalDate
	BEGIN
		IF NOT EXISTS (
			SELECT TOP 1 1
			FROM dbo.calendar as c
			WHERE c.[date] = @theDate
		)
		BEGIN
			INSERT dbo.calendar (
				  [date]
				, date_id
				, [day]
				, [month]
				, [quarter]
				, [year]
				, week_day_name
				, week_day_abbr
				, month_name
				, month_name_abbr
				, month_start_date
				, month_end_date
				, week_start_date
				, week_end_date
				, week_of_quarter
				, week_of_year
				, [iso_year]	
				, [iso_week]	
				, [iso_week_number]
				, [is_first_day_of_month]
				, [is_last_day_of_month]
			)
			SELECT
				  @theDate																										as [date]
				, DATEPART(year, @theDate) * 10000 + DATEPART(month, @theDate) * 100 + DATEPART(day, @theDate)					as [date_id] 
				, DATEPART(day, @theDate)																						as [day]
				, DATEPART(month, @theDate)																						as [month]
				, DATEPART(quarter, @theDate)																					as [quarter]
				, DATEPART(year, @theDate)																						as [year]
				, DATENAME(weekday, @theDate)																					as [week day name]
				, LEFT(DATENAME(weekday, @theDate), 3)																			as [week day abbr]
				, DATENAME(month, @theDate)																						as [month name]
				, LEFT(DATENAME(month, @theDate), 3)																			as [month abbr]
				, DATEFROMPARTS(datepart(year, @theDate), datepart(month, @theDate), 1)											as [month start date]
				, DATEADD(day, -1, DATEADD(month, 1, DATEFROMPARTS(datepart(year, @theDate), datepart(month, @theDate), 1)))	as [month end date]
				, DATEADD(day, 1 - DATEPART(weekday, @theDate), @theDate)														as [week start date]
				, DATEADD(day, 7 - DATEPART(weekday, @theDate), @theDate)														as [week end date]
				, DATEPART(week, DATEADD(month, - (DATEPART(q, @theDate) - 1) * 3, @theDate))									as [week of quarter]
				, DATEPART(week, @theDate)																						as [week of year]
				, dbo.[ISO_year](@theDate)
				, dbo.[ISO_week](@theDate)
				, CAST(RIGHT(dbo.[ISO_week](@theDate), 2) as int)
				, CASE WHEN DATEPART(day, @theDate)	= 1 THEN 1 ELSE 0 END														as [is_first_day_of_month]
				, CASE WHEN DATEPART(day, DATEADD(day, 1, @theDate))	= 1 THEN 1 ELSE 0 END									as [is_last_day_of_month]
			--)
		END


		SELECT 
			  @theDate = dateadd(day, 1, @theDate)
	END


	;WITH iso as (
		select 
			  c.[date] as iso_week_start_date
			, dateadd(day, 6, c.[date]) as iso_week_end_date
		from dbo.calendar c 
		where c.week_day_abbr = 'MON'
	)
	UPDATE c
	SET 
		  iso_week_start_date = iso.iso_week_start_date
		, iso_week_end_date = iso.iso_week_end_date
		, build_week = RIGHT(CAST([iso_year] as varchar(10)), 2) + RIGHT ([iso_week], 2)
	FROM dbo.calendar as c
	INNER JOIN iso
		ON c.[date] BETWEEN iso.iso_week_start_date AND iso.iso_week_end_date


	UPDATE c
	SET [is_last_week_of_quarter] = 1
	FROM dbo.calendar as c
	INNER JOIN (
		select distinct 
			  [year]
			, [quarter]
			, [date] as last_date_of_quarter
			-- to make sure that the last week of the quarter gets seven days
			, DATEADD(day, -6, [date]) as last_week_of_quarter_start_date 
			, ROW_NUMBER() OVER (PARTITION BY [year], [quarter] ORDER BY [date] DESC) as rn -- to get last date of quarter
		from dbo.calendar as c
	) as lwq
		ON c.[year]					= lwq.[year]
		AND c.[quarter]				= lwq.[quarter]
		AND c.[date] BETWEEN lwq.last_week_of_quarter_start_date AND lwq.last_date_of_quarter
	WHERE lwq.rn = 1


	UPDATE c
	SET 
		  quarter_start_date = DATEFROMPARTS([year], ([quarter] - 1) * 3 + 1, 1)
		, quarter_end_date = DATEADD(day, -1, DATEADD(month, 1, DATEFROMPARTS([year], ([quarter]) * 3, 1)))
	FROM dbo.calendar as c


END

GO

EXEC [dbo].[prc_populate_calendar]


select * from dbo.calendar
