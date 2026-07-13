CREATE OR ALTER FUNCTION [dbo].[fnt_three_column_int_csv]
(
	@string VARCHAR(MAX),
	@column_delimiter CHAR(1),
	@row_delimiter CHAR(1)
)
RETURNS @output TABLE
(
	  [ord] int NOT NULL PRIMARY KEY
	, [value1] int NULL
	, [value2] int NULL
	, [value3] int NULL
)
BEGIN
    DECLARE @json_values nvarchar(max)
    
	SELECT @json_values = '["' + REPLACE(@string, @row_delimiter, '","') + '"]' 

/*
	IMPORTANT:
	This function has dependencies on: 
		[dbo].[fnt_one_column_varchar_csv]
		and
		[dbo].[fnt_one_column_int_csv]


	the csv *needs* to look like this:
	456,345;12,3456;9878,234
	in the above example, the row delimiter is the semi-colon
	and the column delimiter is the comma.

	The selection of delimiters is arbitrary--they just cannot
	be the same thing.

	The above example would be executed like this:

	SELECT value1, value2
	FROM dbo.fnt_two_column_int_csv ('456,345;12,3456;9878,234', ',', ';')

	SELECT value1, value2, value3
	FROM dbo.fnt_three_column_int_csv ('456,0,1;1222,1,0;9878,0,1;', ',', ';')

	get one row at a time. Each row will look like this:
	456,345
	12,3456
	9878,234

	test cases for a row with a non numeric value, empty values,
	missing column delimiter, extra column delimiters, float value:

	SELECT value1, value2
	FROM dbo.fnt_two_column_int_csv ('456,345;12,3456;aa,0;9878,234', ',', ';')

	SELECT value1, value2
	FROM dbo.fnt_two_column_int_csv ('456,345;12,3456;,;9878,234', ',', ';')

	SELECT value1, value2
	FROM dbo.fnt_two_column_int_csv ('456,345,4565;12,3456;aa,0;,;9878,234', ',', ';')

	SELECT value1, value2
	FROM dbo.fnt_two_column_int_csv ('456,345.567;12,3456;aa,0;,;9878,234', ',', ';')

	scenario:
	You want to turn things on and off in batches. To do this, your client
	will construct a two column csv where col 1 is the id of the thing
	to change and column 2 is the bolean value that the thing will
	be updated to. Your client will send the csv to the sproc. The sproc
	will make a call to this table valued function. The result of the
	table valued function will be used to make the changes to the desired
	table in a set-based fashion.

	DECLARE @csv varchar(max) = '456,1;12,1;9878,0';

	-- inside your consuming sproc:

	DECLARE @tmp TABLE (am_id int, status bit)

	INSERT @tmp (am_id, status)
	SELECT value1, CONVERT(bit, value2)
	FROM dbo.fnt_two_column_int_csv(@csv, ',', ';');

	SELECT * FROM @tmp

	UPDATE a
	SET active_status = t.status
	FROM dbo.somethingObject as a
	INNER JOIN @tmp as t
	ON a.am_id = t.am_id

	-- end of consuming sproc
*/

	-- split it into rows:
	DECLARE @rows TABLE (
		  [theRow] nvarchar(max) NULL
		, [ord] int not null
	)

	INSERT @rows ([theRow], [ord])
	SELECT [value], [ord]
	FROM [dbo].[fnt_one_column_varchar_csv](@string, ';')

	-- split the rows into columns
	;WITH cols as (
		SELECT 
			  r.[ord] as row_ord
			, c.[value]
			, c.[ord] as column_ord
		FROM @rows as r
		CROSS APPLY (
			SELECT x.[value], x.[ord]
			FROM dbo.fnt_one_column_int_csv(r.[theRow], @column_delimiter) as x
		) as c
	)
	INSERT @output ([value1], [value2], [value3], [ord])
	SELECT [1], [2], [3], [row_ord]
	FROM (
		SELECT 
			  [row_ord]
			, [value]
			, [column_ord]
		FROM cols
	) as c
	PIVOT (
		MAX([value])  
		FOR column_ord in ([1], [2], [3])
	) as pt;

	RETURN;
END
GO

