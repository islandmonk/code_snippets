CREATE OR ALTER FUNCTION [dbo].[fnt_two_column_int_csv]
(
	@string NVARCHAR(MAX),
	@column_delimiter CHAR(1),
	@row_delimiter CHAR(1)
)
RETURNS @output TABLE
(
	  [ord] int NOT NULL PRIMARY KEY
	, [value1] int NULL
	, [value2] int NULL
)
BEGIN
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

	SELECT *
	FROM dbo.fnt_two_column_int_csv ('456,345;12,3456;9878,234', ',', ';')

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
	INSERT @output ([value1], [value2], [ord])
	SELECT [1], [2], [row_ord]
	FROM (
		SELECT 
			  [row_ord]
			, [value]
			, [column_ord]
		FROM cols
	) as c
	PIVOT (
		MAX([value])  
		FOR column_ord in ([1], [2])
	) as pt

	RETURN;
END

GO


