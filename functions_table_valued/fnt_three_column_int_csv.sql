CREATE OR ALTER FUNCTION [dbo].[fnt_three_column_int_csv]
(
	@string VARCHAR(MAX),
	@column_delimiter CHAR(1),
	@row_delimiter CHAR(1)
)
RETURNS @output TABLE
(
	  [value1] int
	, [value2] int
	, [value3] int
)
BEGIN
	DECLARE
		  @start int
		, @end int
		, @value1 int
		, @value2 int
		, @value3 int
		, @strRowValue varchar(max)
		, @lenStrRowValue int
		, @strValue1 varchar(max)
		, @strValue2 varchar(max)
		, @strValue3 varchar(max)
		, @like_column_delimiter char(3)
		, @column_spot_1 int
		, @column_spot_2 int
		, @row_spot int;

/*
	the csv needs to look like this:
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



	SELECT
		  @start = 1
		, @end = CHARINDEX(@row_delimiter, @string)
		, @like_column_delimiter = '%' + @column_delimiter + '%';

	WHILE @start < LEN(@string) + 1
	BEGIN
		IF @end = 0
		BEGIN
			SET @end = LEN(@string) + 1;
		END

		SELECT @strRowValue =
			CASE
				WHEN @end > @start THEN SUBSTRING(@string, @start, @end - @start)
				ELSE ''
			END;

		-- @strValue is now ONE ROW
		-- it now needs to be split into columns

		IF @strRowValue LIKE @like_column_delimiter
		BEGIN
			SELECT
				  @column_spot_1 = CHARINDEX(@column_delimiter, @strRowValue)
				, @column_spot_2 = CHARINDEX(@column_delimiter, @strRowValue, @column_spot_1 + 1)
				, @strValue1 =
					CASE
						WHEN @column_spot_1 > 0 AND @column_spot_1 < @column_spot_2
						THEN LEFT(@strRowValue, @column_spot_1 - 1)
						ELSE ''
					END
				, @strValue2 =
					CASE
						WHEN @column_spot_1 > 0 AND @column_spot_1 < @column_spot_2
						THEN SUBSTRING(@strRowValue, @column_spot_1 + 1, @column_spot_2 - @column_spot_1 - 1)
						ELSE ''
					END

				, @strValue3 =
					CASE
						WHEN @column_spot_1 > 0 AND @column_spot_1 < @column_spot_2 AND @column_spot_2 < LEN(@strRowValue)
						THEN SUBSTRING(@strRowValue, @column_spot_2 + 1, LEN(@strRowValue))
						ELSE ''
					END

				, @strValue1 = REPLACE(@strValue1, ',', 'x')
				, @strValue2 = REPLACE(@strValue2, ',', 'x')
				, @strValue3 = REPLACE(@strValue3, ',', 'x');


			IF ISNUMERIC(@strValue1) = 1
			AND ISNUMERIC(@strValue2) = 1
			AND ISNUMERIC(@strValue3) = 1
			BEGIN
				SELECT
					  @value1 = CONVERT(int, CONVERT(float, @strValue1))
					, @value2 = CONVERT(int, CONVERT(float, @strValue2))
					, @value3 = CONVERT(int, CONVERT(float, @strValue3));
					
				INSERT INTO @output ([value1], [value2], [value3])
				VALUES(@value1, @value2, @value3);
			END
		END

		SELECT
			  @start = @end + 1
			, @end = CHARINDEX(@row_delimiter, @string, @start);
	END

RETURN;
END
GO


