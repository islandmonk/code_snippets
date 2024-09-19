CREATE OR ALTER FUNCTION [dbo].[fnt_two_column_int_csv]
(
	@string NVARCHAR(MAX),
	@column_delimiter CHAR(1),
	@row_delimiter CHAR(1)
)
RETURNS @output TABLE
(
	  [value1] BIGINT NULL
	, [value2] BIGINT NULL
	, [ord] INT IDENTITY(1,1) PRIMARY KEY
)
BEGIN
	DECLARE
		  @start INT
		, @end INT
		, @strRowValue VARCHAR(MAX)
		, @lenStrRowValue INT
		, @strValue1 VARCHAR(MAX)
		, @strValue2 VARCHAR(MAX)
		, @intValue1 BIGINT
		, @intValue2 BIGINT
		, @like_column_delimiter CHAR(3)
		, @column_spot_1 INT
		, @row_spot INT;

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
				, @strValue1 =
					CASE
						WHEN @column_spot_1 > 0 
						THEN LEFT(@strRowValue, @column_spot_1 - 1)
						ELSE ''
					END
				, @strValue2 =
					CASE
						WHEN @column_spot_1 > 0 
						THEN SUBSTRING(@strRowValue, @column_spot_1 + 1, LEN(@strRowValue))
						ELSE ''
					END
		END
		
		ELSE 
		BEGIN
			SELECT 
				  @strValue1 = @strRowValue
				, @strValue2 = NULL;
		END
		
		SELECT 
			  @intValue1 = CASE WHEN ISNUMERIC(@strValue1) = 1 THEN @strValue1 ELSE NULL END
			, @intValue2 = CASE WHEN ISNUMERIC(@strValue2) = 1 THEN @strValue2 ELSE NULL END
			

		INSERT INTO @output ([value1], [value2])
		VALUES(@intValue1, @intValue2);

		SELECT
			  @start = @end + 1
			, @end = CHARINDEX(@row_delimiter, @string, @start);
	END

RETURN;
END

GO


