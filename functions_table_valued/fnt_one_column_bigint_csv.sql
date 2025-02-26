CREATE OR ALTER FUNCTION [dbo].[fnt_one_column_int_csv] (
	  @string VARCHAR (MAX)
	, @delimiter CHAR (1)
)
RETURNS 
    @output TABLE (
          [value] BIGINT NULL
		, [ord] int identity(1,1)
	)
AS
BEGIN
	/*
		STRING_SPLIT() was introduced to SQL Server in version 2016

		For databases older than that, or in circumstances where the order of the items in the delimited string
		is important, this function is a good candidate replacement.

		Example:
		SELECT * FROM [dbo].[fnt_one_column_int_csv] ('154,256,25654,2654,65468498,5654989,65465498,65465498,55', ',')

		Scenario:
			I have two delimited lists. I would like to convert them to tuples
			List 1: 2,3,4,5,6
			List 2: 25,125,625,3125,15625

		To make a table with the values for one of the lists going into one column and values from the other:

		SELECT p.one, e.two, p.ord
		FROM (
			SELECT [value] as one, ord 
			FROM [dbo].[fnt_one_column_int_csv] ('2,3,4,5,6', ',') as x
		) as p
		INNER JOIN (
			SELECT [value] as two, ord 
			FROM [dbo].[fnt_one_column_int_csv] ('25,125,625,3125,15625', ',') as x
		) as e
			ON p.ord = e.ord
		ORDER BY p.ord

		Result:
			one  two    ord
			---- ------ --- 
			2    25     1
			3    125    2
			4    625    3
			5    3125   4
			6    15625  5

	*/
    DECLARE
          @start bigint
        , @end bigint
        , @value bigint
        , @strValue varchar(max);
        
    SELECT @start = 1, @end = CHARINDEX(@delimiter, @string)

    WHILE @start < LEN(@string) + 1
    BEGIN
        IF @end = 0
        BEGIN
            SET @end = LEN(@string) + 1;
        END
        
        SELECT @strValue = SUBSTRING(@string, @start, @end - @start);
        
        IF ISNUMERIC(@strValue) = 1
        BEGIN
            SELECT @value = CONVERT(bigint, @strValue);
            
            INSERT INTO @output ([value])
            VALUES(@value);
        END

        SELECT
             @start = @end + 1
            , @end = CHARINDEX(@delimiter, @string, @start);
    END

    RETURN

END


