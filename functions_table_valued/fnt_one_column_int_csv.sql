CREATE OR ALTER FUNCTION [dbo].[fnt_one_column_int_csv]
(@string VARCHAR (MAX), @delimiter CHAR (1))
RETURNS 
    @output TABLE (
        [value] INT NULL, ord int identity(1,1))
AS
BEGIN

    DECLARE
          @start int
        , @end int
        , @value int
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
            SELECT @value = CONVERT(int, @strValue);
            
            INSERT INTO @output ([value])
            VALUES(@value);
        END

        SELECT
             @start = @end + 1
            , @end = CHARINDEX(@delimiter, @string, @start);
    END

    RETURN

END


