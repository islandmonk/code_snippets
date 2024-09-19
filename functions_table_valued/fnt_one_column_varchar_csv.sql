CREATE OR ALTER FUNCTION [dbo].[fnt_one_column_varchar_csv]
(@string VARCHAR (MAX), @delimiter CHAR (1))
RETURNS 
    @output TABLE (
        [value] VARCHAR (250) NULL)
AS
BEGIN

    DECLARE
          @start int
        , @end int
        , @value varchar(250);
        
    SELECT @start = 1, @end = CHARINDEX(@delimiter, @string)

    WHILE @start < LEN(@string) + 1
    BEGIN
        IF @end = 0
        BEGIN
            SET @end = LEN(@string) + 1;
        END
        
        SELECT @value = SUBSTRING(@string, @start, @end - @start);
        
        IF LEN(@value) > 0
        BEGIN
            INSERT INTO @output ([value])
            VALUES(@value);
        END

        SELECT
             @start = @end + 1
            , @end = CHARINDEX(@delimiter, @string, @start);
    END

    RETURN

END


GO


