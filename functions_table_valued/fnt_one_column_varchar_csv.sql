CREATE OR ALTER FUNCTION [dbo].[fnt_one_column_varchar_csv]
(@string VARCHAR (MAX), @delimiter CHAR (1))
RETURNS 
    @output TABLE (
          [value] VARCHAR (250) NULL
        , [ord] int NOT NULL
    )
AS
BEGIN
    -- when using this, make sure that the @json_values gen'd below results in 
    -- valid JSON. Sometimes the delimiter can cause issues. For instance, if the
    -- delimiter is a comma but some of the text values contain commas, you'll
    -- need to choose a different delimiter.

    -- example call:
    -- SELECT * FROM [dbo].[fnt_one_column_varchar_csv] ('abc,def,lmnop', ',')

    -- when we have commas in the values:
    -- SELECT * FROM [dbo].[fnt_one_column_varchar_csv] ('ab,c:d,ef:lmnop', ':')
    DECLARE @json_values nvarchar(max)
    
    IF @delimiter <> ','
    BEGIN
        -- if the delimiter is not a comma, it is possible the some of the values 
        -- have actual commas in them. Get those out of there in a safe way
        -- so they can be replaced later.
        SELECT @json_values = REPLACE(@string, ',', '{{comma}}')
    END
	SELECT @json_values = '["' + REPLACE(@string, @delimiter, '","') + '"]' 

	INSERT @output ([value], [ord])
	SELECT REPLACE(x.[value], '{{comma}}', ','), x.[key] + 1 as [ord]
	FROM OPENJSON(@json_values) as x

    RETURN
END


GO
