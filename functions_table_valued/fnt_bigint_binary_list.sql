
ALTER FUNCTION [dbo].[fnt_bigint_binary_list] (@binaryValue varbinary(max))
RETURNS @IDs TABLE (
	  bigint_value bigint NOT NULL  -- this is the only field that should matter
	, binary_value varbinary(8)
	, startSpot int
	, endSpot int
	, ord int identity(1,1) PRIMARY KEY
)
AS
BEGIN
	DECLARE
		  @endSpot int = LEN(@binaryValue)
		, @thisValue varbinary(8)
		, @thisValue_bigint bigint
		, @startSpot int
		, @length int = 8

	-- big integers are 8 bytes, 16 hex characters
	-- start from the right of the binary and work left 8 bytes at a time.
	-- the last number in the array might not be a full 16 char hex.
	-- Thus the varbinary(8) column instead of a binary(8).
	-- return distinct non-NULL values only

	-- example call:
	-- SELECT * FROM [dbo].[fnt_bigint_binary_list](0xACED0005757200025B4A782004B512B175930200007870000000010000000000003105)

	WHILE @endSpot > 0
	BEGIN
		SELECT @startSpot = @endSpot - @length + 1;

		IF @startSpot < 0
		BEGIN
			SELECT @startSpot = 1, @length = @endSpot - @startSpot + 1
		END

		SELECT
			  @thisValue = SUBSTRING(@binaryValue, @startSpot, @length)
			, @thisValue_bigint = TRY_CAST(@thisValue as varbinary(8))

		INSERT @IDs (bigint_value, binary_value, startSpot, endSpot)
		SELECT @thisValue_bigint, @thisValue, @startSpot, @endSpot
		WHERE @thisValue IS NOT NULL
		AND NOT EXISTS (
			SELECT TOP 1 1
			FROM @IDs as x
			WHERE @thisValue_bigint = x.bigint_value
		)

		SELECT @endSpot = @startSpot - 1
	END

	RETURN
END
GO

