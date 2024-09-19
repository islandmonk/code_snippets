CREATE OR ALTER FUNCTION [dbo].[fnt_dependent_objects]
(
	@string NVARCHAR(MAX)
)
RETURNS @output TABLE
(
	  [object_name] nvarchar(1000)
)
BEGIN
	INSERT @output
	SELECT DISTINCT domain.fns_two_part_object_name(id)
	FROM sys.syscomments
	WHERE [text] LIKE '%' + @string + '%';
RETURN;
END
GO


