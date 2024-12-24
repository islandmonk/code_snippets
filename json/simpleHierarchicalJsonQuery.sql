DECLARE @theJson varchar(max) = '
[
    {
        "owner": "doug",
        "cats": [{"cat":"tom"}, {"cat":"strawberry"}, {"cat":"mooney"}]
    },
    {
        "owner": "beth",
        "cats": [{"cat":"roy"}, {"cat":"ella"}]
    }
]';


SELECT 
    ownerData.owner AS Owner,
    catData.cat AS CatName
FROM OPENJSON(@theJson) 
WITH (
    owner NVARCHAR(50) '$.owner',
    cats NVARCHAR(MAX) '$.cats' AS JSON
) AS ownerData
CROSS APPLY OPENJSON(ownerData.cats) 
WITH (
    cat NVARCHAR(50) '$.cat'
) AS catData;