CREATE DATABASE deptest

USE deptest

CREATE TABLE person (person_id int identity(1,1) primary key, person_name nvarchar(250) NOT NULL)

CREATE TABLE house (
	  house_id int identity(1,1) primary key
	, street_address nvarchar(1000)
	, city nvarchar(1000)
	, state nvarchar(1000)
	, zip nvarchar(250)
)
GO

CREATE TABLE house_person (
	  house_id int NOT NULL
	, person_id int NOT NULL
	, moved_in datetime NOT NULL default (getdate())
	, moved_out datetime NULL
)
GO

CREATE OR ALTER VIEW dbo.v_person_house 
AS
SELECT 
	  p.person_id
	, p.person_name
	, h.street_address
	, h.city
	, h.state
	, h.zip
FROM dbo.person as p
INNER JOIN dbo.house_person as hp
	ON p.person_id = hp.person_id
INNER JOIN house as h  -- schema NOT explicitly stated
	ON hp.house_id = h.house_id
GO

SELECT 
	  SCHEMA_NAME(o.schema_id) + '.' + o.name as referencing_object
	--, d.referencing_class_desc
	, d.referenced_schema_name
	, d.referenced_entity_name
	, SCHEMA_NAME(ro.schema_id) + '.' + ro.name as referenced_object
	--, d.referenced_class_desc
FROM sys.sql_expression_dependencies as d
INNER JOIN sys.objects as o
	ON d.referencing_id = o.object_id
LEFT OUTER JOIN sys.objects as ro
	ON d.referenced_id = ro.object_id
ORDER BY 1, 4
GO
/*

referencing_object referenced_schema_name referenced_entity_name referenced_object
------------------ ---------------------- ---------------------- -----------------
dbo.v_person_house NULL                   house                  dbo.house
dbo.v_person_house dbo                    house_person           dbo.house_person
dbo.v_person_house dbo                    person                 dbo.person


*/
CREATE OR ALTER VIEW dbo.v_person_house 
AS
SELECT 
	  p.person_id
	, p.person_name
	, h.street_address
	, h.city
	, h.state
	, h.zip
FROM dbo.person as p
INNER JOIN (
	SELECT *
	FROM dbo.house_person
) as hp
	ON p.person_id = hp.person_id
INNER JOIN dbo.house as h  -- schema explicitly stated
	ON hp.house_id = h.house_id
GO

/*
-- same query as above now shows a non-NULL schema_name for the first row

referencing_object referenced_schema_name referenced_entity_name referenced_object
------------------ ---------------------- ---------------------- -----------------
dbo.v_person_house dbo                    house                  dbo.house
dbo.v_person_house dbo                    house_person           dbo.house_person
dbo.v_person_house dbo                    person                 dbo.person

*/

DROP TABLE dbo.house;

/*
-- referenced_schema_name & referenced_entity_name are still parsed out of the person-house view
-- as expected. But the actual house object is gone so it shows up as null here.
-- this is now a broken reference.

referencing_object referenced_schema_name referenced_entity_name referenced_object
------------------ ---------------------- ---------------------- -----------------
dbo.v_person_house dbo                    house                  NULL
dbo.v_person_house dbo                    house_person           dbo.house_person
dbo.v_person_house dbo                    person                 dbo.person

*/

-- DROP DATABASE deptest


