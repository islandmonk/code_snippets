create table note (
	note_id int identity(1,1) primary key
	, note nvarchar(max) NOT NULL
	, created datetime not null default(getdate())
	, modified datetime not null default(getdate())
)
GO

CREATE TABLE tag (
	  tag_id int identity(1,1) primary key
	, tag nvarchar(max) NOT NULL
	, created datetime not null default(getdate())
)

create table note_tag (
	  note_id int NOT NULL
	, tag_id int not null
	, created datetime not null default(getdate())
	, primary key (note_id, tag_id)
)

CREATE TABLE document (
	  document_id int identity(1,1) primary key
	, document_name nvarchar(max) NOT NULL
	, document_path nvarchar(max) NOT NULL
	, created datetime not null default(getdate())
)

create table note_document (
	  note_id int NOT NULL
	, document_id int not null
	, created datetime not null default(getdate())
	, primary key (note_id, document_id)
)