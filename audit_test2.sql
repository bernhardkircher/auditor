/*http://autoaudit.codeplex.com/
http://stackoverflow.com/questions/349524/sql-server-history-table-populate-through-sp-or-trigger
http://doddleaudit.codeplex.com/
http://weblogs.asp.net/jongalloway/adding-simple-trigger-based-auditing-to-your-sql-server-database


http://ajitananthram.wordpress.com/2012/05/26/auditing-external-activator/
http://solutioncenter.apexsql.com/methods-for-auditing-sql-server-data-changes-part-9-the-apexsql-solution/
http://www.sqlserveraudit.org/
http://msdn.microsoft.com/en-us/library/dd392015%28v=sql.100%29.aspx
http://msdn.microsoft.com/en-us/library/cc280386%28SQL.100%29.aspx
https://www.simple-talk.com/sql/database-administration/pop-rivetts-sql-server-faq-no.5-pop-on-the-audit-trail/
*/

create database auditlog
go
use auditlog
go

-- could use http://msdn.microsoft.com/en-us/library/bb522489.aspx CDC/ChangeTracking for capturing changes instead of triggers
-- but this requires special mssql versions etc nad administrative tasks.

-- TODO create metadata table that holds some info, like context mappings (group tables to contexts, e.g. patient, etc.)

create schema auditlog;

create table auditlog.TableContext (
	Id int Identity not null primary key,
	ContextName nvarchar(128) not null,	
)

-- todo make contextname unique index

create table auditlog.Metadata (
	Id int Identity not null primary key,
	DatabaseName nvarchar(128) not null,
	TableName nvarchar(261) not null,
	SchemaName nvarchar(261) not null,
	-- the column that holds the fk to the context, e.g. a patient_id
	-- TODO this requires that each table has a context column!!!! (fk.)
	ContextColumnName nvarchar(256) not null,
)

-- TODO unique constrainton table + schemaname
-- TODO also support changing tables/metadata -> make history entry etc..

-- use ddl triggers?? when schema changes?

 
 -- TODO use SLOTS!!!! configure to make a new slot all xxx months, do this in 
 -- TODO add views, functions to get changes!!!!
 
 
 -- Table 1 – holds transaction details (who, when, application, host name, etc)
 CREATE TABLE [auditlog].[AuditLogTransaction](
    [TransactionId] [bigint] IDENTITY(1,1) NOT NULL,
	-- this is resetted on each server restart!!!
    DatabaseTranactionId bigint not null,	--sys.dm_tran_current_transaction.transaction_id
    [DatabaseName] [nvarchar](128) NOT NULL,
    [TableName] [nvarchar](261) NOT NULL,
    [TableSchemaName] [nvarchar](261) NOT NULL,
	-- TODO add usecase
    [RawContextInfo] varbinary(128) null,
	[ChangeType] char(1) not null, -- I;u,D
    [HostName] [nvarchar](128) NOT NULL,
    [AppName] [nvarchar](128) NOT NULL,
    [SqlUserName] [nvarchar](128) NOT NULL,
	[ApplicationContextUserName] [nvarchar](128) NOT NULL,
	-- what was the app service / wcf usecase???
	[ApplicationContextAction] [nvarchar](128) NOT NULL,
    [ModificationDateUtc] [datetime2] NOT NULL,
    [AffectedRows] [int] NOT NULL,
    [SysObjectId]  AS (object_id([TableName])),
  PRIMARY KEY CLUSTERED 
  (
       [TransactionId] ASC
  )
)

 CREATE TABLE [auditlog].[AuditLogEntry](
   [Id] [bigint] IDENTITY(1,1) NOT NULL,
   [TransactionId] [bigint] NOT NULL,
   [PrimaryKeyData] [nvarchar](1500) NOT NULL,
   [Columnname] [nvarchar](128) NOT NULL,
   [OldValueLong] [ntext] NULL,
   [NewValueLong] [ntext] NULL,
   [NewValueBlob] [varbinary](max) NULL,
   [NewValue]  AS (isnull(CONVERT([varchar](8000),      [NewValueLong],0),CONVERT([varchar](8000),CONVERT([varbinary](8000),substring([NewValueBlob],(1),(8000)),0),0))),
   [OldValue]  AS (CONVERT([varchar](8000),[OldValueLong],0)),
   [PrimaryKey]  AS ([PrimaryKeyData]),
   [Type] [char](1) NOT NULL,
   [Key1] [nvarchar](500) NULL,
   [Key2] [nvarchar](500) NULL,
   [Key3] [nvarchar](500) NULL,
   [Key4] [nvarchar](500) NULL,
PRIMARY KEY CLUSTERED 
 (
    [Id] ASC
)
)








CREATE TRIGGER [dbo].[tr_i_AUDIT_Audited_Table]
ON [dbo].[Audited_Table]
FOR INSERT
NOT FOR REPLICATION
As
BEGIN
DECLARE 
    @IDENTITY_SAVE              varchar(50),
    @AUDIT_LOG_TRANSACTION_ID       Int,
    @PRIM_KEY               nvarchar(4000),
    @ROWS_COUNT             int

SET NOCOUNT ON
Select @ROWS_COUNT=count(*) from inserted
Set @IDENTITY_SAVE = CAST(IsNull(@@identity,1) AS varchar(50))


declare @ChangeType char(1)

if exists (select * from inserted)
if exists (select * from deleted)
select @ChangeType = 'U'
else
select @ChangeType = 'I'
else
select @ChangeType = 'D'

declare @dbTransactionId bigint
select @dbTransactionId = transaction_id from sys.dm_tran_current_transaction



/*
declare @usecase nvarchar(256) = 'test app method'
declare @username nvarchar(256) = 'bernhard.kircher@world-direct.at'
declare @contextstring nvarchar(max) = @usecase + '|' + @username
declare @contxtbinary varbinary(max) = cast(@contextstring as varbinary(max))
SET CONTEXT_INFO @contxtbinary
GO
*/

declare @contextstring nvarchar(max) = cast(CONTEXT_INFO() as nvarchar(max))
declare @usecase nvarchar(256) 
declare @username nvarchar(256)
if @contextstring is not null begin 
	declare @seperatorIndex int = charindex('|', @contextstring)
	if @seperatorIndex >= 0 begin
		set @usecase = SUBSTRING(@contextstring, 0, @seperatorIndex)
		set @username = SUBSTRING(@contextstring, @seperatorIndex + 1, LEN(@contextstring))
	end
end

--select @contextstring as contextstring, @usecase as usecase, @username as username

INSERT
INTO dbo.AUDIT_LOG_TRANSACTIONS
(
	
    TableName,
    TableSchemaName,
	DatabaseTranactionId,
    [ChangeType],
    HostName,
    AppName,
    SqlUserName,
	ApplicationContextUserName,
	ApplicationContextAction,
    ModificationDateUtc,
    AffectedRows,
	DatabaseName
)
values(
    'Audited_Table',
    'dbo',
	@dbTransactionId,
    @ChangeType, 
    CASE 
      WHEN LEN(HOST_NAME()) < 1 THEN ' '
      ELSE HOST_NAME()
    END,
    CASE 
      WHEN LEN(APP_NAME()) < 1 THEN ' '
      ELSE APP_NAME()
    END,
    SUSER_SNAME(),
	@username,
	@usecase,
    GETUTCDATE(),
    @ROWS_COUNT,
    db_name()
)

Set @AUDIT_LOG_TRANSACTION_ID = SCOPE_IDENTITY()    




select @field = 0, @maxfield = max(ORDINAL_POSITION) from sys.COLUMNS where TABLE_NAME = @TableName
while @field < @maxfield
begin
select @field = min(ORDINAL_POSITION) from sys.COLUMNS where TABLE_NAME = @TableName and ORDINAL_POSITION > @field
select @bit = (@field - 1 )% 8 + 1
select @bit = power(2,@bit - 1)
select @char = ((@field - 1) / 8) + 1
if substring(COLUMNS_UPDATED(),@char, 1) & @bit > 0 or @Type in (''I'',''D'')
begin
select @fieldname = COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = @TableName and ORDINAL_POSITION = @field


--This INSERT INTO code is repeated for each columns that is audited. 
--Below are examples for only two columns
/*INSERT INTO dbo.AUDIT_LOG_DATA
(
    AUDIT_LOG_TRANSACTION_ID,
    PRIMARY_KEY_DATA,
    COL_NAME,
    NEW_VALUE_LONG,
    DATA_TYPE
    , KEY1
)
SELECT
    @AUDIT_LOG_TRANSACTION_ID,
    convert(nvarchar(1500), IsNull('[PK_Column]='+CONVERT(nvarchar(4000), NEW.[PK_Column], 0), '[PK_Column] Is Null')),
    'Column1',
    CONVERT(nvarchar(4000), NEW.[Column1], 0),
    'A'
    , CONVERT(nvarchar(500), CONVERT(nvarchar(4000), NEW.[PK_Column], 0))
FROM inserted NEW
WHERE NEW.[Column1] Is Not Null

 --value is inserted for each column that is selected for auditin
INSERT INTO dbo.AUDIT_LOG_DATA
(
    AUDIT_LOG_TRANSACTION_ID,
    PRIMARY_KEY_DATA,
    COL_NAME,
    NEW_VALUE_LONG,
    DATA_TYPE
    , KEY1
)
SELECT
    @AUDIT_LOG_TRANSACTION_ID,
    convert(nvarchar(1500), IsNull('[PK_Column]='+CONVERT(nvarchar(4000), NEW.[PK_Column], 0), '[PK_Column] Is Null')),
    'Column2',
    CONVERT(nvarchar(4000), NEW.[Column2], 0),
    'A'
    , CONVERT(nvarchar(500), CONVERT(nvarchar(4000), NEW.[PK_Column], 0))
    FROM inserted NEW
    WHERE NEW.[Column2] Is Not Null
	*/
End




-----------------------------------------
 
 -- TODO also log app, host who changed data...
IF NOT EXISTS(SELECT * FROM sys.TABLES WHERE TABLE_NAME= 'AuditData')
CREATE TABLE auditlog.AuditData
(
AuditId [bigint]IDENTITY(1,1) NOT NULL,
[Type] char(1),
TableName nvarchar(128) not null,
PrimaryKeyField bigint not null,
PrimaryKeyValue bigint not null,
FieldName nvarchar(128) not null,
OldValue nvarchar(1000),
NewValue nvarchar(1000),
UpdateDate datetime2 DEFAULT (GetUtcDate()),
SQLUserName nvarchar(128) not null,
-- requires the application to set the currentuser via context()!!!
--ApplicationContextUserName nvarchar(128),
[RawContextInfo] varbinary(128) null,
TableContextId int not null,
)
GO
 
DECLARE @sql nvarchar(max), @TABLE_NAME sysname
SET NOCOUNT ON
 
SELECT @TABLE_NAME= MIN(TABLE_NAME)
FROM sys.Tables
WHERE
TABLE_TYPE= 'BASE TABLE'
AND TABLE_NAME!= 'sysdiagrams'
AND TABLE_NAME!= 'Audit'
 
WHILE @TABLE_NAME IS NOT NULL
BEGIN
EXEC('IF OBJECT_ID (''' + @TABLE_NAME+ '_ChangeTracking'', ''TR'') IS NOT NULL DROP TRIGGER ' + @TABLE_NAME+ '_ChangeTracking')
SELECT @sql =
'
create trigger ' + @TABLE_NAME+ '_ChangeTracking on ' + quotename(@TABLE_NAME)+ ' for insert, update, delete
as
 
declare @bit int ,
@field int ,
@maxfield int ,
@char int ,
@fieldname varchar(128) ,
@TableName varchar(128) ,
@PKCols varchar(1000) ,
@sql varchar(2000),
@UpdateDate varchar(21) ,
@UserName varchar(128) ,
@Type char(1) ,
@PKFieldSelect varchar(1000),
@PKValueSelect varchar(1000)
 
select @TableName = ''' + quotename(@TABLE_NAME)+ '''
 
-- date and user
select @UserName = system_user ,
@UpdateDate = convert(varchar(8), getdate(), 112) + '' '' + convert(varchar(12), getdate(), 114)
 
-- Action
if exists (select * from inserted)
if exists (select * from deleted)
select @Type = ''U''
else
select @Type = ''I''
else
select @Type = ''D''
 
-- get list of columns
select * into #ins from inserted
select * into #del from deleted
 
-- Get primary key columns for full outer join
select @PKCols = coalesce(@PKCols + '' and'', '' on'') + '' i.'' + c.COLUMN_NAME + '' = d.'' + c.COLUMN_NAME
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
where pk.TABLE_NAME = @TableName
and CONSTRAINT_TYPE = ''PRIMARY KEY''
and c.TABLE_NAME = pk.TABLE_NAME
and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
 
-- Get primary key fields select for insert
select @PKFieldSelect = coalesce(@PKFieldSelect+''+'','''') + '''''''' + COLUMN_NAME + ''''''''
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
where pk.TABLE_NAME = @TableName
and CONSTRAINT_TYPE = ''PRIMARY KEY''
and c.TABLE_NAME = pk.TABLE_NAME
and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
 
select @PKValueSelect = coalesce(@PKValueSelect+''+'','''') + ''convert(varchar(100), coalesce(i.'' + COLUMN_NAME + '',d.'' + COLUMN_NAME + ''))''
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
where pk.TABLE_NAME = @TableName
and CONSTRAINT_TYPE = ''PRIMARY KEY''
and c.TABLE_NAME = pk.TABLE_NAME
and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
 
if @PKCols is null
begin
raiserror(''no PK on table %s'', 16, -1, @TableName)
return
end
 
select @field = 0, @maxfield = max(ORDINAL_POSITION) from sys.COLUMNS where TABLE_NAME = @TableName
while @field < @maxfield
begin
select @field = min(ORDINAL_POSITION) from sys.COLUMNS where TABLE_NAME = @TableName and ORDINAL_POSITION > @field
select @bit = (@field - 1 )% 8 + 1
select @bit = power(2,@bit - 1)
select @char = ((@field - 1) / 8) + 1
if substring(COLUMNS_UPDATED(),@char, 1) & @bit > 0 or @Type in (''I'',''D'')
begin
select @fieldname = COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = @TableName and ORDINAL_POSITION = @field
select @sql = ''insert Audit (Type, TableName, PrimaryKeyField, PrimaryKeyValue, FieldName, OldValue, NewValue, UpdateDate, UserName)''
select @sql = @sql + '' select '''''' + @Type + ''''''''
select @sql = @sql + '','''''' + @TableName + ''''''''
select @sql = @sql + '','' + @PKFieldSelect
select @sql = @sql + '','' + @PKValueSelect
select @sql = @sql + '','''''' + @fieldname + ''''''''
select @sql = @sql + '',convert(nvarchar(1000),d.'' + @fieldname + '')''
select @sql = @sql + '',convert(nvarchar(1000),i.'' + @fieldname + '')''
select @sql = @sql + '','''''' + @UpdateDate + ''''''''
select @sql = @sql + '','''''' + @UserName + ''''''''
select @sql = @sql + '' from #ins i full outer join #del d''
select @sql = @sql + @PKCols
select @sql = @sql + '' where i.'' + @fieldname + '' <> d.'' + @fieldname
select @sql = @sql + '' or (i.'' + @fieldname + '' is null and d.'' + @fieldname + '' is not null)''
select @sql = @sql + '' or (i.'' + @fieldname + '' is not null and d.'' + @fieldname + '' is null)''
exec (@sql)
end
end
'
SELECT @sql
EXEC(@sql)
SELECT @TABLE_NAME= MIN(TABLE_NAME) FROM INFORMATION_SCHEMA.Tables
WHERE TABLE_NAME> @TABLE_NAME
AND TABLE_TYPE= 'BASE TABLE'
AND TABLE_NAME!= 'sysdiagrams'
AND TABLE_NAME!= 'Audit'
END
