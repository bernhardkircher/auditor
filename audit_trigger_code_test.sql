USE [auditlog]
GO
/****** Object:  Trigger [dbo].[Test_ChangeTracking]    Script Date: 14.03.2015 16:18:52 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER trigger [dbo].[Test_ChangeTracking] on [dbo].[test] for insert, update, delete
as
 
declare @bit int ,
@field int ,
@maxfield int ,
@char int ,
@fieldname varchar(128) ,
@TableName varchar(128) ,
@PKCols varchar(1000) ,
@sql varchar(2000),
@UpdateDate datetime2 ,
@UserName varchar(128) ,
@Type char(1) ,
@PKFieldSelect varchar(1000),
@PKValueSelect varchar(1000),
@txId bigint
select @TableName = 'Test'
-- date and user
select @UserName = system_user
 
-- Action
if exists (select * from inserted)
if exists (select * from deleted)
select @Type = 'U'
else
select @Type = 'I'
else
select @Type = 'D'
 
-- get list of columns
select * into #ins from inserted
select * into #del from deleted

-- TODO generate this metadata/sql statically when creating the trigger (only once)
 
-- Get primary key columns for full outer join
select @PKCols = coalesce(@PKCols + ' and', ' on') + ' i.' + c.COLUMN_NAME + ' = d.' + c.COLUMN_NAME
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
where pk.TABLE_NAME = @TableName
and CONSTRAINT_TYPE = 'PRIMARY KEY'
and c.TABLE_NAME = pk.TABLE_NAME
and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
 
-- Get primary key fields select for insert
select @PKFieldSelect = coalesce(@PKFieldSelect+'+','') + '''' + COLUMN_NAME + ''''
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
where pk.TABLE_NAME = @TableName
and CONSTRAINT_TYPE = 'PRIMARY KEY'
and c.TABLE_NAME = pk.TABLE_NAME
and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
 
select @PKValueSelect = coalesce(@PKValueSelect+'+','') + 'convert(varchar(100), coalesce(i.' + COLUMN_NAME + ',d.' + COLUMN_NAME + '))'
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
where pk.TABLE_NAME = @TableName
and CONSTRAINT_TYPE = 'PRIMARY KEY'
and c.TABLE_NAME = pk.TABLE_NAME
and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
 
if @PKCols is null
begin
raiserror('no PK on table %s', 16, -1, @TableName)
return
end


-- TODO first insert into transaction table, which holds contextinfo, db transaction and sqluser etc info
-- becuase the auditlog.auditData holds modifications for each column
declare @IDENTITY_SAVE              varchar(50),
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

-- TODO primary key on bulk updates?

-- TODO depends on type, we also need to chec kif it ewas a delete
    declare @id bigint 
	if @ChangeType = 'D' begin
		select @id = deleted.id from deleted
	end else begin
	select @id = inserted.id from inserted
	end

INSERT
INTO auditlog.auditlogtransaction
(
	databasename,
    TableName,
    TableSchemaName,
	DatabaseTransactionId,
    [ChangeType],
    HostName,
    AppName,
    SqlUserName,
	ModificationDateUtc,
    AffectedRows,
	RawContextInfo,
	PrimaryKeyField,
	PrimaryKeyValue
)
values(
    DB_NAME(),
    '[Test]',
    '[dbo]',
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
	GETUTCDATE(),
    @ROWS_COUNT,
    context_info(),
	@PKFieldSelect,
	@id
)

select @txId = scope_identity()
-- TODO use parametrized command!
 
select @field = 0, @maxfield = max(ORDINAL_POSITION) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = @TableName
while @field < @maxfield
begin
select @field = min(ORDINAL_POSITION) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = @TableName and ORDINAL_POSITION > @field
select @bit = (@field - 1 )% 8 + 1
select @bit = power(2,@bit - 1)
select @char = ((@field - 1) / 8) + 1
if substring(COLUMNS_UPDATED(),@char, 1) & @bit > 0 or @Type in ('I','D')
begin
select @fieldname = COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = @TableName and ORDINAL_POSITION = @field
select @sql = 'insert into auditlog.AuditData (FieldName, OldValue, NewValue, TransactionId)'
select @sql = @sql + ' select '
select @sql = @sql + '''' + @fieldname + ''''
select @sql = @sql + ',convert(nvarchar(1000),d.' + @fieldname + ')'
select @sql = @sql + ',convert(nvarchar(1000),i.' + @fieldname + ')'
select @sql = @sql + ', '+ cast(@txid as varchar(max)) +' '
select @sql = @sql + ' from #ins i full outer join #del d'
select @sql = @sql + @PKCols
select @sql = @sql + ' where i.' + @fieldname + ' <> d.' + @fieldname
select @sql = @sql + ' or (i.' + @fieldname + ' is null and d.' + @fieldname + ' is not null)'
select @sql = @sql + ' or (i.' + @fieldname + ' is not null and d.' + @fieldname + ' is null)'

print @sql

exec (@sql)
end
end



