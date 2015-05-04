SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE SCHEMA auditor
go

/*
TODO's:
* DONE  [SysObjectId] should be objectid at time of change, not a calculated field
* DONE should be last trigger!
* DONE spaltennamen quoten bei generiertem sql!
* DONE eigene db für audits -> vorerst nicht, da leichter für backup, restore etc.?
* Index for views etc.
* Schemachanges -> columsn removewd/renamed etc keep a history and join on it?
** eigene sp's für rename etc scenarien bereitstellen, damit trigger, settings passen
* views generieren um history abzufragen?
** changes per headerId
** changes per row
* create udf to retrieve histories etc. and display what changed
** get history of a single row
** get complete history
** eigene sp/udf zum extrahieren von context?
** change per header/transaction (-batch)
** änderungen per user? -< schlechter index etc?
* Konzept für datenwartung?
* SP's -> parameterreiehenfolge immer gleich (1. schema, dann tablename etc)
* SP'S: formattierung, namenskonventionen
* untersdchied updated_Columsn und tatsächlicher änderung

INFO:
Limitations:
* does not track the following columns/changes:
-- 'text' 'ntext' 'image' 'timestamp' (due to limitations in the after triggers, and since we do not use those types it should not matter)
* audited tables mus not contain AuditId or AuditHeaderId columns
* may change @@identity if used by application/other sp's
* Schemachanges etc. are not supported, the user has to perform updates manually if th esource changed and has to recreate the triggers!!
* depending on the length of tablenames etc. there might be a problem (e.g. generated trigger names...) if they are too long. call me if this happens

CONVENTIONS:
* SP's start with sp_
* SP's that should not be used by the end user end with an underscore eg. sp_internal_

*/

/* create internal configuration tables */


-- simple key value settings table to store information that can easily be accessed in SP's etc.
create table auditor.Settings (
	SettingKey nvarchar(200) primary key not null,
	Value nvarchar(max) null,
	ValueDescription nvarchar(max) null
)

-- contains al auditted tables
create table auditor.Tables (
	TableObjectId int primary key not null,  -- the object id from sys.tables
	SchemaId int not null,
	TableName sysname not null,
	SchemaName sysname not null,
	AuditTableObjectId int not null,
	AuditTableObjectName sysname not null,
	AuditTableSchemaId int not null,
	AuditTableSchemaName sysname not null,
)

CREATE UNIQUE NONCLUSTERED INDEX
UI_SchemaId_TableObjectId ON auditor.Tables
(
SchemaId,TableObjectId 
) 
GO
CREATE UNIQUE NONCLUSTERED INDEX
UI_AuditTableSchemaId_AuditTableObjectId ON auditor.Tables
(
AuditTableSchemaId,AuditTableObjectId 
) 

-- stores the audited columns, used to check if columns changed etc.
-- for now, just keep info which tabes/columns are mapped (source - audit) so we can update in case of schema changes...
-- we'll use sys.dm views to compare audit/source talbes and columns etc to generate "diff" and update the schema accordingly.
create table auditor.Columns (
	TableObjectId int not null,
	ColumnId int not null,
	AuditTableObjectId int not null,
	AuditColumnId int not null -- the column id of the column in the audittable, so we can match source table/column to audittable column
)

-- contains DB information at a current time.
-- this table is used to store schema info like from sys.tables which can be used to track changes to the db schema and generate
-- and check if the audittables etc have to be regenerated.
/*create table auditor.TableHistory (
	HistoryId bigint identity primary key,

	-- select * from sys.tables
	name sysname not null,
	object_id int not null, 
	principal_id int null,
	schema_id int not null,
	parent_object_id int not null,
	type char(2) null,
	create_date datetime not null,
	modify_date datetime not null,
	lob_data_space_id int not null,
	filestream_data_space_id int null,
	max_column_id_used int not null,
	lock_on_bulk_load bit not null,
	uses_ansi_nulls bit null,
	is_replicated bit null,
	has_replication_filter bit null,
	is_merge_published bit null,
	is_sync_tran_subscribed bit null,
	has_unchecked_assembly_data bit not null,
	text_in_row_limit int null,
	large_value_types_out_of_row bit null,
	is_tracked_by_cdc bit null,
	lock_escalation tinyint null,
	lock_escalation_desc nvarchar(60) null,
	is_filetable bit null,
	is_memory_optimized bit null,
	durability tinyint null,
	durability_desc nvarchar(60) null
)


-- contains DB information at a current time.
-- this table is used to store schema info like from sys.tables which can be used to track changes to the db schema and generate
-- and check if the audittables etc have to be regenerated.
create table auditor.ColumnHistory (
	HistoryId bigint identity primary key,
	TableHistoryId bigint identity not null references auditor.TableHistory(HistoryId),
	-- select * from sys.columns
	name sysname not null,
	object_id int not null, 
	column_id int not null,
	system_type_id int not null,
	user_type_id int not null,
	max_length smallint not null,
	precision tinyint not null,
	scale tinyint not null,

)
TODO keep history of index info and constraint info?
*/

create table auditor.AuditHeader (
	Id bigint identity primary key,
	TransactionId bigint not null,  -- the database transaction id from sys.dm_tran_current_transaction 	
	[TransactionSequenceNumber] bigint not null,
    [TableName] sysname NOT NULL,
    [SchemaName] sysname NOT NULL,
	[RawContextInfo] varbinary(128) null,
	[ChangeType] char(1) not null check  ([ChangeType] in ('I', 'U', 'D')), 
    [Columns_Updated] varbinary null, -- change bitmask
	[HostName] [nvarchar](128) NOT NULL,
    [AppName] [nvarchar](128) NOT NULL,
    [SqlUserName] [nvarchar](128) NOT NULL,
	[AuditDateUtc] [datetime2] NOT NULL default(getutcdate()),
    [AffectedRows] [int] NOT NULL,
    [SysObjectId]  int not null
)

-- TODO custom sp to extract user info???


go


go
drop procedure [auditor].[sp_GenerateAuditViewFunction_]
go
CREATE PROCEDURE [auditor].[sp_GenerateAuditViewFunction_]
	@TableName sysname,
	@SchemaName sysname = 'dbo',
	@AuditTableSchemaName sysname,
	@AuditTableSuffix nvarchar(128) = '_Audit'
AS
BEGIN

	DECLARE @TableObjectId BIGINT
	
	SELECT @TableObjectId = Id FROM dbo.sysobjects WHERE id = object_id(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) and OBJECTPROPERTY(id, N'IsUserTable') = 1

	-- Check if table exists
	IF @TableObjectId IS NULL
	BEGIN
		PRINT 'ERROR: Table does not exist'
		-- TODO raise error
		RETURN
	END

	IF @AuditTableSuffix is null
	BEGIN
		PRINT 'ERROR: @AuditTableSuffix cannot be null'
		-- TODO raise error
		RETURN
	END

	-- Declare variable to build statements
	DECLARE @CreateStatement nvarchar(max)
	
	-- Check if audit table exists
	DECLARE @AuditTableObjectId BIGINT
	SELECT @AuditTableObjectId = Id FROM dbo.sysobjects WHERE id = object_id(QUOTENAME(@AuditTableSchemaName) + '.' + QUOTENAME(@SchemaName + '_' + @TableName + @AuditTableSuffix)) and OBJECTPROPERTY(id, N'IsUserTable') = 1
	IF @AuditTableObjectId IS NULL
	BEGIN
		print 'could not find audittable... TODO raise error'
		return
	END
		-- AuditTable does not exist, create new
		/* Schema auf each audit table:
- AuditId bigint identity -> incremenet, and uniquely identifies the audit row, used for versioning
- AuditHeaderId bigint not null references auditor.AuditHeader (id)
- 
*/

/*Select A.name IndexName,A.index_id,c.name ColumnName, 
            Case When System_type_id = User_Type_id Then Type_Name(System_type_id) Else Type_Name(User_Type_id) End Data_type 
            ,A.type_desc,is_identity,is_included_column,is_replicated 
            ,Is_Unique,Is_Primary_Key,IS_Unique_Constraint,Fill_factor,is_Padded,is_replicated,is_nullable,is_computed 
    From sys.indexes A  
    Inner Join sys.index_columns B On A.object_id = B.object_id And A.index_id = B.index_id 
    Inner Join sys.columns C On c.object_id = B.object_id  And C.column_id  = B.column_id 
    Where A.Object_ID = @TableObjectId
        order by A.Index_id,Is_included_Column,Key_ordinal asc 
		*/

-- get PKs and generate function to retrieve per row
		Select A.index_id,c.name ColumnName, 
            Case When System_type_id = User_Type_id Then Type_Name(System_type_id) Else Type_Name(User_Type_id) End Data_type              
    into #tmpPrimaryKeyColumns
	From sys.indexes A  
    Inner Join sys.index_columns B On A.object_id = B.object_id And A.index_id = B.index_id 
    Inner Join sys.columns C On c.object_id = B.object_id  And C.column_id  = B.column_id 
    Where A.Object_ID = @TableObjectId and Is_Primary_Key = 1
        order by A.Index_id,Is_included_Column,Key_ordinal asc 

	declare @PkFunctionParameters nvarchar(max)
	-- format: @Paramname1 paramtype1, @Paramname2 paramtype2,...
	select @PkFunctionParameters = COALESCE(@PkFunctionParameters + ',', '') + ('@' + pk.ColumnName + ' ' + pk.Data_type) from
	#tmpPrimaryKeyColumns pk

	declare @PkWhereClause nvarchar(max)
	-- format: x.PkColumn1 = @paramname1 and x.PkColumn2 = @paramname2 and... 
	select @PkWhereClause = COALESCE(@PkWhereClause + ' AND ', '') + ('x.' + QUOTENAME(pk.ColumnName) + ' = @' + pk.ColumnName) from
	#tmpPrimaryKeyColumns pk

	/*
	Template:
	CREATE FUNCTION auditor.dbo_test_rowhistory
(	
	-- Add the parameters for the function here
	@Id bigint
)
RETURNS TABLE 
AS
RETURN 
(
	select h.AuditDateUtc as HeaderAuditDateUtc, h.HostName as HeaderHostName, h.SqlUserName as HeaderSqlUsername,
	h.AppName as HeaderAppName, h.SchemaName as HeaderSchemaName, h.TableName as HeaderTableName, 
	h.TransactionId as HeaderTransactionId, h.TransactionSequenceNumber as HeaderTransactionSequenceNumber,
	h.RawContextInfo as HeaderRawContextInfo, h.ChangeType as HeaderchangeType, h.Columns_Updated as HeaderColumns_Updated,
	h.AffectedRows as HeaderAffectedRows,
	x.* from auditor.auditheader h
	inner join auditor.dbo_test_audit x on h.Id = x.AuditHeaderId
	where x.Id = @Id
	
)
GO
*/

	-- we try to generate a table inlined value function (= only 1 expression/statement)  since sql server can optimeze them/inline them
		-- Start of create table
		declare @FunctionName nvarchar(max)
		set @FunctionName = '[' + @AuditTableSchemaName + '].['  + @SchemaName + '_' + @TableName + @AuditTableSuffix + '_RowHistory]'

		SET @CreateStatement = '
		if exists(select * from sys.objects where name = '''+ (@SchemaName + '_' + @TableName + @AuditTableSuffix + '_RowHistory')+''' and schema_id = ' + cast(schema_id(@AuditTableSchemaName) as nvarchar(max)) +' AND type = ''IF'') BEGIN
			DROP FUNCTION ' + @FunctionName + '
		END
		GO

		CREATE FUNCTION '+@FunctionName+' (' + @PkFunctionParameters + ')
RETURNS Table 
AS
RETURN 
(
	select h.AuditDateUtc as HeaderAuditDateUtc, h.HostName as HeaderHostName, h.SqlUserName as HeaderSqlUsername,
	h.AppName as HeaderAppName, h.SchemaName as HeaderSchemaName, h.TableName as HeaderTableName, 
	h.TransactionId as HeaderTransactionId, h.TransactionSequenceNumber as HeaderTransactionSequenceNumber,
	h.RawContextInfo as HeaderRawContextInfo, h.ChangeType as HeaderchangeType, h.Columns_Updated as HeaderColumns_Updated,
	h.AffectedRows as HeaderAffectedRows,
	x.* from auditor.auditheader h
	inner join ' + QUOTENAME(@AuditTableSchemaName) + '.' + QUOTENAME(@SchemaName + '_' + @TableName + @AuditTableSuffix) + ' x on h.Id = x.AuditHeaderId
	where '+ @PkWhereClause +'
	
)
GO'	
		EXEC (@CreateStatement)

	
END
GO

drop procedure [auditor].[sp_GenerateAuditTable_]
go
CREATE PROCEDURE [auditor].[sp_GenerateAuditTable_]
	@TableName sysname,
	@SchemaName sysname = 'dbo',
	@AuditTableSchemaName sysname,
	@AuditTableSuffix nvarchar(128) = '_Audit'
AS
BEGIN

	DECLARE @TableObjectId BIGINT
	
	SELECT @TableObjectId = Id FROM dbo.sysobjects WHERE id = object_id(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) and OBJECTPROPERTY(id, N'IsUserTable') = 1

	-- Check if table exists
	IF @TableObjectId IS NULL
	BEGIN
		PRINT 'ERROR: Table does not exist'
		-- TODO raise error
		RETURN
	END

	IF @AuditTableSuffix is null
	BEGIN
		PRINT 'ERROR: @AuditTableSuffix cannot be null'
		-- TODO raise error
		RETURN
	END

	-- Declare cursor to loop over columns
	DECLARE TableColumns CURSOR Read_Only
	FOR SELECT b.name, c.name as TypeName, b.length, b.isnullable, b.collation, b.xprec, b.xscale
		FROM sysobjects a 
		inner join syscolumns b on a.id = b.id 
		inner join systypes c on b.xtype = c.xtype and c.name <> 'sysname' 
		WHERE a.id = @TableObjectId
		ORDER BY b.colId

	OPEN TableColumns


	-- Declare temp variable to fetch records into
	DECLARE @ColumnName nvarchar(128)
	DECLARE @ColumnType nvarchar(128)
	DECLARE @ColumnLength smallint
	DECLARE @ColumnNullable int
	DECLARE @ColumnCollation sysname
	DECLARE @ColumnPrecision tinyint
	DECLARE @ColumnScale tinyint

	-- Declare variable to build statements
	DECLARE @CreateStatement nvarchar(max)
	DECLARE @ListOfFields nvarchar(max)
	SET @ListOfFields = ''

	-- Check if audit table exists
	DECLARE @AuditTableObjectId BIGINT
	SELECT @AuditTableObjectId = Id FROM dbo.sysobjects WHERE id = object_id(QUOTENAME(@AuditTableSchemaName) + '.' + QUOTENAME(@TableName + @AuditTableSuffix)) and OBJECTPROPERTY(id, N'IsUserTable') = 1
	IF @AuditTableObjectId IS NULL
	BEGIN
		-- AuditTable does not exist, create new
		/* Schema auf each audit table:
- AuditId bigint identity -> incremenet, and uniquely identifies the audit row, used for versioning
- AuditHeaderId bigint not null references auditor.AuditHeader (id)
- 
*/
		-- Start of create table
		SET @CreateStatement = 'CREATE TABLE [' + @AuditTableSchemaName + '].['  + @SchemaName + '_' + @TableName + @AuditTableSuffix + '] ('
		SET @CreateStatement = @CreateStatement + '[AuditId] [bigint] IDENTITY (1, 1) NOT NULL,'
		SET @CreateStatement = @CreateStatement + '[AuditHeaderId] bigint not null references [auditor].[AuditHeader](id),'
		

		FETCH Next FROM TableColumns
		INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		
		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF (@ColumnType <> 'text' and @ColumnType <> 'ntext' and @ColumnType <> 'image' and @ColumnType <> 'timestamp')
			BEGIN
				SET @ListOfFields = @ListOfFields + quotename(@ColumnName) + ','
		
				SET @CreateStatement = @CreateStatement + '[' + @ColumnName + '] [' + @ColumnType + '] '
				
				IF @ColumnType in ('binary', 'char', 'nchar', 'nvarchar', 'varbinary', 'varchar')
				BEGIN
					IF (@ColumnLength = -1)
						Set @CreateStatement = @CreateStatement + '(max) '	 	
					ELSE
						SET @CreateStatement = @CreateStatement + '(' + cast(@ColumnLength as varchar(10)) + ') '	 	
				END
		
				IF @ColumnType in ('decimal', 'numeric')
					SET @CreateStatement = @CreateStatement + '(' + cast(@ColumnPrecision as varchar(10)) + ',' + cast(@ColumnScale as varchar(10)) + ') '	 	
		
				IF @ColumnType in ('char', 'nchar', 'nvarchar', 'varchar', 'text', 'ntext')
					SET @CreateStatement = @CreateStatement + 'COLLATE ' + @ColumnCollation + ' '
		
				IF @ColumnNullable = 0
					SET @CreateStatement = @CreateStatement + 'NOT '	 	
		
				SET @CreateStatement = @CreateStatement + 'NULL, '	 	
			END

			FETCH Next FROM TableColumns
			INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		END

		-- remove last ','
		set @CreateStatement = substring(@CreateStatement,1, len(@CreateStatement)-1) 
		set @CreateStatement = @CreateStatement + ')'
			
		-- Create audit table
		PRINT 'Creating audit table [' + @AuditTableSchemaName + '].[' + @SchemaName + '_' + @TableName + @AuditTableSuffix + ']'
		EXEC (@CreateStatement)

		-- Set primary key and default values
		SET @CreateStatement = 'ALTER TABLE [' + @AuditTableSchemaName + '].[' + @SchemaName + '_' + @TableName + @AuditTableSuffix + '] ADD '
		-- TODO additional index on original PK? etc? or iclude AuditHeaderId
		SET @CreateStatement = @CreateStatement + 'CONSTRAINT [PK_' + @SchemaName +'_'+ + @TableName + @AuditTableSuffix + '] PRIMARY KEY CLUSTERED ([AuditId])  ON [PRIMARY]'		
		EXEC (@CreateStatement)

	END

	CLOSE TableColumns
	DEALLOCATE TableColumns

END
GO

drop procedure [auditor].[sp_GenerateAuditTrigger_]
go
CREATE PROCEDURE [auditor].[sp_GenerateAuditTrigger_]
	@TableName sysname,
	@SchemaName sysname = 'dbo',
	@AuditTableSchemaName sysname,
	@AuditTableName sysname
AS BEGIN


	DECLARE @TableObjectId BIGINT
	SELECT @TableObjectId = Id FROM dbo.sysobjects WHERE id = object_id(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) and OBJECTPROPERTY(id, N'IsUserTable') = 1


-- Declare cursor to loop over columns
	DECLARE TableColumns CURSOR Read_Only
	FOR SELECT b.name, c.name as TypeName, b.length, b.isnullable, b.collation, b.xprec, b.xscale
		FROM sysobjects a 
		inner join syscolumns b on a.id = b.id 
		inner join systypes c on b.xtype = c.xtype and c.name <> 'sysname' 
		WHERE a.id = @TableObjectId
		ORDER BY b.colId

	OPEN TableColumns

	-- Declare temp variable to fetch records into
	DECLARE @ColumnName nvarchar(128)
	DECLARE @ColumnType nvarchar(128)
	DECLARE @ColumnLength smallint
	DECLARE @ColumnNullable int
	DECLARE @ColumnCollation sysname
	DECLARE @ColumnPrecision tinyint
	DECLARE @ColumnScale tinyint

	-- Declare variable to build statements	
	DECLARE @ListOfFields nvarchar(max)
	SET @ListOfFields = ''


FETCH Next FROM TableColumns
		INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		
		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF (@ColumnType <> 'text' and @ColumnType <> 'ntext' and @ColumnType <> 'image' and @ColumnType <> 'timestamp')
			BEGIN
				SET @ListOfFields = @ListOfFields + quotename(@ColumnName) + ','			
			END

			FETCH Next FROM TableColumns
			INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		END

		
	CLOSE TableColumns
	DEALLOCATE TableColumns
		
	
	DECLARE @HeaderInsertSql nvarchar(max) = '
	SET NOCOUNT ON;
	declare @RowCount int
	Select @RowCount=count(*) from inserted -- TODO use deleted within delete trigger
	
	declare @HeaderId bigint 
	INSERT INTO [auditor].[AuditHeader] (
		ChangeType,TransactionId, TransactionSequenceNumber, TableName, SchemaName, [SysObjectId], RawContextInfo, Columns_Updated, HostName, AppName, SqlUserName, AffectedRows)'
	SET @HeaderInsertSql = @HeaderInsertSql + ' SELECT ' -- TODO add I,U,D depending on usecase here
	declare @HeaderInsertSqlEnd nvarchar(max) = ',tx.transaction_id, tx.transaction_sequence_num, '''+ @TableName +''', ''' + @SchemaName + ''','+cast(@TableObjectId as nvarchar(max))+', CONTEXT_INFO(), COLUMNS_UPDATED(),
	CASE 
      WHEN LEN(HOST_NAME()) < 1 THEN '' ''
      ELSE HOST_NAME()
    END,
    CASE 
      WHEN LEN(APP_NAME()) < 1 THEN '' ''
      ELSE APP_NAME()
    END,
    SUSER_SNAME(),
	@RowCount
	 FROM sys.dm_tran_current_transaction tx;
	 
	 -- there can only be one header			
	SELECT @HeaderId = SCOPE_IDENTITY();'

	-- TODO if the client uses @@identity this won't work (because we isnert in the trigger into auditheader etc..., we'd need to do the following:
	/*
	--at the beginning of the trigger:
	Set @IDENTITY_SAVE = CAST(IsNull(@@IDENTITY,1) AS varchar(50))

	--at the end:
	 DECLARE @maxprec AS varchar(2)
    SET @maxprec=CAST(@@MAX_PRECISION as varchar(2))
    EXEC('SELECT IDENTITY(decimal('+@maxprec+',0),'+@IDENTITY_SAVE+',1) id INTO #tmp')
	*/

		/* Drop Triggers, if they exist */
	declare @InsertTriggerName sysname = 'Auditor_' + @TableName + '_Insert'
	declare @UpdateTriggerName sysname = 'Auditor_' + @TableName + '_Update'
	declare @DeleteTriggerName sysname = 'Auditor_' + @TableName + '_Delete'
	PRINT 'Dropping triggers'
	IF exists (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[' + @SchemaName + '].['+@InsertTriggerName+']') and OBJECTPROPERTY(id, N'IsTrigger') = 1) 
		EXEC ('drop trigger [' + @SchemaName + '].['+@InsertTriggerName+']')

	IF exists (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[' + @SchemaName + '].['+@UpdateTriggerName+']') and OBJECTPROPERTY(id, N'IsTrigger') = 1) 
		EXEC ('drop trigger [' + @SchemaName + '].['+@UpdateTriggerName+']')

	IF exists (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[' + @SchemaName + '].['+@DeleteTriggerName+']') and OBJECTPROPERTY(id, N'IsTrigger') = 1) 
		EXEC ('drop trigger [' + @SchemaName + '].['+@DeleteTriggerName+']')

	/* Create triggers */
	PRINT 'Creating triggers' 
	-- the joy of dynamic sql.... fun when debugging.
	EXEC ('CREATE TRIGGER [' + @InsertTriggerName + '] ON ' + @SchemaName + '.' + @TableName + ' FOR INSERT AS BEGIN '+@HeaderInsertSql+'''I''' + @HeaderInsertSqlEnd + ' INSERT INTO [' + @AuditTableSchemaName + '].[' + @AuditTableName + '](' +  @ListOfFields + 'AuditHeaderId) SELECT ' + @ListOfFields + ' @HeaderId FROM Deleted END')	
	EXEC('CREATE TRIGGER [' + @UpdateTriggerName + '] ON ' + @SchemaName + '.' + @TableName + ' FOR UPDATE AS BEGIN '+@HeaderInsertSql+'''U''' + @HeaderInsertSqlEnd + ' INSERT INTO [' + @AuditTableSchemaName + '].[' + @AuditTableName + '](' +  @ListOfFields + 'AuditHeaderId) SELECT ' + @ListOfFields + ' @HeaderId FROM Deleted END')
	EXEC ('CREATE TRIGGER [' + @DeleteTriggerName + '] ON ' + @SchemaName + '.' + @TableName + ' FOR DELETE AS BEGIN '+@HeaderInsertSql+'''D''' + @HeaderInsertSqlEnd + ' INSERT INTO [' + @AuditTableSchemaName + '].[' + @AuditTableName + '](' +  @ListOfFields + 'AuditHeaderId) SELECT ' + @ListOfFields + ' @HeaderId FROM Deleted END')

	-- set trigger order, so theat the audittriggers get fired last, if there are any other triggers that may modify the data.
	exec sp_settriggerorder @InsertTriggerName, 'last', 'INSERT'
	exec sp_settriggerorder @UpdateTriggerName, 'last', 'UPDATE'
	exec sp_settriggerorder @DeleteTriggerName, 'last', 'DELETE'
END
GO

-- Add's a  table to be audited.
drop procedure [auditor].[sp_RegisterTable]
go
CREATE PROCEDURE [auditor].[sp_RegisterTable]
	@TableName sysname,
	@SchemaName sysname = 'dbo',
	@AuditTableSuffix nvarchar(128) = '_Audit'
AS
BEGIN
	declare @AuditTableSchemaName sysname = 'auditor'

	declare @SchemaId int;
	DECLARE @TableObjectId BIGINT	
	SELECT @TableObjectId = object_id, @SchemaId = schema_id FROM sys.tables WHERE object_id = object_id(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) and Type = 'U'

	IF @TableObjectId IS NULL OR @SchemaId IS NULL
	BEGIN
		PRINT 'ERROR: Table or Schema does not exist'
		-- TODO raise error
		RETURN
	END

	if exists(select * from auditor.Tables t where t.SchemaId = @SchemaId AND t.TableObjectId = @TableObjectId) begin
		PRINT 'ERROR: Table already registered for auditing'
		-- TODO raise error
		RETURN
	end

	-- create audittable
	exec [auditor].[sp_GenerateAuditTable_] @TableName, @SchemaName, @AuditTableSchemaName, @AuditTableSuffix
	-- create triggers
	declare @AuditTableName nvarchar(max) = @SchemaName + '_' + @TableName + @AuditTableSuffix
	exec [auditor].[sp_GenerateAuditTrigger_] @TableName, @SchemaName, @AuditTableSchemaName, @AuditTableName

	-- insert into auditor.Tables
	insert into auditor.Tables(SchemaId, SchemaName, TableObjectId, TableName,AuditTableObjectId, AuditTableObjectName, AuditTableSchemaId, AuditTableSchemaName)
	SELECT @SchemaId, @SchemaName, @TableObjectId, @TableName, t.object_id, @AuditTableName,  t.schema_id, @AuditTableSchemaName from sys.tables t where t.object_id=object_Id(quotename(@AuditTableSchemaName) + '.' + quotename(@AuditTableName))

END
go

drop procedure [auditor].[sp_RegenerateTrigger]
go
CREATE PROCEDURE [auditor].[sp_RegenerateTrigger]
	@TableName sysname,
	@SchemaName sysname = 'dbo'
AS
BEGIN

	declare @SchemaId int;
	DECLARE @TableObjectId BIGINT	
	SELECT @TableObjectId = object_id, @SchemaId = schema_id FROM sys.tables WHERE object_id = object_id(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) and Type = 'U'

	declare @AuditTableObjectId int
	declare @AuditTableSchemaId int

	select @AuditTableObjectId = t.AuditTableObjectId, @AuditTableSchemaId = t.AuditTableSchemaId from auditor.Tables t where t.SchemaId = @SchemaId AND t.TableObjectId = @TableObjectId
	if @AuditTableObjectId is null or @AuditTableSchemaId is null  begin
		PRINT 'ERROR: Table not registered for auditing - call auditor.[sp_RegisterTable] first.'
		-- TODO raise error
		RETURN
	end
	
	-- create triggers
	declare @AuditSchemaName sysname = SCHEMA_NAME(@AuditTableSchemaId);
	declare @AuditTableName sysname = Object_Name(@AuditTableObjectId);	
	exec [auditor].[sp_GenerateAuditTrigger_] @TableName, @SchemaName,@AuditSchemaName , @AuditTableName

END
go

-- Add's a  table to be audited.
CREATE PROCEDURE [auditor].[sp_UnregisterTable]
	@TableName sysname,
	@SchemaName sysname = 'dbo'
AS
BEGIN
	declare @SchemaId int;
	DECLARE @TableObjectId BIGINT	
	SELECT @TableObjectId = object_id, @SchemaId = schema_id FROM sys.tables WHERE object_id = object_id(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) and Type = 'U'

	IF @TableObjectId IS NULL OR @SchemaId IS NULL
	BEGIN
		PRINT 'ERROR: Table or Schema does not exist'
		-- TODO raise error
		RETURN
	END

	if NOT exists(select * from auditor.Tables t where t.SchemaId = @SchemaId AND t.TableObjectId = @TableObjectId) begin
		PRINT 'ERROR: Table not registered for auditing'
		-- TODO raise error
		RETURN
	end
	
	exec('drop trigger')

END

--delete from auditor.Tables
begin transaction
exec [auditor].[sp_RegisterTable] 'Test', 'dbo', '_Audit'
exec [auditor].[sp_RegenerateTrigger] 'Test', 'dbo'
--commit
rollback

select * from auditor.AuditHeader
select * from auditor.dbo_test_audit

go

----------------------------------------------
-- FUNCTIONS
----------------------------------------------


-- get history of a specific row of a table, optionally within a timerange
drop procedure auditor.[GetRowHistory]
go
CREATE procedure auditor.[GetRowHistory]
			@TableName sysname, @SchemaName sysname, @PKExpression nvarchar(max), @MinDate datetime2 = null, @MaxDate datetime2 = null
AS 
BEGIN 

	declare @Sql nvarchar(max)
	declare @AuditTableName sysname
	declare @AuditTableSchemaName sysname
	declare @TableObjectId int
	declare @SchemaId int

	SELECT @TableObjectId = object_id, @SchemaId = schema_id FROM sys.tables WHERE object_id = object_id(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) and Type = 'U'

	IF @TableObjectId IS NULL OR @SchemaId IS NULL
	BEGIN
		PRINT 'ERROR: Table or Schema does not exist'
		-- TODO raise error
		RETURN
	END

	if NOT exists(select * from auditor.Tables t where t.SchemaId = @SchemaId AND t.TableObjectId = @TableObjectId) begin
		PRINT 'ERROR: Table not registered for auditing'
		-- TODO raise error
		RETURN
	end

	select @AuditTableName = t.AuditTableObjectName, @AuditTableSchemaName = t.AuditTableSchemaName from auditor.Tables t where t.SchemaId = @SchemaId AND t.TableObjectId = @TableObjectId
	if @AuditTableName is null or @AuditTableSchemaName is null  begin
		PRINT 'ERROR: Table not registered for auditing - call auditor.[sp_RegisterTable] first.'
		-- TODO raise error
		RETURN
	end
	
	set @Sql = N'
	SELECT * from auditor.AuditHeader h
	inner join ' + quotename(@AuditTableSchemaName) + '.' + quotename(@AuditTableName) + ' a on h.Id = a.AuditHeaderId
	where ' + @PKExpression + ' AND ( @MinDate is null or @MaxDate is null or h.AuditDateUtc between @MinDate and @MaxDate)'

	print @Sql
	exec sp_executesql @Sql ,N'@MinDate datetime2, @MaxDate datetime2', @MinDate=@MinDate, @MaxDate=@MaxDate
END 
go

exec auditor.[GetRowHistory] 'Test', 'dbo', 'a.Id != 549175'


-----------------------------------------------------
-----------------------------------------------------
------------------- SCHEMA CHANGES ------------------
-----------------------------------------------------
-----------------------------------------------------

create table auditor.SchemaAuditEntry (
	Id bigint identity primary key,
	[ObjectName] sysname NOT NULL,
    [SchemaName] sysname NOT NULL,
	[HostName] [nvarchar](128) NOT NULL,
    [AppName] [nvarchar](128) NOT NULL,
    [UserName] [nvarchar](128) NOT NULL,
	[LoginName] [nvarchar](128) NOT NULL,
	[AuditDateUtc] [datetime2] NOT NULL default(getutcdate()),
    --[SysObjectId]  AS (object_id([SchemaName] + '.' + [TableName])),
	EventName sysname not null,
	Sql nvarchar(max) not null,
	XmlEventData xml not null
)

go

--Note: Database triggers must be created in the dbo schema   
CREATE TRIGGER [Auditor_SchemaDDLTrigger]
ON DATABASE
FOR DDL_DATABASE_LEVEL_EVENTS
AS 
BEGIN

  
  SET NOCOUNT ON
  SET ARITHABORT ON
  SET ANSI_PADDING ON
  
declare @ContextInfo varbinary(128)
select @ContextInfo = context_info from master.dbo.sysprocesses where spid=@@SPID;

--check for recursive execution  of trigger 
IF @ContextInfo = 0x1
	RETURN 
	
  DECLARE 
    @EventData XML,
    @Schema sysname,
    @ObjectName sysname,
    @EventType sysname,
    @SQL VARCHAR(max)
    
  SET @EventData = EventData()
  
  SET @Schema = @EventData.value('data(/EVENT_INSTANCE/SchemaName)[1]', 'VARCHAR(50)')
  SET @ObjectName = @EventData.value('data(/EVENT_INSTANCE/ObjectName)[1]', 'VARCHAR(50)')
  SET @EventType = @EventData.value('data(/EVENT_INSTANCE/EventType)[1]', 'VARCHAR(50)')
  
  INSERT INTO auditor.SchemaAuditEntry (AuditDateUtc, LoginName, UserName, EventName, SchemaName, ObjectName, Sql, XMLEventData, HostName, AppName)
  SELECT 
    GetUtcDate(),
    SUSER_SNAME(),
    @EventData.value('data(/EVENT_INSTANCE/UserName)[1]', 'sysname'),
    @EventType, @Schema, @ObjectName,
    @EventData.value('data(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]', 'VARCHAR(max)'),
    @EventData,
	CASE 
      WHEN LEN(HOST_NAME()) < 1 THEN '?'
      ELSE HOST_NAME()
    END,
    CASE 
      WHEN LEN(APP_NAME()) < 1 THEN '?'
      ELSE APP_NAME()
    END
    
  
  /*IF @EventType = 'ALTER_TABLE' OR @EventType = 'RENAME'
    BEGIN 
		print 'TODO we could regenerate the audit triggers here...'
    END    
  */
END   


