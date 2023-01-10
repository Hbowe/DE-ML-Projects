CREATE TABLE pw_addresses
(
	    address varchar(100),
        amount NVARCHAR(4000)
    )

CREATE TABLE #tmp(JsonFileName VARCHAR(100));

INSERT INTO #tmp
EXEC xp_cmdshell 'dir /B "JSON Folder"';

declare @fileName varchar(100)

While (Select Count(*) From #tmp where JsonFileName is not null) > 0
Begin

    Select Top 1 @fileName = JsonFileName From #tmp

    DECLARE @NewFile varchar(100)  
    SELECT @NewFile = CONCAT('JSON Folder', @fileName)


    DECLARE @SQL varchar(max) 
    SET @SQL = 
    
    'Declare @JSON varchar(max) 
    
    SELECT @JSON = BulkColumn FROM OPENROWSET(BUlK ' + char(39) + @Newfile + char(39) + ', SINGLE_CLOB) import
    INSERT INTO pw_addresses
    SELECT * FROM OPENJSON (@JSON, ''$.balances'')
    WITH (
        address varchar(100),
        amount NVARCHAR(4000)
    ) '
    
    EXEC(@SQL)
    Delete from #tmp Where JsonFileName = @FileName

End

DROP TABLE #tmp

