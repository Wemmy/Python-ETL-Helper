DECLARE @sql NVARCHAR(MAX);
SET @sql = N'SELECT ';

-- Get all columns from Table1
SELECT @sql = @sql + 
    't1.' + QUOTENAME(c.name) + ', '
FROM sys.columns c
WHERE c.object_id = OBJECT_ID({table1});

SET @sql = LEFT(@sql, LEN(@sql)-1)  -- Remove trailing comma
    + 'into tmp FROM'+ {table1}+' t1 UNION ALL SELECT ';

-- Check if each column exists in Table2, put NULL for missing ones
SELECT @sql = @sql +  
    (CASE WHEN EXISTS (
        SELECT * 
        FROM sys.columns c2 
        WHERE c2.object_id = OBJECT_ID({table2}) AND c2.name = c.name
    ) THEN 't2.'+ QUOTENAME(c.name) + '' ELSE 'NULL AS '+ QUOTENAME(c.name) END)+ 
        ', '
FROM sys.columns c
WHERE c.object_id = OBJECT_ID({table1});

SET @sql = LEFT(@sql, LEN(@sql)-1)  -- Remove trailing comma
    + ' FROM '+{table2}+' t2;';
PRINT(@sql)
EXEC sp_executesql @sql;

-- replace temp to perm
drop table if exists {table1}
;
select * into {table1} from tmp
;