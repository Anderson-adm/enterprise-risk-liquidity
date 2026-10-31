-- Create a schema if missing; the name is a bound value quoted by QUOTENAME.
DECLARE @schema sysname = :schema;
DECLARE @ddl nvarchar(400) = N'CREATE SCHEMA ' + QUOTENAME(@schema);
IF SCHEMA_ID(@schema) IS NULL EXEC sp_executesql @ddl
