let
 S = Sql.Database("srv","erp"),
 q = Value.NativeQuery(S, "WITH c AS (SELECT id, amt FROM dbo.sales) SELECT c.id, c.amt * 2 AS dbl FROM c", null, [EnableFolding=true])
in
 q
