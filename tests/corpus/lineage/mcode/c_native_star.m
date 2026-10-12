let
 S = Sql.Database("srv","erp"),
 q = Value.NativeQuery(S, "SELECT * FROM dbo.sales", null)
in
 q
