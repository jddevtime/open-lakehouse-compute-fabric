let
 S = Sql.Database("srv","erp"),
 q = Value.NativeQuery(S, "SELECT * FROM " & Tbl, null)
in
 q
