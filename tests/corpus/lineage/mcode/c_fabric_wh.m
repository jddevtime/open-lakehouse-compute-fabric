let
 S = Sql.Database("abc.datawarehouse.fabric.microsoft.com", "wh1"),
 t = S{[Schema="mart",Item="sales"]}[Data],
 k = Table.SelectColumns(t, {"id"})
in
 k
